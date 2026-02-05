package indexer

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/csvquery/csvquery/internal/common"

	"github.com/pierrec/lz4/v4"
)

var (
	// Pool for 256KB bufio.Writers (used in flushChunk)
	bufWriterPool = sync.Pool{
		New: func() interface{} {
			return bufio.NewWriterSize(nil, 256*1024)
		},
	}
	// Pool for 64KB bufio.Readers (used in kWayMerge)
	bufReaderPool = sync.Pool{
		New: func() interface{} {
			return bufio.NewReaderSize(nil, 64*1024)
		},
	}
)

// Sorter handles external merge sort for large datasets
type Sorter struct {
	Name          string
	outputPath    string
	tempDir       string
	chunkSize     int // Max records per chunk
	chunkFiles    []string
	totalRecords  int64
	bytesWritten  int64
	mergedRecords int64
	state         int32 // Atomic state

	// Buffer for current chunk
	memBuffer []common.IndexRecord

	// Distinct counts per chunk
	chunkDistincts []int64

	// Bloom Filter (Concurrent Building)
	bloom *common.BloomFilter
}

// NewSorter creates a new external sorter
//
// Parameters:
//   - name: Name for the sorter (e.g., for logging)
//   - outputPath: Final sorted index file path
//   - tempDir: Directory for temporary chunk files
//   - memoryLimit: Maximum memory to use for sorting buffer in bytes
//   - bloom: Optional bloom filter to populate during merge (can be nil)
//
// Memory calculation:
//   - Each record = 80 bytes on disk, ~100 bytes in memory (with Go overhead)
//   - chunkSize = memoryLimit / 100
//   - Min chunk size 1000
func NewSorter(name, outputPath, tempDir string, memoryLimit int, bloom *common.BloomFilter) *Sorter {
	// Estimate records per chunk based on memory limit
	// RecordSize ~ 80 bytes (key+offset+line) + Overhead
	// Let's assume 100 bytes per record safely
	chunkSize := memoryLimit / 100
	if chunkSize < 1000 {
		chunkSize = 1000
	}

	return &Sorter{
		Name:       name,
		outputPath: outputPath,
		tempDir:    tempDir,
		chunkSize:  chunkSize,
		memBuffer:  make([]common.IndexRecord, 0, chunkSize),
		bloom:      bloom,
	}
}

// Add adds a record to the sorter
// When buffer is full, it's sorted and written to a temp file
func (sorter *Sorter) Add(record common.IndexRecord) error {
	sorter.memBuffer = append(sorter.memBuffer, record)
	atomic.AddInt64(&sorter.totalRecords, 1)

	// Flush chunk when full
	if len(sorter.memBuffer) >= sorter.chunkSize {
		return sorter.flushChunk()
	}
	return nil
}

// flushChunk sorts the current buffer and writes to a temp file
func (sorter *Sorter) flushChunk() error {
	if len(sorter.memBuffer) == 0 {
		return nil
	}

	// Sort by key, then offset (Zero Allocation)
	slices.SortFunc(sorter.memBuffer, func(a, b common.IndexRecord) int {
		cmp := bytes.Compare(a.Key[:], b.Key[:])
		if cmp != 0 {
			return cmp
		}
		// Tie-breaker: Offset
		if a.Offset < b.Offset {
			return -1
		}
		if a.Offset > b.Offset {
			return 1
		}
		return 0
	})

	// Write to temp file
	chunkPath := filepath.Join(sorter.tempDir, fmt.Sprintf("chunk_%d.tmp", len(sorter.chunkFiles)))
	file, err := os.Create(chunkPath)
	if err != nil {
		return fmt.Errorf("failed to create chunk file: %w", err)
	}

	// Use LZ4 compression for temp chunks (FAST)
	// We use frame format for simplicity in temp files
	lzWriter := lz4.NewWriter(file)

	// Get buffered writer from pool
	bufferedWriter := bufWriterPool.Get().(*bufio.Writer)
	bufferedWriter.Reset(lzWriter)
	// Ensure we return it to the pool
	defer func() {
		bufferedWriter.Reset(nil) // Release reference
		bufWriterPool.Put(bufferedWriter)
	}()

	var distinctCount int64 = 0
	var lastKey [64]byte

	// Count distinct keys (and verify order if needed, but we just sorted)
	for i, rec := range sorter.memBuffer {
		if i == 0 || rec.Key != lastKey {
			distinctCount++
			lastKey = rec.Key
		}
	}

	// Batch write the entire sorted buffer
	if err := common.WriteBatchRecords(bufferedWriter, sorter.memBuffer); err != nil {
		_ = bufferedWriter.Flush()
		_ = lzWriter.Close()
		_ = file.Close()
		return err
	}
	atomic.AddInt64(&sorter.bytesWritten, int64(len(sorter.memBuffer))*common.RecordSize)

	if err := bufferedWriter.Flush(); err != nil {
		_ = lzWriter.Close()
		_ = file.Close()
		return err
	}

	if err := lzWriter.Close(); err != nil {
		_ = file.Close()
		return err
	}
	_ = file.Close()

	sorter.chunkFiles = append(sorter.chunkFiles, chunkPath)
	sorter.chunkDistincts = append(sorter.chunkDistincts, distinctCount)
	sorter.memBuffer = sorter.memBuffer[:0] // Clear buffer

	return nil
}

// Finalize performs the final merge and writes the output file
// Returns the count of distinct keys
func (sorter *Sorter) Finalize() (int64, error) {
	// Flush any remaining buffer
	if err := sorter.flushChunk(); err != nil {
		return 0, err
	}

	// Transition to Merging
	atomic.StoreInt32(&sorter.state, int32(StateMerging))

	// ALWAYS perform k-way merge to ensure output is compressed (even if 1 chunk)
	if len(sorter.chunkFiles) == 0 {
		// Empty file
		f, err := os.Create(sorter.outputPath)
		if err != nil {
			return 0, err
		}
		_ = f.Close()
		atomic.StoreInt32(&sorter.state, int32(StateDone))
		return 0, nil
	}

	// K-way merge (reads raw chunks -> writes compressed output)
	count, err := sorter.kWayMerge()
	if err == nil {
		atomic.StoreInt32(&sorter.state, int32(StateDone))
	}
	return count, err
}

// mergeItem represents an item in the priority queue for k-way merge
type mergeItem struct {
	record common.IndexRecord
	source int // Index of source file
}

// manualHeap is a manual implementation of a Min-Heap for mergeItem
// container/heap uses interface{} boxing which triggers allocations.
type manualHeap []mergeItem

func (mergeHeap manualHeap) Len() int { return len(mergeHeap) }
func (h manualHeap) Less(i, j int) bool {
	return h[i].Less(h[j])
}
func (h manualHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (mergeHeap *manualHeap) Push(x mergeItem) {
	*mergeHeap = append(*mergeHeap, x)
	mergeHeap.up(len(*mergeHeap) - 1)
}

func (mergeHeap *manualHeap) Pop() mergeItem {
	old := *mergeHeap
	n := len(old)
	x := old[0]
	old[0] = old[n-1]
	*mergeHeap = old[0 : n-1]
	mergeHeap.down(0, n-1)
	return x
}

func (mergeHeap *manualHeap) up(j int) {
	for {
		i := (j - 1) / 2 // parent
		if i == j || !(*mergeHeap)[j].Less((*mergeHeap)[i]) {
			break
		}
		mergeHeap.Swap(i, j)
		j = i
	}
}

func (mergeHeap *manualHeap) down(i0, n int) {
	i := i0
	for {
		j1 := 2*i + 1
		if j1 >= n || j1 < 0 { // j1 < 0 after int overflow
			break
		}
		j := j1 // left child
		if j2 := j1 + 1; j2 < n && (*mergeHeap)[j2].Less((*mergeHeap)[j1]) {
			j = j2 // = 2*i + 2  // right child
		}
		if !(*mergeHeap)[j].Less((*mergeHeap)[i]) {
			break
		}
		mergeHeap.Swap(j, i)
		i = j
	}
}

// Helper for Less calling on element
func (m mergeItem) Less(other mergeItem) bool {
	cmp := bytes.Compare(m.record.Key[:], other.record.Key[:])
	if cmp != 0 {
		return cmp < 0
	}
	return m.record.Offset < other.record.Offset
}

// kWayMerge performs k-way merge of sorted chunk files
func (sorter *Sorter) kWayMerge() (int64, error) {
	chunkCount := len(sorter.chunkFiles)

	// Open all chunk files
	readers := make([]*bufio.Reader, chunkCount) // Changed to bufio.Reader
	files := make([]*os.File, chunkCount)

	for i, path := range sorter.chunkFiles {
		chunkFile, err := os.Open(path)
		if err != nil {
			return 0, fmt.Errorf("failed to open chunk %d: %w", i, err)
		}
		files[i] = chunkFile
		// Temp files are now LZ4 compressed
		// lz4.NewReader wraps the file directly
		// BUFFERING IS CRITICAL: We read small records.
		lzReader := lz4.NewReader(chunkFile)

		// Get reader from pool
		bufReader := bufReaderPool.Get().(*bufio.Reader)
		bufReader.Reset(lzReader)
		readers[i] = bufReader
	}

	// cleanup readers (return to pool)
	defer func() {
		for _, r := range readers {
			if r != nil {
				r.Reset(nil)
				bufReaderPool.Put(r)
			}
		}
	}()

	// Close all files on exit
	defer func() {
		for _, outputFile := range files {
			if outputFile != nil {
				_ = outputFile.Close()
			}
		}
	}()

	// Create output file
	outFile, err := os.Create(sorter.outputPath)
	if err != nil {
		return 0, fmt.Errorf("failed to create output file: %w", err)
	}
	defer func() { _ = outFile.Close() }()

	// Use BlockWriter for compressed output
	writer, err := common.NewBlockWriter(outFile)
	if err != nil {
		return 0, err
	}

	// Initialize heap with first record from each chunk
	mergeHeap := make(manualHeap, 0, chunkCount)

	for i := 0; i < chunkCount; i++ {
		rec, err := common.ReadRecord(readers[i])
		if err == nil {
			mergeHeap = append(mergeHeap, mergeItem{record: rec, source: i})
		}
	}
	// Init heap (floyd's algorithm or just manual push... manual push is slower for init but K is small)
	// We just appended, now simplify heapify
	// Go's heap.Init is O(n). We can just do that manually:
	n := len(mergeHeap)
	for i := n/2 - 1; i >= 0; i-- {
		mergeHeap.down(i, n)
	}

	var distinctCount int64 = 0
	var lastKey [64]byte
	var firstRecord = true

	// Merge phase
	for len(mergeHeap) > 0 {
		// Pop smallest
		heapItem := mergeHeap.Pop()
		rec := heapItem.record

		// Check distinct
		if firstRecord || rec.Key != lastKey {
			distinctCount++

			// Add to bloom filter if distinct
			if sorter.bloom != nil {
				// We need string key for Bloom.
				// Trim nulls
				keyStr := string(bytes.TrimRight(rec.Key[:], "\x00"))
				sorter.bloom.Add(keyStr)
			}

			lastKey = rec.Key
			firstRecord = false
		}

		// Write to output using BlockWriter (Write ALL records)
		if err := writer.WriteRecord(rec); err != nil {
			return 0, err
		}
		atomic.AddInt64(&sorter.mergedRecords, 1)

		// Read next from same source
		nextRec, err := common.ReadRecord(readers[heapItem.source])
		if err == nil {
			mergeHeap.Push(mergeItem{record: nextRec, source: heapItem.source})
		}
	}

	// Finalize block writer
	if err := writer.Close(); err != nil {
		return 0, err
	}

	return distinctCount, nil
}

// Cleanup removes temporary files
func (sorter *Sorter) Cleanup() {
	for _, path := range sorter.chunkFiles {
		_ = os.Remove(path)
	}
	sorter.chunkFiles = nil
}

// State constants
const (
	StateCollecting = iota
	StateMerging
	StateDone
)

type SorterStats struct {
	TotalRecords  int64
	MergedRecords int64
	BytesWritten  int64
	ChunkCount    int
	State         int
}

// GetStats returns current progress stats
func (sorter *Sorter) GetStats() SorterStats {
	// Atomic load state
	state := int(atomic.LoadInt32(&sorter.state))

	// If state is Done, we might have cleaned up chunks, so return 0 or cached?
	// 0 is fine if Done.
	chunkCount := 0
	if state != StateDone {
		chunkCount = len(sorter.chunkFiles)
	}

	return SorterStats{
		TotalRecords:  atomic.LoadInt64(&sorter.totalRecords),
		MergedRecords: atomic.LoadInt64(&sorter.mergedRecords),
		BytesWritten:  atomic.LoadInt64(&sorter.bytesWritten),
		ChunkCount:    chunkCount,
		State:         state,
	}
}
