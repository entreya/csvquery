package indexer

import (
	"bufio"
	"bytes"
	"github.com/csvquery/csvquery/internal/common"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"

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
func (s *Sorter) Add(record common.IndexRecord) error {
	s.memBuffer = append(s.memBuffer, record)
	atomic.AddInt64(&s.totalRecords, 1)

	// Flush chunk when full
	if len(s.memBuffer) >= s.chunkSize {
		return s.flushChunk()
	}
	return nil
}

// flushChunk sorts the current buffer and writes to a temp file
func (s *Sorter) flushChunk() error {
	if len(s.memBuffer) == 0 {
		return nil
	}

	// Sort by key, then offset (Zero Allocation)
	slices.SortFunc(s.memBuffer, func(a, b common.IndexRecord) int {
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
	chunkPath := filepath.Join(s.tempDir, fmt.Sprintf("chunk_%d.tmp", len(s.chunkFiles)))
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
	for i, rec := range s.memBuffer {
		if i == 0 || rec.Key != lastKey {
			distinctCount++
			lastKey = rec.Key
		}
	}

	// Batch write the entire sorted buffer
	if err := common.WriteBatchRecords(bufferedWriter, s.memBuffer); err != nil {
		bufferedWriter.Flush()
		lzWriter.Close()
		file.Close()
		return err
	}
	atomic.AddInt64(&s.bytesWritten, int64(len(s.memBuffer))*common.RecordSize)

	if err := bufferedWriter.Flush(); err != nil {
		lzWriter.Close()
		file.Close()
		return err
	}

	if err := lzWriter.Close(); err != nil {
		file.Close()
		return err
	}
	file.Close()

	s.chunkFiles = append(s.chunkFiles, chunkPath)
	s.chunkDistincts = append(s.chunkDistincts, distinctCount)
	s.memBuffer = s.memBuffer[:0] // Clear buffer

	return nil
}

// Finalize performs the final merge and writes the output file
// Returns the count of distinct keys
func (s *Sorter) Finalize() (int64, error) {
	// Flush any remaining buffer
	if err := s.flushChunk(); err != nil {
		return 0, err
	}

	// Transition to Merging
	atomic.StoreInt32(&s.state, int32(StateMerging))

	// ALWAYS perform k-way merge to ensure output is compressed (even if 1 chunk)
	if len(s.chunkFiles) == 0 {
		// Empty file
		f, err := os.Create(s.outputPath)
		if err != nil {
			return 0, err
		}
		f.Close()
		atomic.StoreInt32(&s.state, int32(StateDone))
		return 0, nil
	}

	// K-way merge (reads raw chunks -> writes compressed output)
	count, err := s.kWayMerge()
	if err == nil {
		atomic.StoreInt32(&s.state, int32(StateDone))
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

func (h manualHeap) Len() int { return len(h) }
func (h manualHeap) Less(i, j int) bool {
	return h[i].Less(h[j])
}
func (h manualHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *manualHeap) Push(x mergeItem) {
	*h = append(*h, x)
	h.up(len(*h) - 1)
}

func (h *manualHeap) Pop() mergeItem {
	old := *h
	n := len(old)
	x := old[0]
	old[0] = old[n-1]
	*h = old[0 : n-1]
	h.down(0, n-1)
	return x
}

func (h *manualHeap) up(j int) {
	for {
		i := (j - 1) / 2 // parent
		if i == j || !(*h)[j].Less((*h)[i]) {
			break
		}
		h.Swap(i, j)
		j = i
	}
}

func (h *manualHeap) down(i0, n int) {
	i := i0
	for {
		j1 := 2*i + 1
		if j1 >= n || j1 < 0 { // j1 < 0 after int overflow
			break
		}
		j := j1 // left child
		if j2 := j1 + 1; j2 < n && (*h)[j2].Less((*h)[j1]) {
			j = j2 // = 2*i + 2  // right child
		}
		if !(*h)[j].Less((*h)[i]) {
			break
		}
		h.Swap(j, i)
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
func (s *Sorter) kWayMerge() (int64, error) {
	k := len(s.chunkFiles)

	// Open all chunk files
	readers := make([]*bufio.Reader, k) // Changed to bufio.Reader
	files := make([]*os.File, k)

	for i, path := range s.chunkFiles {
		f, err := os.Open(path)
		if err != nil {
			return 0, fmt.Errorf("failed to open chunk %d: %w", i, err)
		}
		files[i] = f
		// Temp files are now LZ4 compressed
		// lz4.NewReader wraps the file directly
		// BUFFERING IS CRITICAL: We read small records.
		lzReader := lz4.NewReader(f)

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
		for _, f := range files {
			if f != nil {
				f.Close()
			}
		}
	}()

	// Create output file
	outFile, err := os.Create(s.outputPath)
	if err != nil {
		return 0, fmt.Errorf("failed to create output file: %w", err)
	}
	defer outFile.Close()

	// Use BlockWriter for compressed output
	writer, err := common.NewBlockWriter(outFile)
	if err != nil {
		return 0, err
	}

	// Initialize heap with first record from each chunk
	h := make(manualHeap, 0, k)

	for i := 0; i < k; i++ {
		rec, err := common.ReadRecord(readers[i])
		if err == nil {
			h = append(h, mergeItem{record: rec, source: i})
		}
	}
	// Init heap (floyd's algorithm or just manual push... manual push is slower for init but K is small)
	// We just appended, now simplify heapify
	// Go's heap.Init is O(n). We can just do that manually:
	n := len(h)
	for i := n/2 - 1; i >= 0; i-- {
		h.down(i, n)
	}

	var distinctCount int64 = 0
	var lastKey [64]byte
	var firstRecord = true

	// Merge phase
	for len(h) > 0 {
		// Pop smallest
		item := h.Pop()
		rec := item.record

		// Check distinct
		if firstRecord || rec.Key != lastKey {
			distinctCount++

			// Add to bloom filter if distinct
			if s.bloom != nil {
				// We need string key for Bloom.
				// Trim nulls
				keyStr := string(bytes.TrimRight(rec.Key[:], "\x00"))
				s.bloom.Add(keyStr)
			}

			lastKey = rec.Key
			firstRecord = false
		}

		// Write to output using BlockWriter (Write ALL records)
		if err := writer.WriteRecord(rec); err != nil {
			return 0, err
		}
		atomic.AddInt64(&s.mergedRecords, 1)

		// Read next from same source
		nextRec, err := common.ReadRecord(readers[item.source])
		if err == nil {
			h.Push(mergeItem{record: nextRec, source: item.source})
		}
	}

	// Finalize block writer
	if err := writer.Close(); err != nil {
		return 0, err
	}

	return distinctCount, nil
}

// Cleanup removes temporary files
func (s *Sorter) Cleanup() {
	for _, path := range s.chunkFiles {
		os.Remove(path)
	}
	s.chunkFiles = nil
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
func (s *Sorter) GetStats() SorterStats {
	// Atomic load state
	state := int(atomic.LoadInt32(&s.state))

	// If state is Done, we might have cleaned up chunks, so return 0 or cached?
	// 0 is fine if Done.
	chunkCount := 0
	if state != StateDone {
		chunkCount = len(s.chunkFiles)
	}

	return SorterStats{
		TotalRecords:  atomic.LoadInt64(&s.totalRecords),
		MergedRecords: atomic.LoadInt64(&s.mergedRecords),
		BytesWritten:  atomic.LoadInt64(&s.bytesWritten),
		ChunkCount:    chunkCount,
		State:         state,
	}
}
