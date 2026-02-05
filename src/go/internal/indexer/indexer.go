package indexer

import (
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/csvquery/csvquery/internal/common"
)

// IndexerConfig holds configuration for the indexer
type IndexerConfig struct {
	InputFile   string  // Path to CSV file
	OutputDir   string  // Output directory for indexes
	Columns     string  // JSON array of column definitions
	Separator   string  // CSV separator
	Workers     int     // Number of parallel workers
	MemoryMB    int     // Memory limit per worker in MB
	BloomFPRate float64 // Bloom filter false positive rate
	Verbose     bool    // Enable verbose output
	Version     string  // version string
}

// Indexer builds multiple indexes from a CSV file
type Indexer struct {
	config      IndexerConfig
	colDefs     [][]string // Parsed column definitions
	scanner     *Scanner
	tempDir     string
	meta        common.IndexMeta
	metaMutex   sync.Mutex
	sorters     []*Sorter
	sorterMutex sync.RWMutex
	stopReport  chan struct{}
}

// NewIndexer creates a new indexer
func NewIndexer(config IndexerConfig) *Indexer {
	return &Indexer{
		config: config,
		meta: common.IndexMeta{
			Indexes: make(map[string]common.IndexStats),
		},
		stopReport: make(chan struct{}),
	}
}

// Run executes the full indexing process
func (indexer *Indexer) Run() error {
	// startTime := time.Now()

	// Print header
	fmt.Println("╔══════════════════════════════════════════════════════════════════════════╗")
	content := fmt.Sprintf("CSVQUERY INDEXER (PIPELINED) v%s", indexer.config.Version)
	padding := 74 - len(content)
	left := padding / 2
	right := padding - left
	fmt.Printf("║%*s%s%*s║\n", left, "", content, right, "")
	fmt.Println("╚══════════════════════════════════════════════════════════════════════════╝")
	fmt.Printf("\nInput:    %s\n", indexer.config.InputFile)
	fmt.Printf("Output:   %s\n", indexer.config.OutputDir)

	// Parse column definitions
	if err := indexer.parseColumns(); err != nil {
		return err
	}
	fmt.Printf("Indexes:  %d\n", len(indexer.colDefs))
	fmt.Printf("Workers:  %d\n", indexer.config.Workers)
	fmt.Printf("Memory:   %dMB per worker\n\n", indexer.config.MemoryMB)

	// Create output directory
	if err := os.MkdirAll(indexer.config.OutputDir, 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	// Create temp directory for Sorter spills
	indexer.tempDir = filepath.Join(indexer.config.OutputDir, ".csvquery_temp")
	if err := os.MkdirAll(indexer.tempDir, 0755); err != nil {
		return fmt.Errorf("failed to create temp directory: %w", err)
	}

	// NOTE: Cleanup registration moved to main.go using indexer.Cleanup()

	// Open scanner
	var err error
	indexer.scanner, err = NewScanner(indexer.config.InputFile, indexer.config.Separator)
	if err != nil {
		return err
	}
	// Propagate worker count to scanner
	if indexer.config.Workers > 0 {
		indexer.scanner.SetWorkers(indexer.config.Workers)
	}
	defer func() { _ = indexer.scanner.Close() }()

	// Validate columns
	for _, cols := range indexer.colDefs {
		if err := indexer.scanner.ValidateColumns(cols); err != nil {
			return err
		}
	}

	// Initialize Channels and Sorters
	numIndexes := len(indexer.colDefs)
	// Change to buffered channel of SLICES (Batching)
	channels := make([]chan []common.IndexRecord, numIndexes)
	errors := make(chan error, numIndexes)
	results := make(chan string, numIndexes)

	var wg sync.WaitGroup

	// Start reporting
	indexer.startReporting()
	defer indexer.stopReporting()

	fmt.Println("Phase 1: Starting Pipelined Indexing...")

	// Launch Sorter Consumers (One per index)
	for i, cols := range indexer.colDefs {
		// Buffer depth for batches
		channels[i] = make(chan []common.IndexRecord, 100)
		wg.Add(1)

		go func(indexIdx int, columns []string, batchChannel <-chan []common.IndexRecord) {
			defer wg.Done()
			// Normalize index name to lowercase to match QueryEngine expectations
			colName := strings.ToLower(strings.Join(columns, "_"))

			err := indexer.runSorterNode(colName, batchChannel)
			if err != nil {
				errors <- fmt.Errorf("%s: %v", colName, err)
			} else {
				results <- colName
			}
		}(i, cols, channels[i])
	}

	// Build column indices for scanner
	colIndices := make([][]int, len(indexer.colDefs))
	for i, cols := range indexer.colDefs {
		colIndices[i] = make([]int, len(cols))
		for j, col := range cols {
			colIndices[i][j], _ = indexer.scanner.GetColumnIndex(col)
		}
	}

	// Prepare per-worker buffers
	// workerBuffers[workerID][indexID] -> []IndexRecord
	numWorkers := indexer.config.Workers
	if numWorkers == 0 {
		numWorkers = runtime.NumCPU()
	}
	workerBuffers := make([][][]common.IndexRecord, numWorkers)
	const batchSize = 1000 // Send batches of 1000 records

	for w := 0; w < numWorkers; w++ {
		workerBuffers[w] = make([][]common.IndexRecord, numIndexes)
		for i := 0; i < numIndexes; i++ {
			workerBuffers[w][i] = make([]common.IndexRecord, 0, batchSize)
		}
	}

	// Start Scanning
	lastProgress := time.Now()

	err = indexer.scanner.Scan(colIndices, func(workerID int, keys [][]byte, offset, line int64) {
		// keys corresponds to indexer.colDefs index
		// Use workerID to access thread-local buffer
		if workerID >= len(workerBuffers) {
			// Should not happen if Scanner respects worker count
			return
		}

		buffers := workerBuffers[workerID]

		for i, key := range keys {
			// Optimization: Append to buffer
			var keyBytes [64]byte
			copy(keyBytes[:], key)

			rec := common.IndexRecord{
				Key:    keyBytes,
				Offset: offset,
				Line:   line,
			}

			buffers[i] = append(buffers[i], rec)

			// Flush if full
			if len(buffers[i]) >= batchSize {
				// We must copy the slice or allocate a new one because the channel sends ownership?
				// Actually, we pass the slice. We should assume ownership transfer.
				// So we need to allocate a new buffer for the next batch.
				// Or copy to a new slice and send that.

				// Send a copy to avoid race conditions if we reuse the backing array immediately?
				// If we reuse `buffers[i][:0]`, the backing array is shared.
				// If consumer reads it while producer appends, race.
				// So we must detach the buffer.

				batchToSend := buffers[i]
				channels[i] <- batchToSend

				// allocate new buffer
				buffers[i] = make([]common.IndexRecord, 0, batchSize)
			}
		}

		if indexer.config.Verbose && time.Since(lastProgress) > 5*time.Second {
			// fmt.Println(indexer.scanner.ScanProgress())
			lastProgress = time.Now()
		}
	})

	// Flush remaining buffers
	for w := 0; w < numWorkers; w++ {
		for i := 0; i < numIndexes; i++ {
			if len(workerBuffers[w][i]) > 0 {
				channels[i] <- workerBuffers[w][i]
			}
		}
	}

	// Close all channels to signal Sorters to finish
	for _, batchChannel := range channels {
		close(batchChannel)
	}

	if err != nil {
		return fmt.Errorf("scanning failed: %w", err)
	}

	// Wait for all sorters to finish
	wg.Wait()
	close(results)
	close(errors)

	// Collect results
	hasError := false
	for {
		select {
		case name, ok := <-results:
			if !ok {
				results = nil
			} else {
				fmt.Printf("  ✅ %s\n", name)
			}
		case err, ok := <-errors:
			if !ok {
				errors = nil
			} else {
				fmt.Printf("  ❌ %v\n", err)
				hasError = true
			}
		}
		if results == nil && errors == nil {
			break
		}
	}

	// Stats
	rows, bytes, elapsed := indexer.scanner.GetStats()
	indexer.meta.TotalRows = rows
	fmt.Printf("\nStatistics:\n")
	fmt.Printf("  Rows: %d\n", rows)
	fmt.Printf("  Size: %.1f GB\n", float64(bytes)/1024/1024/1024)
	fmt.Printf("  Time: %v\n", elapsed.Round(time.Millisecond))
	fmt.Printf("  Rate: %.0f rows/sec\n", float64(rows)/elapsed.Seconds())

	// Capture CSV DNA for integrity protection
	if csvMeta, err := indexer.calculateFingerprint(); err == nil {
		indexer.meta.CsvSize = csvMeta.size
		indexer.meta.CsvMtime = csvMeta.mtime
		indexer.meta.CsvHash = csvMeta.hash
	}

	// Cleanup temp files
	indexer.Cleanup()

	// Save metadata
	if err := indexer.saveMeta(); err != nil {
		fmt.Printf("⚠️ Failed to save metadata: %v\n", err)
	}

	if hasError {
		return fmt.Errorf("some indexes failed to build")
	}

	return nil
}

// runSorterNode consumes data from channel and feeds the Sorter
func (indexer *Indexer) runSorterNode(name string, batchChannel <-chan []common.IndexRecord) error {
	csvName := strings.TrimSuffix(filepath.Base(indexer.config.InputFile), filepath.Ext(indexer.config.InputFile))
	indexPath := filepath.Join(indexer.config.OutputDir, csvName+"_"+name+".cidx")
	bloomPath := indexPath + ".bloom"

	// Temp dir strictly for this sorter (for external spills)
	tempSortDir := filepath.Join(indexer.tempDir, fmt.Sprintf("sort_%s", name))
	if err := os.MkdirAll(tempSortDir, 0755); err != nil {
		return fmt.Errorf("failed to create temp sort dir: %w", err)
	}

	// Memory limit per indexer (shared budget)
	totalMemBytes := indexer.config.MemoryMB * 1024 * 1024
	numIndexes := len(indexer.colDefs)
	memoryPerIndex := totalMemBytes / numIndexes
	if memoryPerIndex < 10*1024*1024 {
		memoryPerIndex = 10 * 1024 * 1024 // Minimum 10MB per index
	}

	// Initialize Bloom Filter
	var bloom *common.BloomFilter
	if indexer.config.BloomFPRate > 0 {
		// Use a safe initial estimate.
		// Since we don't know the exact count yet (it's streaming), we estimate.
		// 10M is a safe fallback default. If it's too small, FP rate increases.
		bloom = common.NewBloomFilter(10_000_000, indexer.config.BloomFPRate)
	}

	sorter := NewSorter(name, indexPath, tempSortDir, memoryPerIndex, bloom)

	indexer.sorterMutex.Lock()
	indexer.sorters = append(indexer.sorters, sorter)
	indexer.sorterMutex.Unlock()

	defer func() {
		sorter.Cleanup()
		// idx.cleanup() handles the root temp dir.
	}()

	// Consume channel (Batches)
	for batch := range batchChannel {
		for _, indexRecord := range batch {
			if err := sorter.Add(indexRecord); err != nil {
				return err
			}
		}
	}

	// Finalize sorting
	distinctCount, err := sorter.Finalize()
	if err != nil {
		return err
	}

	// Get file size
	stat, _ := os.Stat(indexPath)
	fileSize := stat.Size()

	// Update metadata
	indexer.metaMutex.Lock()
	indexer.meta.Indexes[name] = common.IndexStats{
		DistinctCount: distinctCount,
		FileSize:      fileSize,
	}
	indexer.metaMutex.Unlock()

	// Serialize Bloom Filter
	if bloom != nil {
		if err := os.WriteFile(bloomPath, bloom.Serialize(), 0644); err != nil {
			fmt.Printf("  ⚠️  Bloom filter failed for %s: %v\n", name, err)
		}
	}

	return nil
}

// parseColumns parses the JSON column definitions
func (indexer *Indexer) parseColumns() error {
	// Parse JSON
	var raw interface{}
	if err := json.Unmarshal([]byte(indexer.config.Columns), &raw); err != nil {
		return fmt.Errorf("failed to parse columns JSON: %w", err)
	}

	// Handle different formats
	switch v := raw.(type) {
	case []interface{}:
		for _, item := range v {
			switch col := item.(type) {
			case string:
				// Single column: "COL1"
				indexer.colDefs = append(indexer.colDefs, []string{col})
			case []interface{}:
				// Composite or array: ["COL1"] or ["COL1", "COL2"]
				var cols []string
				for _, c := range col {
					if s, ok := c.(string); ok {
						cols = append(cols, s)
					}
				}
				if len(cols) > 0 {
					indexer.colDefs = append(indexer.colDefs, cols)
				}
			}
		}
	default:
		return fmt.Errorf("columns must be a JSON array")
	}

	if len(indexer.colDefs) == 0 {
		return fmt.Errorf("no valid column definitions found")
	}

	return nil
}

// saveMeta writes metadata to JSON file
func (indexer *Indexer) saveMeta() error {
	indexer.meta.CapturedAt = time.Now()

	data, err := json.MarshalIndent(indexer.meta, "", "  ")
	if err != nil {
		return err
	}

	csvName := strings.TrimSuffix(filepath.Base(indexer.config.InputFile), filepath.Ext(indexer.config.InputFile))
	metaPath := filepath.Join(indexer.config.OutputDir, csvName+"_meta.json")
	return os.WriteFile(metaPath, data, 0644)
}

type csvDNA struct {
	size  int64
	mtime int64
	hash  string
}

func (indexer *Indexer) calculateFingerprint() (csvDNA, error) {
	file, err := os.Open(indexer.config.InputFile)
	if err != nil {
		return csvDNA{}, err
	}
	defer func() { _ = file.Close() }()

	stat, err := file.Stat()
	if err != nil {
		return csvDNA{}, err
	}

	size := stat.Size()
	mtime := stat.ModTime().Unix()
	sampleSize := int64(512 * 1024) // 512KB per sample

	hasher := sha1.New()

	// 1. Start Sample
	buf := make([]byte, sampleSize)
	n, _ := file.ReadAt(buf, 0)
	hasher.Write(buf[:n])

	// 2. Middle Sample
	if size > sampleSize*3 {
		n, _ = file.ReadAt(buf, (size/2)-(sampleSize/2))
		hasher.Write(buf[:n])
	}

	// 3. End Sample
	if size > sampleSize {
		start := size - sampleSize
		if start < 0 {
			start = 0
		}
		n, _ = file.ReadAt(buf, start)
		hasher.Write(buf[:n])
	}

	return csvDNA{
		size:  size,
		mtime: mtime,
		hash:  hex.EncodeToString(hasher.Sum(nil)),
	}, nil
}

// Cleanup removes temp files
func (indexer *Indexer) Cleanup() {
	// Remove temp directory
	if indexer.tempDir != "" {
		_ = os.RemoveAll(indexer.tempDir)
	}
}

// startReporting
func (indexer *Indexer) startReporting() {
	if !indexer.config.Verbose {
		return
	}
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()

		startTime := time.Now()

		for {
			select {
			case <-ticker.C:
				indexer.printStatus(startTime)
			case <-indexer.stopReport:
				fmt.Println() // New line after progress
				return
			}
		}
	}()
}

func (indexer *Indexer) stopReporting() {
	if !indexer.config.Verbose {
		return
	}
	close(indexer.stopReport)
}

func (indexer *Indexer) printStatus(startTime time.Time) {
	rowsScanned, bytesScanned, _ := indexer.scanner.GetStats()

	indexer.sorterMutex.RLock()
	sorters := make([]*Sorter, len(indexer.sorters))
	copy(sorters, indexer.sorters)
	indexer.sorterMutex.RUnlock()

	// Determine phase
	phase := "Scanning"
	doneCount := 0
	mergingCount := 0
	for _, s := range sorters {
		st := s.GetStats()
		switch st.State {
		case StateMerging:
			mergingCount++
		case StateDone:
			doneCount++
		}
	}
	if doneCount == len(sorters) && len(sorters) > 0 {
		phase = "Done"
	} else if mergingCount > 0 {
		phase = "Merging"
	}

	// Calculate rate and ETA
	elapsed := time.Since(startTime)
	rate := float64(rowsScanned) / elapsed.Seconds()
	if rate == 0 {
		rate = 1
	}

	// Use file size to estimate total rows (if scanning)
	etaStr := "calculating..."
	if phase == "Scanning" && bytesScanned > 0 {
		// Estimate based on file size
		fileInfo, err := os.Stat(indexer.config.InputFile)
		if err == nil && fileInfo.Size() > 0 {
			progress := float64(bytesScanned) / float64(fileInfo.Size())
			if progress > 0 {
				totalTime := elapsed.Seconds() / progress
				remaining := time.Duration((totalTime - elapsed.Seconds()) * float64(time.Second))
				if remaining > 0 {
					etaStr = remaining.Round(time.Second).String()
				} else {
					etaStr = "finishing..."
				}
			}
		}
	} else if phase == "Merging" {
		etaStr = "merging..."
	} else if phase == "Done" {
		etaStr = "complete"
	}

	// Simple single-line output
	fmt.Printf("\r\033[K[%s] Rows: %d | Rate: %.0f/s | Elapsed: %s | ETA: %s",
		phase, rowsScanned, rate, elapsed.Round(time.Second), etaStr)
}
