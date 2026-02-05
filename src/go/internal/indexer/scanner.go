// Package main - Scanner module for CsvQuery
//
// Scanner provides high-performance CSV parsing using Memory Mapping (mmap) and Parallelism.
// It achieves gigabytes/sec throughput by avoiding data copying and utilizing all CPU cores.
//
// Level 1 Optimization:
//   - syscall.Mmap for zero-copy file access
//   - Parallel chunk processing using Goroutines
//
// Level 2 Optimization (SIMD/SWAR):
//   - SWAR (SIMD Within A Register) for delimiter detection (fallback for pure Go)
//   - Byte-level parsing to avoid unchecked string allocations
package indexer

import (
	"bytes"
	"fmt"
	"math/bits"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/csvquery/csvquery/internal/common"
	"github.com/csvquery/csvquery/internal/simd"
)

// Scanner reads CSV files efficiently using Mmap and Parallelism
type Scanner struct {
	filePath    string
	separator   byte // optimized for single byte separator
	headers     []string
	headerMap   map[string]int
	data        []byte // mmapped data
	fileSize    int64
	workers     int
	startTime   time.Time
	rowsScanned int64
	scanBytes   int64
}

// NewScanner creates a new Mmap-based CSV scanner
func NewScanner(filePath, separator string) (*Scanner, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer func() { _ = file.Close() }()

	stats, err := file.Stat()
	if err != nil {
		return nil, err
	}
	size := stats.Size()

	// Mmap the file
	data, err := common.MmapFile(file)
	if err != nil {
		return nil, err
	}

	scanner := &Scanner{
		filePath:  filePath,
		separator: separator[0], // assume single byte separator
		data:      data,
		fileSize:  size,
		workers:   runtime.NumCPU(),
		startTime: time.Now(),
	}

	// Read headers from the first line
	if err := scanner.readHeaders(); err != nil {
		_ = scanner.Close()
		return nil, err
	}

	return scanner, nil
}

// readHeaders parses the first row as column headers
func (scanner *Scanner) readHeaders() error {
	// Find first newline
	idx := bytes.IndexByte(scanner.data, '\n')
	if idx == -1 {
		return fmt.Errorf("empty or invalid csv")
	}

	line := scanner.data[:idx]
	// Handle CR
	if len(line) > 0 && line[len(line)-1] == '\r' {
		line = line[:len(line)-1]
	}

	// Handle BOM (EF BB BF)
	if len(line) >= 3 && line[0] == 0xEF && line[1] == 0xBB && line[2] == 0xBF {
		line = line[3:]
	} else if len(line) >= 2 && ((line[0] == 0xFF && line[1] == 0xFE) || (line[0] == 0xFE && line[1] == 0xFF)) {
		return fmt.Errorf("UTF-16 encoding detected but not supported. Please convert CSV to UTF-8")
	}

	// Parse headers
	parts := bytes.Split(line, []byte{scanner.separator})
	scanner.headers = make([]string, len(parts))
	scanner.headerMap = make(map[string]int)

	for i, part := range parts {
		name := string(bytes.TrimSpace(part)) // Trim whitespace
		// Trim quotes if present
		if len(name) >= 2 && name[0] == '"' && name[len(name)-1] == '"' {
			name = name[1 : len(name)-1]
		}
		scanner.headers[i] = name
		// Map is case-insensitive (store lower)
		scanner.headerMap[strings.ToLower(name)] = i
	}

	return nil
}

// GetColumnIndex returns the index of a column by name
func (scanner *Scanner) GetColumnIndex(name string) (int, bool) {
	idx, ok := scanner.headerMap[strings.ToLower(strings.TrimSpace(name))]
	return idx, ok
}

// GetHeaders returns all column headers
func (scanner *Scanner) GetHeaders() []string {
	return scanner.headers
}

// ValidateColumns checks if all requested columns exist
func (scanner *Scanner) ValidateColumns(columns []string) error {
	for _, col := range columns {
		normalized := strings.ToLower(strings.TrimSpace(col))
		if _, ok := scanner.headerMap[normalized]; !ok {
			// Use %q to show exact strings (reveals quoting/spacing/newlines)
			return fmt.Errorf("column not found: %s (detected %d headers: %q)", col, len(scanner.headers), scanner.headers)
		}
	}
	return nil
}

// SetWorkers sets the number of parallel workers
func (scanner *Scanner) SetWorkers(n int) {
	if n > 0 {
		scanner.workers = n
	}
}

// Scan processes the CSV in parallel
//
// Parameters:
//   - indexDefs: Array of column index definitions
//   - handler: Function called for each row (MUST be thread-safe)
func (scanner *Scanner) Scan(indexDefs [][]int, handler func(workerID int, keys [][]byte, offset, line int64)) error {
	// Find start of data (after header)
	startIdx := bytes.IndexByte(scanner.data, '\n') + 1
	if startIdx <= 0 || startIdx >= len(scanner.data) {
		return nil // End of file
	}

	dataSize := len(scanner.data)
	chunkSize := (dataSize - startIdx) / scanner.workers

	// CRITICAL FIX: Precompute ALL safe boundaries first to prevent gaps/overlaps.
	// boundaries[i] is the start position for worker i.
	// boundaries[workers] is dataSize (end sentinel).
	boundaries := make([]int, scanner.workers+1)
	boundaries[0] = startIdx
	boundaries[scanner.workers] = dataSize

	for i := 1; i < scanner.workers; i++ {
		hint := startIdx + (i * chunkSize)
		if hint < dataSize {
			boundaries[i] = findSafeRecordBoundary(scanner.data, hint)
		} else {
			boundaries[i] = dataSize
		}
	}

	// Launch workers with gap-free boundaries
	var wg sync.WaitGroup

	for i := 0; i < scanner.workers; i++ {
		start := boundaries[i]
		end := boundaries[i+1]

		// Skip empty chunks
		if start >= end {
			continue
		}

		wg.Add(1)
		go func(chunkStart, chunkEnd int, workerID int) {
			defer wg.Done()
			scanner.processChunk(chunkStart, chunkEnd, workerID, indexDefs, handler)
		}(start, end, i)
	}

	wg.Wait()
	scanner.scanBytes = int64(dataSize)
	return nil
}

// findSafeRecordBoundary finds the next newline that is NOT inside a quoted field
// hint is the approximate start position (from simple chunking)
func findSafeRecordBoundary(data []byte, hint int) int {
	// First, find the first newline at or after hint
	currentPosition := hint
	if currentPosition >= len(data) {
		return len(data)
	}

	// Seek to next newline
	nextNL := bytes.IndexByte(data[currentPosition:], '\n')
	if nextNL == -1 {
		return len(data)
	}
	currentPosition += nextNL

	currentNL := currentPosition

	loopCount := 0

	for {
		loopCount++

		// seek next newline
		if currentNL+1 >= len(data) {
			return len(data)
		}

		nextNL := bytes.IndexByte(data[currentNL+1:], '\n')
		if nextNL == -1 {
			// End of file is a valid boundary always
			return currentNL + 1
		}
		nextPos := currentNL + 1 + nextNL

		// Count quotes in data[currentNL+1 : nextPos]
		quotes := 0
		for i := currentNL + 1; i < nextPos; i++ {
			if data[i] == '"' {
				quotes++
			}
		}

		if quotes%2 == 0 {
			// Even quotes implies self-contained line.
			// So currentNL is a valid start.
			return currentNL + 1
		}

		// Odd quotes implies we are inside a multiline field or just finished one half of it.
		// So currentNL was invalid.
		// Try next one.
		currentNL = nextPos
	}
}

func (scanner *Scanner) processChunk(start, end int, workerID int, indexDefs [][]int, handler func(workerID int, keys [][]byte, offset, line int64)) {
	if start >= len(scanner.data) {
		return
	}

	// Clamp end to data length
	if end > len(scanner.data) {
		end = len(scanner.data)
	}

	// Skip if start >= end (can happen with small files and many workers)
	if start >= end {
		return
	}

	dataChunk := scanner.data[start:end]
	chunkLen := len(dataChunk)
	if chunkLen == 0 {
		return
	}

	sep := scanner.separator

	// Reusable buffers per worker
	keys := make([][]byte, len(indexDefs))

	// Pre-calculate max column needed
	maxCol := -1
	for _, indices := range indexDefs {
		for _, idx := range indices {
			if idx > maxCol {
				maxCol = idx
			}
		}
	}

	currentRowValues := make([][]byte, maxCol+1)
	scratchBuf := make([]byte, 0, 1024)

	// SIMD Phase: Generate bitmaps for the entire chunk
	bitmapLen := (chunkLen + 63) / 64
	quotesBitmap := make([]uint64, bitmapLen)
	sepsBitmap := make([]uint64, bitmapLen)
	newlinesBitmap := make([]uint64, bitmapLen)

	// Use custom separator if not comma
	if sep == ',' {
		simd.Scan(dataChunk, quotesBitmap, sepsBitmap, newlinesBitmap)
	} else {
		simd.ScanWithSeparator(dataChunk, sep, quotesBitmap, sepsBitmap, newlinesBitmap)
	}

	// Local accumulators for atomics optimization
	var localRowsScanned int64
	var localScanBytes int64

	// Parse using bitmaps
	lineStart := 0
	inQuote := false

	for wordIdx := 0; wordIdx < bitmapLen; wordIdx++ {
		quoteMask := quotesBitmap[wordIdx]
		newlineMask := newlinesBitmap[wordIdx]

		// Fast path: no structural chars in this 64-byte word
		if quoteMask == 0 && newlineMask == 0 && !inQuote {
			continue
		}

		// Process each set bit (quote or newline)
		combined := quoteMask | newlineMask
		for combined != 0 {
			tz := bits.TrailingZeros64(combined)
			bitMask := uint64(1) << tz
			combined &^= bitMask

			bytePos := wordIdx*64 + tz
			if bytePos >= chunkLen {
				break
			}

			isQuote := (quoteMask & bitMask) != 0
			isNewline := (newlineMask & bitMask) != 0

			if isQuote {
				inQuote = !inQuote
				continue
			}

			if isNewline && !inQuote {
				// End of line found
				lineEnd := bytePos
				lineBytes := dataChunk[lineStart:lineEnd]

				// Handle CR
				if len(lineBytes) > 0 && lineBytes[len(lineBytes)-1] == '\r' {
					lineBytes = lineBytes[:len(lineBytes)-1]
				}

				if len(lineBytes) > 0 {
					// Clear buffer
					for k := range currentRowValues {
						currentRowValues[k] = nil
					}

					// Parse line using SIMD bitmaps
					scanner.parseLineSimd(lineBytes, sep, int64(start+lineStart), workerID, indexDefs, handler, keys, currentRowValues, &scratchBuf, lineStart, quotesBitmap, sepsBitmap)
					localRowsScanned++
				}

				localScanBytes += int64(lineEnd - lineStart + 1)
				lineStart = bytePos + 1
			}
		}

		// Periodic progress update (every ~64KB)
		if wordIdx%1024 == 0 {
			atomic.AddInt64(&scanner.scanBytes, localScanBytes)
			atomic.AddInt64(&scanner.rowsScanned, localRowsScanned)
			localScanBytes = 0
			localRowsScanned = 0
		}
	}

	// Handle last line (no trailing newline)
	if lineStart < chunkLen && !inQuote {
		lineBytes := dataChunk[lineStart:]

		// Handle CR
		if len(lineBytes) > 0 && lineBytes[len(lineBytes)-1] == '\r' {
			lineBytes = lineBytes[:len(lineBytes)-1]
		}

		if len(lineBytes) > 0 {
			for k := range currentRowValues {
				currentRowValues[k] = nil
			}
			scanner.parseLineSimd(lineBytes, sep, int64(start+lineStart), workerID, indexDefs, handler, keys, currentRowValues, &scratchBuf, lineStart, quotesBitmap, sepsBitmap)
			localRowsScanned++
		}
		localScanBytes += int64(chunkLen - lineStart)
	}

	// Final update
	atomic.AddInt64(&scanner.scanBytes, localScanBytes)
	atomic.AddInt64(&scanner.rowsScanned, localRowsScanned)
}

// parseLineSimd parses a single line using pre-computed SIMD bitmaps.
// This enables quote-aware field extraction at hardware speed.
//
// Parameters:
//   - line: the raw line bytes (without newline)
//   - sep: separator byte
//   - offset: byte offset in the original file
//   - lineStartInChunk: where this line starts within the chunk (for bitmap indexing)
//   - quotesBitmap, sepsBitmap: pre-computed bitmaps from SIMD scan
func (scanner *Scanner) parseLineSimd(
	line []byte,
	sep byte,
	offset int64,
	workerID int,
	indexDefs [][]int,
	handler func(workerID int, keys [][]byte, offset, line int64),
	keys [][]byte,
	currentRowValues [][]byte,
	scratchBuf *[]byte,
	lineStartInChunk int,
	quotesBitmap, sepsBitmap []uint64,
) {
	maxCol := len(currentRowValues) - 1
	lineLen := len(line)

	if lineLen == 0 {
		return
	}

	// Extract fields using the bitmap
	colIdx := 0
	fieldStart := 0
	inQuote := false

	for i := 0; i < lineLen && colIdx <= maxCol; i++ {
		// Get the bitmap position (relative to chunk start)
		bitmapPos := lineStartInChunk + i
		wordIdx := bitmapPos / 64
		bitPos := uint(bitmapPos % 64)

		if wordIdx >= len(quotesBitmap) {
			// Fallback to byte-by-byte for safety
			break
		}

		isQuote := (quotesBitmap[wordIdx] & (1 << bitPos)) != 0
		isSep := (sepsBitmap[wordIdx] & (1 << bitPos)) != 0

		if isQuote {
			inQuote = !inQuote
			continue
		}

		if isSep && !inQuote {
			// End of field
			valBytes := line[fieldStart:i]
			// Trim quotes if present
			if len(valBytes) >= 2 && valBytes[0] == '"' && valBytes[len(valBytes)-1] == '"' {
				valBytes = valBytes[1 : len(valBytes)-1]
			}
			currentRowValues[colIdx] = valBytes
			colIdx++
			fieldStart = i + 1
		}
	}

	// Handle last field
	if colIdx <= maxCol && fieldStart <= lineLen {
		valBytes := line[fieldStart:]
		if len(valBytes) >= 2 && valBytes[0] == '"' && valBytes[len(valBytes)-1] == '"' {
			valBytes = valBytes[1 : len(valBytes)-1]
		}
		currentRowValues[colIdx] = valBytes
	}

	// Populate keys
	*scratchBuf = (*scratchBuf)[:0]

	for i, indices := range indexDefs {
		if len(indices) == 1 {
			idx := indices[0]
			if idx < len(currentRowValues) && currentRowValues[idx] != nil {
				keys[i] = currentRowValues[idx]
			} else {
				keys[i] = []byte{}
			}
		} else {
			startLen := len(*scratchBuf)
			*scratchBuf = append(*scratchBuf, '[')
			for j, idx := range indices {
				if j > 0 {
					*scratchBuf = append(*scratchBuf, ',')
				}
				*scratchBuf = append(*scratchBuf, '"')

				if idx < len(currentRowValues) && currentRowValues[idx] != nil {
					*scratchBuf = append(*scratchBuf, currentRowValues[idx]...)
				}
				*scratchBuf = append(*scratchBuf, '"')
			}
			*scratchBuf = append(*scratchBuf, ']')

			endLen := len(*scratchBuf)
			keys[i] = (*scratchBuf)[startLen:endLen]
		}
	}

	handler(workerID, keys, offset, 0)

	// Clear currentRowValues slots
	for k := 0; k < len(currentRowValues); k++ {
		currentRowValues[k] = nil
	}
}

// GetStats returns scanning statistics
func (scanner *Scanner) GetStats() (rowsScanned int64, bytesRead int64, elapsed time.Duration) {
	return atomic.LoadInt64(&scanner.rowsScanned), atomic.LoadInt64(&scanner.scanBytes), time.Since(scanner.startTime)
}

// Close releases resources
func (scanner *Scanner) Close() error {
	return common.MunmapFile(scanner.data)
}

// ScanProgress returns a human-readable progress string
func (scanner *Scanner) ScanProgress() string {
	elapsed := time.Since(scanner.startTime)
	mbRead := float64(scanner.fileSize) / 1024 / 1024
	return fmt.Sprintf("Scanned %.1f MB in %v", mbRead, elapsed.Round(time.Millisecond))
}
