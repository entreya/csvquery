package query

import (
	"bufio"
	"bytes"

	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/csvquery/csvquery/internal/common"
	"github.com/csvquery/csvquery/internal/schema"
	"github.com/csvquery/csvquery/internal/updatemgr"
)

// QueryConfig holds query parameters
type QueryConfig struct {
	CsvPath      string     // Path to CSV file
	IndexDir     string     // Directory containing .didx files
	Where        *Condition // Root of the filter tree
	Limit        int        // Max results (0 = no limit)
	Offset       int        // Skip first N results
	CountOnly    bool       // Only output count
	Explain      bool       // Output execution plan
	GroupBy      string     // Column to group by
	AggCol       string     // Column to aggregate
	AggFunc      string     // Aggregation function (count, sum, avg, min, max)
	Verbose      bool       // Output verbose logging
	DebugHeaders bool       // Debug raw headers detection
}

// QueryEngine executes queries against disk indexes
type QueryEngine struct {
	config          QueryConfig
	VirtualDefaults []string // Default values for virtual columns

	// Writer for output (defaults to stdout)
	Writer io.Writer

	// Updates
	Updates *updatemgr.UpdateManager
}

// NewQueryEngine creates a query engine
func NewQueryEngine(config QueryConfig) *QueryEngine {
	qe := &QueryEngine{
		config: config,
		Writer: os.Stdout,
	}

	// Load Updates
	if config.CsvPath != "" {
		if um, err := updatemgr.Load(config.CsvPath); err == nil {
			qe.Updates = um
		}
	}

	return qe
}

// applyUpdates applies overrides to the row
func (q *QueryEngine) applyUpdates(cols []string, overrides map[string]string, headers map[string]int) []string {
	// Create a copy to minimize side effects on internal buffers if needed,
	// but mostly we overwrite slots.
	// If cols are too short, we append? (Virtual columns)
	// runHelper fills virtual columns before this?
	// Yes, typically.

	for col, val := range overrides {
		if idx, ok := headers[col]; ok {
			if idx < len(cols) {
				cols[idx] = val
			} else {
				// If index is outside, maybe we need to extend?
				for len(cols) <= idx {
					cols = append(cols, "")
				}
				cols[idx] = val
			}
		}
	}
	return cols
}

// Run executes the query and outputs results
func (q *QueryEngine) Run() error {
	// 1. Validation & Setup
	if q.config.CsvPath == "" {
		return fmt.Errorf("csv path required")
	}
	totalStart := time.Now()

	// Allow count-only mode without WHERE or GROUP BY (counts all rows)
	if q.config.Where == nil && q.config.GroupBy == "" && !q.config.CountOnly {
		return fmt.Errorf("no WHERE conditions or GROUP BY specified")
	}

	// Fast path: COUNT(*) without filters - just count newlines in CSV
	if q.config.CountOnly && q.config.Where == nil && q.config.GroupBy == "" {
		return q.runCountAll()
	}

	// If Updates exist, we need special handling.
	// For MVP/Robustness, let's use Full Scan if Updates exist for now.
	if q.Updates != nil && len(q.Updates.Overrides) > 0 {
		return q.runFullScan()
	}

	// 1. Planning Phase
	// Find the best index (single or composite)
	indexPath, searchKey, hasSearchKey, plan, err := q.findBestIndex()
	if err != nil {
		// Fallback to Full Scan
		return q.runFullScan()
	}

	// OPTIMIZATION: If the index covers ALL conditions in Where, we can skip the post-filter.
	// This is critical for COUNT performance (avoids random access CSV reads).
	if q.config.Where != nil {
		if covered, ok := plan["covered_columns"].([]string); ok && len(covered) > 0 {
			// Check if all Where conditions are covered (case-insensitive)
			allCovered := true
			conds := q.config.Where.ExtractIndexConditions()

			for k := range conds {
				isCovered := false
				for _, c := range covered {
					// Use case-insensitive comparison because ExtractIndexConditions lowercases keys
					if strings.EqualFold(c, k) {
						isCovered = true
						break
					}
				}
				if !isCovered {
					if q.config.Verbose {
						fmt.Fprintf(os.Stderr, "DEBUG: Column '%s' NOT covered by index\n", k)
					}
					allCovered = false
					break
				}
			}

			if allCovered {
				// Perfect match! Disable post-filter.
				// For Count(*) this means we never touch the CSV file (only index).
				if q.config.Verbose {
					fmt.Fprintln(os.Stderr, "DEBUG: All WHERE conditions covered by index. Disabling post-filter.")
				}
				q.config.Where = nil
			} else {
				if q.config.Verbose {
					fmt.Fprintln(os.Stderr, "DEBUG: Not all WHERE conditions covered.")
				}
			}
		} else {
			if q.config.Verbose {
				fmt.Fprintf(os.Stderr, "DEBUG: No covered_columns in plan. plan=%v\n", plan)
			}
		}
	}

	if q.config.Explain {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(plan)
	}

	// 2. Execution Phase (Index Lookup)
	execStart := time.Now()

	// Open index file
	indexFile, err := os.Open(indexPath)
	if err != nil {
		return fmt.Errorf("failed to open index: %w", err)
	}
	defer func() { _ = indexFile.Close() }()

	// Initialize BlockReader
	br, err := common.NewBlockReader(indexFile)
	if err != nil {
		return fmt.Errorf("failed to init block reader: %w", err)
	}

	// Try bloom filter first (only if we have a valid search key)
	if hasSearchKey {
		bloomPath := indexPath + ".bloom"
		if _, err := os.Stat(bloomPath); err == nil {
			bloom, bloomCleanup, err := common.LoadBloomFilterMmap(bloomPath)
			if err == nil {
				if bloomCleanup != nil {
					defer bloomCleanup()
				}
				if !bloom.MightContain(searchKey) {
					// Key definitely not in index
					if q.config.CountOnly {
						fmt.Println("0")
					}
					// Metrics even for 0 result
					q.printMetrics(totalStart, execStart, time.Now())
					return nil
				}
			}
		}
	}

	// Identify Candidate Blocks
	startBlockIdx := 0
	endBlockIdx := len(br.Footer.Blocks) - 1

	if hasSearchKey {
		// Binary search in Sparse Index to find the first block that COULD contain the key
		startBlockIdx = q.findStartBlock(br.Footer, searchKey)
		if startBlockIdx == -1 {
			if q.config.CountOnly {
				fmt.Println("0")
			}
			q.printMetrics(totalStart, execStart, time.Now())
			return nil
		}
		endBlockIdx = len(br.Footer.Blocks) - 1
	}

	// execTime := time.Since(execStart)
	// fetchStart := time.Now()

	// 3. Fetching Phase (Scanning Blocks & Output)
	// Dispatch to Aggregation or Standard Output
	var runErr error
	if q.config.GroupBy != "" {
		// Use plan["index"] to check if we are scanning the GroupBy index
		indexName, _ := plan["index"].(string)
		runErr = q.runAggregation(br, searchKey, hasSearchKey, startBlockIdx, endBlockIdx, indexName)
	} else {
		runErr = q.runStandardOutput(br, searchKey, hasSearchKey, startBlockIdx, endBlockIdx)
	}

	if runErr != nil {
		return runErr
	}

	// Output Metrics to Stderr
	// fmt.Fprintf(os.Stderr, "Time-Execution: %v\n", execTime)
	// fmt.Fprintf(os.Stderr, "Time-Fetching: %v\n", time.Since(fetchStart))
	// fmt.Fprintf(os.Stderr, "Time-Total: %v\n", time.Since(totalStart))

	return nil
}

func (q *QueryEngine) printMetrics(totalStart, execStart, fetchStart time.Time) {
	// No-op
}

// runCountAll counts all data rows in the CSV file (excluding header)
// This is an optimized path for COUNT(*) without any filters.
// First tries to count from index metadata (instant), then falls back to CSV scan.
func (q *QueryEngine) runCountAll() error {
	// OPTIMIZATION: Try counting from index metadata first (O(blocks) instead of O(file))
	if count, ok := q.tryCountFromIndex(); ok {
		_, _ = fmt.Fprintln(q.Writer, count)
		return nil
	}

	// Fallback: Count newlines in CSV file
	return q.runCountAllViaCsv()
}

// tryCountFromIndex attempts to count records by summing RecordCount from index blocks.
// Returns (count, true) if successful, (0, false) if no usable index.
func (q *QueryEngine) tryCountFromIndex() (int64, bool) {
	if q.config.IndexDir == "" {
		return 0, false
	}

	// Find any .cidx file for this CSV
	csvBase := filepath.Base(q.config.CsvPath)
	csvBase = strings.TrimSuffix(csvBase, filepath.Ext(csvBase))
	pattern := filepath.Join(q.config.IndexDir, csvBase+"_*.cidx")
	matches, err := filepath.Glob(pattern)
	if err != nil || len(matches) == 0 {
		return 0, false
	}

	// Open first available index
	f, err := os.Open(matches[0])
	if err != nil {
		return 0, false
	}
	defer func() { _ = f.Close() }()

	br, err := common.NewBlockReader(f)
	if err != nil {
		return 0, false
	}

	// Sum RecordCount from all blocks
	var total int64
	for _, block := range br.Footer.Blocks {
		if block.RecordCount == 0 {
			// Old index format without RecordCount - fall back to CSV scan
			return 0, false
		}
		total += block.RecordCount
	}

	if q.config.Verbose {
		fmt.Fprintf(os.Stderr, "DEBUG: COUNT via index %s: %d records from %d blocks\n",
			filepath.Base(matches[0]), total, len(br.Footer.Blocks))
	}

	return total, true
}

// runCountAllViaCsv counts newlines in CSV file using parallel workers.
func (q *QueryEngine) runCountAllViaCsv() error {
	f, err := os.Open(q.config.CsvPath)
	if err != nil {
		return fmt.Errorf("failed to open CSV: %w", err)
	}
	defer func() { _ = f.Close() }()

	// Memory-map the file
	data, err := common.MmapFile(f)
	if err != nil {
		return fmt.Errorf("failed to mmap CSV: %w", err)
	}
	defer func() { _ = common.MunmapFile(data) }()

	if len(data) == 0 {
		_, _ = fmt.Fprintln(q.Writer, 0)
		return nil
	}

	// Calculate workers (default to NumCPU, max 16 for very simple task)
	workers := runtime.NumCPU()
	if workers > 16 {
		workers = 16
	}
	chunkSize := len(data) / workers
	if chunkSize < 1024*1024 { // Minimum 1MB per chunk
		workers = 1
		chunkSize = len(data)
	}

	var totalCount int64
	var wg sync.WaitGroup
	var mu sync.Mutex

	for i := 0; i < workers; i++ {
		start := i * chunkSize
		end := start + chunkSize
		if i == workers-1 {
			end = len(data)
		}

		wg.Add(1)
		go func(chunk []byte) {
			defer wg.Done()
			// bytes.Count is highly optimized (SIMD/Assembly)
			c := int64(bytes.Count(chunk, []byte{'\n'}))
			mu.Lock()
			totalCount += c
			mu.Unlock()
		}(data[start:end])
	}

	wg.Wait()

	// Handle last line if no newline at EOF
	if len(data) > 0 && data[len(data)-1] != '\n' {
		totalCount++
	}

	// Subtract 1 for header row (assuming header exists if file not empty)
	// Logic matches previous implementation: count-- if count > 0
	if totalCount > 0 {
		totalCount--
	}

	_, _ = fmt.Fprintln(q.Writer, totalCount)
	return nil
}

// findStartBlock finds the FIRST block that might contain the key.
func (q *QueryEngine) findStartBlock(sparse common.SparseIndex, key string) int {
	left, right := 0, len(sparse.Blocks)-1
	result := -1

	// Binary search for FIRST block where StartKey <= key
	for left <= right {
		mid := (left + right) / 2
		if sparse.Blocks[mid].StartKey <= key {
			result = mid
			left = mid + 1 // Continue searching right for rightmost match
		} else {
			right = mid - 1
		}
	}

	if result == -1 {
		return -1 // Key is smaller than all blocks
	}

	// Backtrack to first block with this StartKey
	targetKey := sparse.Blocks[result].StartKey
	if targetKey == key {
		for result > 0 && sparse.Blocks[result-1].StartKey == key {
			result--
		}
	}

	return result
}

// compareRecordKey compares a fixed [64]byte index key (null-padded) against a search key.
// Zero allocations: no string conversion, no TrimRight copy.
func compareRecordKey(key *[64]byte, searchKey []byte) int {
	// Find effective length by scanning backwards past null bytes
	keyLen := 64
	for keyLen > 0 && key[keyLen-1] == 0 {
		keyLen--
	}
	return bytes.Compare(key[:keyLen], searchKey)
}

// runStandardOutput outputs matching records via stdout
func (q *QueryEngine) runStandardOutput(br *common.BlockReader, searchKey string, hasSearchKey bool, startBlockIdx, endBlockIdx int) error {
	// Read Headers & Setup Context for filtering
	headers, virtualDefaults, err := q.getHeaderMap()
	if err != nil {
		return fmt.Errorf("failed to read headers: %v", err)
	}
	q.VirtualDefaults = virtualDefaults

	var csvF *os.File
	var csvData []byte

	// Helper to load CSV only when needed
	ensureCsvLoaded := func() error {
		if csvData != nil {
			return nil
		}
		var err error
		csvF, err = os.Open(q.config.CsvPath)
		if err != nil {
			return err
		}
		csvData, err = common.MmapFile(csvF)
		return err
	}
	defer func() {
		if csvData != nil {
			_ = common.MunmapFile(csvData)
		}
		if csvF != nil {
			_ = csvF.Close()
		}
	}()

	// Determine MaxCol for extraction
	maxCol := -1
	if q.config.Where != nil {
		for _, idx := range headers {
			if idx > maxCol {
				maxCol = idx
			}
		}
	}

	count := int64(0)
	skipped := 0
	limitReached := false

	writer := bufio.NewWriter(q.Writer)
	defer func() { _ = writer.Flush() }()

	searchKeyBytes := []byte(searchKey)
	rowMap := make(map[string]string, len(headers))
	colsBuf := make([]string, 0, maxCol+1)

	for i := startBlockIdx; i <= endBlockIdx; i++ {
		if limitReached {
			break
		}

		blockMeta := br.Footer.Blocks[i]
		fmt.Fprintf(os.Stderr, "DEBUG: Processing Block %d: Key=%s Len=%d\n", i, blockMeta.StartKey, blockMeta.Length)

		if hasSearchKey && blockMeta.StartKey > searchKey {
			break
		}

		records, err := br.ReadBlock(blockMeta)
		if err != nil {
			return err
		}

		for index := range records {
			// use pointer to avoid copying 80 bytes
			rec := &records[index]
			if hasSearchKey {
				cmp := compareRecordKey(&rec.Key, searchKeyBytes)
				if cmp < 0 {
					continue
				}
				if cmp > 0 {
					limitReached = true
					break
				}
			}

			// Read CSV Line
			if q.config.Where != nil || !q.config.CountOnly {
				if err := ensureCsvLoaded(); err != nil {
					return err
				}
				// fmt.Fprintf(os.Stderr, "DEBUG: csvData len=%d cap=%d ptr=%p path=%s\n", len(csvData), cap(csvData), csvData, q.config.CsvPath)
				if len(csvData) == 0 {
					return fmt.Errorf("CRITICAL: csvData is empty! Path: %s", q.config.CsvPath)
				}

				rowEnd := bytes.IndexByte(csvData[rec.Offset:], '\n')
				if rowEnd == -1 {
					rowEnd = len(csvData) - int(rec.Offset)
				}
				row := csvData[rec.Offset : int(rec.Offset)+rowEnd]
				row = bytes.TrimSuffix(row, []byte{'\r'})

				// Post-Filter (Where)
				if q.config.Where != nil {
					// Extract cols for filtering
					cols := extractCols(row, ',', maxCol, colsBuf)

					// Inject Virtual Columns
					if len(q.VirtualDefaults) > 0 {
						cols = append(cols, q.VirtualDefaults...)
					}

					clear(rowMap)
					for k, v := range headers {
						if v < len(cols) {
							rowMap[strings.ToLower(k)] = cols[v]
						} else {
							rowMap[strings.ToLower(k)] = "" // Default to empty string
						}
					}
					if !q.config.Where.Evaluate(rowMap) {
						continue
					}
					// Update reuse buffer
					colsBuf = cols
				}
			}

			if skipped < q.config.Offset {
				skipped++
				continue
			}

			count++
			if !q.config.CountOnly {
				_, _ = fmt.Fprintf(writer, "%d,%d\n", rec.Offset, rec.Line)
			}

			if q.config.Limit > 0 && count >= int64(q.config.Limit) {
				limitReached = true
				break
			}
		}
	}

	if q.config.CountOnly {
		_, _ = fmt.Fprintln(writer, count)
	}
	return nil
}

// runAggregation performs GroupBy and Aggregation
func (q *QueryEngine) runAggregation(br *common.BlockReader, searchKey string, hasSearchKey bool, startBlockIdx, endBlockIdx int, indexName string) error {
	headers, virtualDefaults, err := q.getHeaderMap()
	if err != nil {
		return fmt.Errorf("failed to read headers: %v", err)
	}
	q.VirtualDefaults = virtualDefaults // Store for use in loop

	// Check Optimization Eligibility
	isGroupingByIndex := strings.EqualFold(indexName, q.config.GroupBy)

	// Pre-calculate if we can perform metadata-only aggregation
	// We can skip scan if:
	// 1. We are grouping by the index column (checked above)
	// 2. The block is Distinct (checked per block)
	// 3. We are doing COUNT or DISTINCT (not SUM/AVG which need values)
	canUseMetadata := q.config.AggFunc == "count" || q.config.AggFunc == ""

	// fmt.Fprintf(os.Stderr, "DEBUG-GROUPBY: indexName=%q, GroupBy=%q, AggFunc=%q, isDistinctMode=%v, canSkipScan=%v, blocks=%d\n", ...

	var csvF *os.File
	var csvData []byte

	// Helper to load CSV only when needed
	ensureCsvLoaded := func() error {
		if csvData != nil {
			return nil
		}
		var err error
		csvF, err = os.Open(q.config.CsvPath)
		if err != nil {
			return err
		}
		csvData, err = common.MmapFile(csvF)
		return err
	}
	defer func() {
		if csvData != nil {
			_ = common.MunmapFile(csvData)
		}
		if csvF != nil {
			_ = csvF.Close()
		}
	}()

	// Setup Columns
	groupKey := strings.ToLower(q.config.GroupBy)
	groupC, ok := headers[groupKey]
	if !ok {
		var avail []string
		for k := range headers {
			avail = append(avail, k)
		}
		return fmt.Errorf("column '%s' not found. Available: %v", q.config.GroupBy, avail)
	}
	aggC := 0
	if q.config.AggCol != "" && q.config.AggCol != "*" {
		var ok bool
		aggC, ok = headers[strings.ToLower(q.config.AggCol)]
		if !ok {
			return fmt.Errorf("aggregation column '%s' not found", q.config.AggCol)
		}
	}
	isCountOnly := q.config.AggFunc == "count"

	maxCol := groupC
	if aggC > maxCol {
		maxCol = aggC
	}
	// If filtering is enabled, we must extract columns involved in the filter.
	if q.config.Where != nil {
		for _, idx := range headers {
			if idx > maxCol {
				maxCol = idx
			}
		}
	}

	results := make(map[string]float64)
	counts := make(map[string]int64)

	limitReached := false

	searchKeyBytes := []byte(searchKey)
	rowMap := make(map[string]string, len(headers))
	colsBuf := make([]string, 0, maxCol+1)

	for i := startBlockIdx; i <= endBlockIdx; i++ {
		if limitReached {
			break
		}

		blockMeta := br.Footer.Blocks[i]

		if hasSearchKey && blockMeta.StartKey > searchKey {
			break
		}

		// *** ULTRA-FAST DISTINCT/COUNT SCAN ***
		// If block contains only one key, we can skip reading it entirely!
		if isGroupingByIndex && blockMeta.IsDistinct && canUseMetadata {
			groupKey := blockMeta.StartKey

			// Handle Limit/Offset/Search logic if needed (search is handled by loop range/break checks)

			if q.config.AggFunc == "count" {
				// For count, add the number of records in this block
				results[groupKey] += float64(blockMeta.RecordCount)
			} else {
				// For distinct, just mark presence
				results[groupKey] = 1
			}
			continue // Skip ReadBlock!
		}

		// Read Block (for mixed blocks or data aggregation)
		records, err := br.ReadBlock(blockMeta)
		if err != nil {
			return err
		}

		if csvData == nil {
			if err := ensureCsvLoaded(); err != nil {
				return err
			}
		}

		for index := range records {
			rec := &records[index]
			if hasSearchKey {
				cmp := compareRecordKey(&rec.Key, searchKeyBytes)
				if cmp < 0 {
					continue
				}
				if cmp > 0 {
					limitReached = true
					break
				}
			}

			// Read CSV Line
			rowEnd := bytes.IndexByte(csvData[rec.Offset:], '\n')
			if rowEnd == -1 {
				rowEnd = len(csvData) - int(rec.Offset)
			}
			row := csvData[rec.Offset : int(rec.Offset)+rowEnd]
			row = bytes.TrimSuffix(row, []byte{'\r'})

			cols := extractCols(row, ',', maxCol, colsBuf)

			// Inject Virtual Columns
			if len(q.VirtualDefaults) > 0 {
				cols = append(cols, q.VirtualDefaults...)
			}

			var groupVal string
			if groupC < len(cols) {
				groupVal = cols[groupC]
			}

			// Where Filter
			if q.config.Where != nil {
				clear(rowMap)
				for k, v := range headers {
					if v < len(cols) {
						rowMap[strings.ToLower(k)] = cols[v]
					} else {
						rowMap[strings.ToLower(k)] = ""
					}
				}
				if !q.config.Where.Evaluate(rowMap) {
					continue
				}
			}

			var val float64
			if !isCountOnly && aggC < len(cols) {
				val, _ = strconv.ParseFloat(cols[aggC], 64)
			}

			switch q.config.AggFunc {
			case "count":
				results[groupVal]++
			case "sum":
				results[groupVal] += val
			case "min":
				if curr, ok := results[groupVal]; !ok || val < curr {
					results[groupVal] = val
				}
			case "max":
				if curr, ok := results[groupVal]; !ok || val > curr {
					results[groupVal] = val
				}
			case "avg":
				results[groupVal] += val
				counts[groupVal]++
			case "": // Distinct Mode (Implicit)
				results[groupVal] = 1
			}

			// Recapture buffer ownership (not strictly needed since we use colsBuf every iteration, but good practice)
			colsBuf = cols
		}
	}

	// delete(results, "") - Allow empty keys as valid groups

	return json.NewEncoder(q.Writer).Encode(results)
}

// extractCols extraction columns from a byte slice line without excessive allocation
func extractCols(line []byte, sep byte, maxCol int, buf []string) []string {
	cols := buf[:0]
	start := 0
	inQuote := false
	for i := 0; i < len(line); i++ {
		if line[i] == '"' {
			inQuote = !inQuote
		}
		if line[i] == sep && !inQuote {
			val := string(line[start:i])
			// Trim quotes if present
			if len(val) >= 2 && val[0] == '"' && val[len(val)-1] == '"' {
				val = val[1 : len(val)-1]
			}
			cols = append(cols, val)
			start = i + 1
			if len(cols) > maxCol {
				return cols
			}
		}
	}
	val := string(line[start:])
	if len(val) >= 2 && val[0] == '"' && val[len(val)-1] == '"' {
		val = val[1 : len(val)-1]
	}
	cols = append(cols, val)
	return cols
}

// getHeaderMap returns map of column name -> index (including virtual columns)
func (q *QueryEngine) getHeaderMap() (map[string]int, []string, error) {
	f, err := os.Open(q.config.CsvPath)
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = f.Close() }()

	br := bufio.NewReader(f)
	// Check for BOM (Byte Order Mark)
	r, _, err := br.ReadRune()
	if err != nil {
		return nil, nil, err
	}
	if r != '\uFEFF' {
		_ = br.UnreadRune()
	}

	csvReader := csv.NewReader(br)
	header, err := csvReader.Read()
	if err != nil {
		return nil, nil, err
	}

	m := make(map[string]int)
	if q.config.DebugHeaders {
		fmt.Printf("DEBUG: Raw Headers found: %d\n", len(header))
	}
	for i, h := range header {
		// Sanitize: Trim space
		clean := strings.TrimSpace(h)

		// Debug print strict
		if q.config.DebugHeaders {
			fmt.Printf("  [%d] %q -> %q\n", i, h, clean)
		}

		// Normalize to lowercase for case-insensitive lookup
		m[strings.ToLower(clean)] = i
	}

	// Load Schema for Virtual Columns
	s, err := schema.Load(q.config.CsvPath)
	if err == nil {
		// Sort keys for deterministic order
		var keys []string
		for k := range s.VirtualColumns {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		var virtualDefaults []string
		startIdx := len(header)
		for _, k := range keys {
			if _, exists := m[k]; !exists {
				m[k] = startIdx
				startIdx++
				virtualDefaults = append(virtualDefaults, s.VirtualColumns[k])
			}
		}
		return m, virtualDefaults, nil
	}

	return m, nil, nil
}

// findBestIndex finds the best index for the query conditions
func (q *QueryEngine) findBestIndex() (string, string, bool, map[string]interface{}, error) {
	plan := make(map[string]interface{})
	plan["query"] = q.config.Where

	csvName := strings.TrimSuffix(filepath.Base(q.config.CsvPath), filepath.Ext(q.config.CsvPath))

	// 1. Try to find the best composite index
	if q.config.Where != nil {
		conds := q.config.Where.ExtractIndexConditions()
		if len(conds) > 0 {
			// Get all columns and sort them to match PHP's naming convention
			var cols []string
			for col := range conds {
				cols = append(cols, col)
			}
			sort.Strings(cols)

			// Try the longest possible composite index first, then shorter ones
			for i := len(cols); i >= 1; i-- {
				// For now, we only support exact matches on the leading columns of the query
				// Let's try the full combination
				currentCols := cols[:i]
				indexName := strings.Join(currentCols, "_")

				// Build search key matched to Indexer's format
				var searchKey string
				if i == 1 {
					searchKey = conds[currentCols[0]]
				} else {
					// Manual JSON construction
					var b strings.Builder
					b.WriteByte('[')
					for k, col := range currentCols {
						if k > 0 {
							b.WriteByte(',')
						}
						b.WriteByte('"')
						b.WriteString(conds[col])
						b.WriteByte('"')
					}
					b.WriteByte(']')
					searchKey = b.String()
				}

				// Try lowercase index path first (new convention after normalization fix)
				indexPath := filepath.Join(q.config.IndexDir, csvName+"_"+indexName+".cidx")
				if _, err := os.Stat(indexPath); err != nil {
					// Try uppercase (legacy index files created before normalization)
					upperIndexName := strings.ToUpper(indexName)
					altPath := filepath.Join(q.config.IndexDir, csvName+"_"+upperIndexName+".cidx")
					if _, err := os.Stat(altPath); err == nil {
						indexPath = altPath
					}
				}

				if _, err := os.Stat(indexPath); err == nil {
					plan["strategy"] = "Index Scan (Composite)"
					plan["index"] = indexName
					plan["covered_columns"] = currentCols
					return indexPath, searchKey, true, plan, nil
				}
			}
		}
	}

	// 2. Fallback: GroupBy index (Preferred for Aggregation)
	if q.config.GroupBy != "" {
		groupName := strings.ReplaceAll(q.config.GroupBy, ",", "_")
		indexPath := filepath.Join(q.config.IndexDir, csvName+"_"+groupName+".cidx")
		if info, err := os.Stat(indexPath); err == nil {
			if !info.IsDir() {
				plan["strategy"] = "GroupBy Index Scan"
				plan["index"] = groupName
				return indexPath, "", false, plan, nil
			}
		}
	}

	return "", "", false, nil, fmt.Errorf("no suitable index found")
}

// runFullScan scans the entire CSV file to find matching rows
func (q *QueryEngine) runFullScan() error {
	f, err := os.Open(q.config.CsvPath)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	// Map headers
	headers, virtualDefaults, err := q.getHeaderMap()
	if err != nil {
		return err
	}
	q.VirtualDefaults = virtualDefaults

	// Header Map for Indexing
	headerMap := make(map[string]int)
	for k, v := range headers {
		headerMap[k] = v
	}

	// Buffered Reader (Need ReadBytes for offset tracking)
	reader := bufio.NewReader(f)

	// Line Counting
	lineNum := int64(1) // Header is line 1
	currentOffset := int64(0)

	// Read Header Line to skip
	headerLine, err := reader.ReadBytes('\n')
	if err != nil {
		return err
	}
	currentOffset += int64(len(headerLine))

	// Output Writer
	writer := bufio.NewWriter(q.Writer)
	defer func() { _ = writer.Flush() }()

	// Metrics
	execStart := time.Now()
	count := int64(0)
	skipped := 0

	rowMap := make(map[string]string, len(headers))
	colsBuf := make([]string, 0, len(headers))

	// Max column index
	maxCol := 0
	for _, v := range headers {
		if v > maxCol {
			maxCol = v
		}
	}

	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			if err == io.EOF {
				if len(line) == 0 {
					break
				}
			} else {
				return err
			}
		}

		rowOffset := currentOffset
		currentOffset += int64(len(line))
		lineNum++

		// Trim whitespace/newlines
		trimmed := bytes.TrimSpace(line)

		cols := extractCols(trimmed, ',', maxCol, colsBuf)

		if len(q.VirtualDefaults) > 0 {
			cols = append(cols, q.VirtualDefaults...)
		}

		if q.Updates != nil {
			rowId := fmt.Sprintf("%d", lineNum) // Implicit RowID
			if override, exists := q.Updates.Overrides[rowId]; exists {
				cols = q.applyUpdates(cols, override, headerMap)
			}
		}

		if q.config.Where != nil {
			clear(rowMap)
			for k, v := range headers {
				if v < len(cols) {
					rowMap[strings.ToLower(k)] = cols[v]
				}
			}

			if !q.config.Where.Evaluate(rowMap) {
				continue
			}
		}

		if skipped < q.config.Offset {
			skipped++
			continue
		}

		count++
		if !q.config.CountOnly {
			_, _ = fmt.Fprintf(writer, "%d,%d\n", rowOffset, lineNum)
		}

		if q.config.Limit > 0 && count >= int64(q.config.Limit) {
			break
		}

		colsBuf = cols
	}

	if q.config.CountOnly {
		_, _ = fmt.Fprintln(writer, count)
	}

	// Metrics
	fmt.Fprintf(os.Stderr, "Full Scan Time: %v\n", time.Since(execStart))

	return nil
}
