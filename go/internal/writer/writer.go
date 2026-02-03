package writer

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
)

// WriterConfig holds configuration for the writer
type WriterConfig struct {
	CsvPath   string
	Separator string
}

// CsvWriter handles writing to CSV files
type CsvWriter struct {
	config WriterConfig
}

// NewCsvWriter creates a new writer instance
func NewCsvWriter(config WriterConfig) *CsvWriter {
	if config.Separator == "" {
		config.Separator = ","
	}
	return &CsvWriter{config: config}
}

// Write appends rows to the CSV file.
// If headers are provided and file doesn't exist, it creates the file with headers.
// If file exists, it validates headers match (if provided).
func (w *CsvWriter) Write(headers []string, rows [][]string) error {
	// Ensure directory exists
	dir := filepath.Dir(w.config.CsvPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %v", err)
	}

	// Open file with O_APPEND|O_CREATE|O_RDWR
	file, err := os.OpenFile(w.config.CsvPath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("failed to open file: %v", err)
	}
	defer file.Close()

	// Exclusive Lock
	if err := lockFile(file); err != nil {
		return fmt.Errorf("failed to lock file: %v", err)
	}
	defer unlockFile(file)

	// Check if file is new (size 0)
	stat, err := file.Stat()
	if err != nil {
		return err
	}

	csvW := csv.NewWriter(file)
	csvW.Comma = rune(w.config.Separator[0])

	// If new file, write headers
	if stat.Size() == 0 {
		if len(headers) == 0 {
			return fmt.Errorf("cannot create new file without headers")
		}
		if err := csvW.Write(headers); err != nil {
			return err
		}
	} else {
		// Existing file: Validate headers if provided
		if len(headers) > 0 {
			// Read first line to verify headers
			// We need to read from a separate handle or seek?
			// Since we opened with O_APPEND, read might be tricky?
			// Actually O_APPEND forces writes to end, but read pointer depends.
			// Let's rely on user correctness for now to avoid overhead?
			// OR validation is crucial.
			// Let's open a separate read handle for validation or seek to 0.
			// file is RDWR.

			// Seek to 0
			if _, err := file.Seek(0, 0); err != nil {
				return fmt.Errorf("failed to seek: %v", err)
			}

			reader := csv.NewReader(file)
			reader.Comma = rune(w.config.Separator[0])
			existingHeaders, err := reader.Read()
			if err != nil {
				return fmt.Errorf("failed to read existing headers: %v", err)
			}

			// Validate
			if !reflect.DeepEqual(existingHeaders, headers) {
				return fmt.Errorf("header mismatch. File: %v, New: %v", existingHeaders, headers)
			}

			// Seek back to end for writing?
			// O_APPEND handles writing position automatically (atomic append).
			// Seek affects read pointer. Write pointer is forced to end by O_APPEND.
		}
	}

	// Write Rows
	if err := csvW.WriteAll(rows); err != nil {
		return err
	}

	csvW.Flush()
	return csvW.Error()
}
