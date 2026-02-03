package alter

import (
	"encoding/csv"
	"fmt"
	"os"
	"strings"

	"github.com/csvquery/csvquery/internal/schema"
)

// AlterConfig configures the alteration
type AlterConfig struct {
	CsvPath      string
	AddColumn    string
	DefaultValue string
	Materialize  bool
	Separator    string
}

// AlterTable handles CSV schema changes
type AlterTable struct {
	config AlterConfig
}

// NewAlterTable creates a new instance
func NewAlterTable(config AlterConfig) *AlterTable {
	if config.Separator == "" {
		config.Separator = ","
	}
	return &AlterTable{config: config}
}

// Run performs the alteration (O(1) Metadata or O(N) Rewrite)
func (a *AlterTable) Run() error {
	s, err := schema.Load(a.config.CsvPath)
	if err != nil {
		return fmt.Errorf("failed to load schema: %v", err)
	}

	// Check if column already exists (virtual)
	if _, exists := s.VirtualColumns[a.config.AddColumn]; exists && !a.config.Materialize {
		return fmt.Errorf("column '%s' already exists (virtual)", a.config.AddColumn)
	}

	// Check physical file
	inputFile, err := os.Open(a.config.CsvPath)
	if err != nil {
		return fmt.Errorf("failed to open csv: %v", err)
	}
	// We need to close inputFile later, but for materialization we might read it fully.
	// Let's handle closing in specific blocks or use a func.

	// Check header
	reader := csv.NewReader(inputFile)
	reader.Comma = rune(a.config.Separator[0])
	header, err := reader.Read()
	_ = inputFile.Close() // Close immediately after checking header

	if err == nil {
		for _, col := range header {
			if strings.EqualFold(col, a.config.AddColumn) {
				return fmt.Errorf("column '%s' already exists in physical file", a.config.AddColumn)
			}
		}
	}

	if a.config.Materialize {
		return a.materialize(s)
	}

	// Virtual Mode
	s.AddVirtualColumn(a.config.AddColumn, a.config.DefaultValue)
	if err := s.Save(); err != nil {
		return fmt.Errorf("failed to save schema: %v", err)
	}

	return nil
}

// materialize rewrites the CSV file to include the new column
func (a *AlterTable) materialize(s *schema.Schema) error {
	// 1. Open Input
	inputFile, err := os.Open(a.config.CsvPath)
	if err != nil {
		return err
	}
	defer func() { _ = inputFile.Close() }()

	reader := csv.NewReader(inputFile)
	reader.Comma = rune(a.config.Separator[0])

	// 2. Open Output (Temp)
	tempPath := a.config.CsvPath + ".tmp"
	outputFile, err := os.Create(tempPath)
	if err != nil {
		return err
	}
	defer func() { _ = outputFile.Close() }()

	writer := csv.NewWriter(outputFile)
	writer.Comma = rune(a.config.Separator[0])

	// 3. Process Header
	header, err := reader.Read()
	if err != nil {
		return fmt.Errorf("failed to read header: %v", err)
	}
	newHeader := append(header, a.config.AddColumn)
	if err := writer.Write(newHeader); err != nil {
		return err
	}

	// 4. Process Rows
	// Note: We don't support LazyQuotes/FieldsPerRecord checks here for simplicity,
	// but standard encoding/csv handles robust parsing.
	defaultValue := a.config.DefaultValue

	for {
		record, err := reader.Read()
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			return err
		}

		newRecord := append(record, defaultValue)
		if err := writer.Write(newRecord); err != nil {
			return err
		}
	}

	writer.Flush()
	if err := writer.Error(); err != nil {
		return err
	}

	// Explicit close to ensure flush
	_ = outputFile.Close()
	_ = inputFile.Close()

	// 5. Atomic Rename
	if err := os.Rename(tempPath, a.config.CsvPath); err != nil {
		return fmt.Errorf("failed to replace csv file: %v", err)
	}

	// 6. Cleanup Virtual Schema if needed
	// If this column was virtual, remove it now that it's physical
	if _, ok := s.VirtualColumns[a.config.AddColumn]; ok {
		s.RemoveVirtualColumn(a.config.AddColumn)
		if err := s.Save(); err != nil {
			return fmt.Errorf("failed to update schema after materialization: %v", err)
		}
	}

	return nil
}
