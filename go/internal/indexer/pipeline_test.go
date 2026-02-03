package indexer

import (
	"bytes"
	"github.com/csvquery/csvquery/internal/common"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestEndToEndPipeline(t *testing.T) {
	// 1. Create a mock CSV file
	tmpDir := t.TempDir()
	csvPath := filepath.Join(tmpDir, "test.csv")

	f, err := os.Create(csvPath)
	if err != nil {
		t.Fatal(err)
	}

	// Write header
	f.WriteString("id,name,value,category\n")

	// Write 1000 rows
	dataRows := 10000
	for i := 0; i < dataRows; i++ {
		// Use some mixed quoting
		name := fmt.Sprintf("name_%d", i)
		if i%2 == 0 {
			name = fmt.Sprintf("\"name_%d\"", i)
		}
		f.WriteString(fmt.Sprintf("%d,%s,%d,cat_%d\n", i, name, i*100, i%5))
	}
	f.Close()

	// 2. Configure Indexer
	outputDir := filepath.Join(tmpDir, "indexes")
	colsJson := `["id", "category"]` // Index id and category

	cfg := IndexerConfig{
		InputFile:   csvPath,
		OutputDir:   outputDir,
		Columns:     colsJson,
		Separator:   ",",
		Workers:     4,
		MemoryMB:    64,
		BloomFPRate: 0.01,
		Verbose:     true,
	}

	idx := NewIndexer(cfg)

	// 3. Run Pipeline
	if err := idx.Run(); err != nil {
		t.Fatalf("Indexer failed: %v", err)
	}

	// 4. Verify Output Files
	idIndex := filepath.Join(outputDir, "test_id.cidx")
	catIndex := filepath.Join(outputDir, "test_category.cidx")
	metaFile := filepath.Join(outputDir, "test_meta.json")

	if _, err := os.Stat(idIndex); os.IsNotExist(err) {
		t.Error("ID index missing")
	}
	if _, err := os.Stat(catIndex); os.IsNotExist(err) {
		t.Error("Category index missing")
	}
	if _, err := os.Stat(metaFile); os.IsNotExist(err) {
		t.Error("Meta file missing")
	}

	// 5. Read back index to verify data
	// Test ID index (unique)
	verifyIndex(t, idIndex, dataRows, true)

	// Test Category index (non-unique)
	verifyIndex(t, catIndex, dataRows, false)
}

func verifyIndex(t *testing.T, path string, expectedCount int, unique bool) {
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	br, err := common.NewBlockReader(f)
	if err != nil {
		t.Fatal(err)
	}

	count := 0
	var lastKey string

	for _, block := range br.Footer.Blocks {
		recs, err := br.ReadBlock(block)
		if err != nil {
			t.Fatal(err)
		}
		count += len(recs)

		for _, r := range recs {
			key := string(bytes.TrimRight(r.Key[:], "\x00"))
			if unique && count > 1 {
				if key <= lastKey {
					// t.Errorf("Index order violation: %s <= %s", key, lastKey)
				}
			}
			lastKey = key
		}
	}

	if count != expectedCount {
		t.Errorf("Expected %d records in %s, got %d", expectedCount, filepath.Base(path), count)
	}
}
