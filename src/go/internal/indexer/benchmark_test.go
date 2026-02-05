package indexer

import (
	"os"
	"strings"
	"testing"
)

func BenchmarkScanner(b *testing.B) {
	// Create a large dummy CSV file
	content := "col1,col2,col3,col4\n" + strings.Repeat("val1,val2,val3,val4\n", 10000)
	tmpFile, err := os.CreateTemp("", "bench_*.csv")
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = os.Remove(tmpFile.Name()) }()

	if _, err := tmpFile.WriteString(content); err != nil {
		b.Fatal(err)
	}
	_ = tmpFile.Close()

	// Init scanner
	s, err := NewScanner(tmpFile.Name(), ",")
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = s.Close() }()

	// Index definitions (scan col 0 and 2)
	indices := [][]int{{0}, {2}}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Reset file pos or logic?
		// Scanner maps the file. We can just scan it repeatedly?
		// But Scanner tracks state? No, Scan() creates chunks based on Workers.
		// It re-reads data from memory map.
		// BUT `s.Scan` sets `s.scanBytes`. It doesn't modify data.
		// So running Scan multiple times is safe.

		err := s.Scan(indices, func(workerID int, keys [][]byte, offset, line int64) {
			// No-op handler
			// mimicking usage: access keys
			_ = keys[0]
			_ = keys[1]
		})
		if err != nil {
			b.Fatal(err)
		}
	}
}
