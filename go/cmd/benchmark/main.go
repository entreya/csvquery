package main

import (
	"bufio"
	"fmt"
	"github.com/csvquery/csvquery/internal/indexer"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"time"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: benchmark <size_mb>")
		return
	}

	sizeMB := 500 // Default 500MB
	// Generate File
	fmt.Printf("Generating %d MB CSV...\n", sizeMB)
	tmpDir, _ := os.MkdirTemp("", "csv_bench")
	defer os.RemoveAll(tmpDir)

	csvPath := filepath.Join(tmpDir, "bench.csv")
	f, err := os.Create(csvPath)
	if err != nil {
		panic(err)
	}

	w := bufio.NewWriterSize(f, 64*1024)
	w.WriteString("id,code,value,description\n")

	// Write untils size reached
	bytesWritten := int64(0)
	limit := int64(sizeMB) * 1024 * 1024

	rows := 0
	buf := make([]byte, 0, 1024)

	rng := rand.New(rand.NewSource(123))

	for bytesWritten < limit {
		rows++
		// Faster string generation
		// id,code,value,description
		buf = buf[:0]
		buf = fmt.Appendf(buf, "%d,US-%d,%d,\"Description for item %d with some padding to make it longer\"\n", rows, rng.Intn(1000), rng.Intn(10000), rows)

		n, _ := w.Write(buf)
		bytesWritten += int64(n)
	}
	w.Flush()
	f.Close()

	fmt.Printf("Generated %d rows (%.2f MB)\n", rows, float64(bytesWritten)/1024/1024)

	// Benchmark
	fmt.Println("Starting Indexing...")

	cfg := indexer.IndexerConfig{
		InputFile:   csvPath,
		OutputDir:   tmpDir,
		Columns:     `["id", "code"]`,
		Separator:   ",",
		Workers:     runtime.NumCPU(),
		MemoryMB:    256,
		BloomFPRate: 0.01,
		Verbose:     true,
	}

	idx := indexer.NewIndexer(cfg)

	start := time.Now()
	if err := idx.Run(); err != nil {
		panic(err)
	}
	elapsed := time.Since(start)

	mbPerSec := float64(bytesWritten) / 1024 / 1024 / elapsed.Seconds()
	fmt.Printf("\n--------------------------------------------------\n")
	fmt.Printf("Throughput: %.2f MB/s\n", mbPerSec)
	fmt.Printf("Time:       %v\n", elapsed)
	fmt.Printf("--------------------------------------------------\n")
}
