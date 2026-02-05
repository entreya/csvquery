// Package main provides the CsvQuery indexer - a high-performance CSV indexing tool.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"syscall"

	"github.com/csvquery/csvquery/internal/indexer"
	"github.com/csvquery/csvquery/internal/query"
	"github.com/csvquery/csvquery/internal/server"
	"github.com/csvquery/csvquery/internal/writer"
)

// Version information
const (
	Version   = "1.6.7"
	BuildDate = "2026-02-05"
)

// Global state for graceful shutdown
var (
	shutdownChan = make(chan os.Signal, 1)
	cleanupFuncs []func()
)

func main() {
	setupSignalHandler()

	// Parse command
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	command := os.Args[1]

	switch command {
	case "index":
		runIndex(os.Args[2:])
	case "query":
		runQuery(os.Args[2:])
	case "daemon":
		runDaemon(os.Args[2:])
	case "write":
		runWrite(os.Args[2:])
	case "version":
		fmt.Printf("CsvQuery v%s (%s)\n", Version, BuildDate)
	case "help":
		printUsage()
	default:
		fmt.Printf("Unknown command: %s\n", command)
		printUsage()
		os.Exit(1)
	}
}

func setupSignalHandler() {
	signal.Notify(shutdownChan, os.Interrupt, syscall.SIGTERM)
	go handleShutdown()
}

// handleShutdown handles graceful shutdown on signals
func handleShutdown() {
	<-shutdownChan
	fmt.Fprintln(os.Stderr, "\n⚠️  Received shutdown signal, cleaning up...")

	// Run cleanup functions in reverse order
	for i := len(cleanupFuncs) - 1; i >= 0; i-- {
		cleanupFuncs[i]()
	}

	fmt.Fprintln(os.Stderr, "✅ Cleanup complete")
	os.Exit(130) // Standard exit code for SIGINT
}

func getDir(path string) string {
	return filepath.Dir(path)
}

func printUsage() {
	fmt.Println(`CsvQuery - High Performance CSV Indexer & Query Engine

Usage:
    csvquery <command> [arguments]

Commands:
    index    Create indexes from CSV
    query    Query CSV (using indexes if available)
    daemon   Start Unix Domain Socket server
    write    Append data to CSV
    version  Show version
    help     Show this help

Use "csvquery <command> --help" for command-specific options.`)
}

// runIndex handles the index command
func runIndex(args []string) {
	fs := flag.NewFlagSet("index", flag.ExitOnError)

	input := fs.String("input", "", "Input CSV file path")
	output := fs.String("output", "", "Output directory for indexes")
	columns := fs.String("columns", "[]", "JSON array of columns to index")
	separator := fs.String("separator", ",", "CSV separator")
	workers := fs.Int("workers", runtime.NumCPU(), "Number of parallel workers")
	memoryMB := fs.Int("memory", 500, "Memory limit in MB per worker")
	bloomFP := fs.Float64("bloom", 0.01, "Bloom filter false positive rate")
	verbose := fs.Bool("verbose", false, "Enable verbose output")

	_ = fs.Parse(args)

	if *input == "" {
		fmt.Fprintln(os.Stderr, "Error: --input is required")
		fs.PrintDefaults()
		os.Exit(1)
	}

	if *output == "" {
		*output = getDir(*input)
	}

	// Create indexer and run
	idx := indexer.NewIndexer(indexer.IndexerConfig{
		InputFile:   *input,
		OutputDir:   *output,
		Columns:     *columns,
		Separator:   *separator,
		Workers:     *workers,
		MemoryMB:    *memoryMB,
		BloomFPRate: *bloomFP,
		Verbose:     *verbose,
		Version:     Version,
	})

	// Register cleanup
	cleanupFuncs = append(cleanupFuncs, func() {
		// Stop indexer if running?
	})

	if err := idx.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

// runQuery handles the query command
func runQuery(args []string) {
	fs := flag.NewFlagSet("query", flag.ExitOnError)

	csvPath := fs.String("csv", "", "Path to CSV file")
	indexDir := fs.String("index-dir", "", "Directory containing index files")
	whereJSON := fs.String("where", "{}", "JSON object of conditions")
	limit := fs.Int("limit", 0, "Maximum results (0 = no limit)")
	offset := fs.Int("offset", 0, "Skip first N results")
	countOnly := fs.Bool("count", false, "Only output count")
	explain := fs.Bool("explain", false, "Explain query plan")
	groupBy := fs.String("group-by", "", "Column to group by")
	aggCol := fs.String("agg-col", "", "Column to aggregate")
	aggFunc := fs.String("agg-func", "", "Aggregation function")
	debugHeaders := fs.Bool("debug-headers", false, "Debug raw headers")

	_ = fs.Parse(args)

	// Default index-dir to CSV directory
	if *indexDir == "" && *csvPath != "" {
		*indexDir = filepath.Dir(*csvPath)
	}

	if *indexDir == "" {
		fmt.Fprintln(os.Stderr, "Error: --index-dir or --csv is required")
		fs.PrintDefaults()
		os.Exit(1)
	}

	// Parse WHERE conditions
	cond, err := query.ParseCondition([]byte(*whereJSON))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error parsing --where JSON: %v\nRaw JSON: %s\n", err, *whereJSON)
		os.Exit(1)
	}

	// Create and run query engine
	engine := query.NewQueryEngine(query.QueryConfig{
		CsvPath:      *csvPath,
		IndexDir:     *indexDir,
		Where:        cond,
		Limit:        *limit,
		Offset:       *offset,
		CountOnly:    *countOnly,
		Explain:      *explain,
		GroupBy:      *groupBy,
		AggCol:       *aggCol,
		AggFunc:      *aggFunc,
		DebugHeaders: *debugHeaders,
	})

	if err := engine.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		// os.Exit(1) // Don't exit on query error, just print (unless critical)
	}
}

// runDaemon handles the daemon command
func runDaemon(args []string) {
	fs := flag.NewFlagSet("daemon", flag.ExitOnError)

	socket := fs.String("socket", "/tmp/csvquery.sock", "Socket path")
	csvPath := fs.String("csv", "", "Path to CSV")
	indexDir := fs.String("index-dir", "", "Index directory")
	workers := fs.Int("workers", 50, "Max concurrency")

	_ = fs.Parse(args)

	if err := server.RunDaemon(*socket, *csvPath, *indexDir, *workers); err != nil {
		fmt.Fprintf(os.Stderr, "Daemon Error: %v\n", err)
		os.Exit(1)
	}
}

// runWrite handles the write command
func runWrite(args []string) {
	fs := flag.NewFlagSet("write", flag.ExitOnError)

	csvPath := fs.String("csv", "", "Path to CSV file")
	headersJSON := fs.String("headers", "[]", "JSON array of headers (for new file)")
	dataJSON := fs.String("data", "[]", "JSON array of rows (each row is array of strings)")
	separator := fs.String("separator", ",", "CSV separator")

	_ = fs.Parse(args)

	if *csvPath == "" {
		fmt.Fprintln(os.Stderr, "Error: --csv is required")
		os.Exit(1)
	}

	var headers []string
	_ = json.Unmarshal([]byte(*headersJSON), &headers)

	var data [][]string
	_ = json.Unmarshal([]byte(*dataJSON), &data)

	w := writer.NewCsvWriter(writer.WriterConfig{
		CsvPath:   *csvPath,
		Separator: *separator,
	})
	if err := w.Write(headers, data); err != nil {
		fmt.Fprintf(os.Stderr, "Write Error: %v\n", err)
		os.Exit(1)
	}
}
