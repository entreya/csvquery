#!/bin/bash
# Unified benchmark runner for CsvQuery vs PHP Baseline

set -e

CSV_FILE=$1

if [ -z "$CSV_FILE" ]; then
    echo "Usage: ./benchmarks/run_benchmark.sh <large_csv_file>"
    exit 1
fi

if [ ! -f "$CSV_FILE" ]; then
    echo "Error: File $CSV_FILE not found."
    exit 1
fi

echo "🚀 Starting Hyperfine Benchmark..."
hyperfine \
  --warmup 3 \
  "php benchmarks/php_baseline.php $CSV_FILE" \
  "./bin/csvquery_darwin_arm64 index --input $CSV_FILE --verbose" \
  --export-markdown benchmarks/results.md \
  --export-csv benchmarks/results.csv

echo -e "\n📊 Terminal Bar Chart:"
cut -d, -f1,2 benchmarks/results.csv | tail -n +2 | termgraph --title "Performance Comparison"

echo -e "\n🎨 Generating High-Resolution Plot..."
python3 benchmarks/visualize.py

echo -e "\n✅ Done! Check 'results.md' and 'benchmark_viz.png' in the benchmarks folder."
