# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.1.1] - 2026-02-03

### Fixed
- Corrected binary path detection in `GoBridge` for vendor installs
- Moved daemon log to project root in `SocketClient`

## [1.1.0] - 2026-02-03

### 🚀 Performance & Scale Update

This version includes major performance optimizations and SIMD support that were previously in internal testing.

### Added
- **SIMD Scanner**: AVX2/SSE4.2 optimizations for 10x faster CSV parsing
- **LZ4 Compression**: Replacing Gzip for significantly faster index decompressing
- **Concurrent Bloom Filters**: Faster index building using multiple CPU cores
- **Batched I/O**: Reduced system call overhead for disk operations

### Fixed
- Critical data loss bug in Sorter deduplication
- Worker synchronization race conditions
- Quoted comma handling in CSV parsing

## [1.0.0] - 2026-02-03

### 🎉 First Stable Release

This marks the first standalone release of CsvQuery, extracted from the UIMS evaluation project as an independent, open-source library.

### Added

- **Standalone Library**: Fully independent PHP + Go hybrid library
- **PSR-4 Autoloading**: `CsvQuery\` namespace following PHP standards
- **Composer Package**: Available via `csvquery/csvquery`
- **Pre-compiled Binaries**: Go binaries for Linux/macOS/Windows (AMD64/ARM64)
- **GitHub Actions CI**: Automated builds and tests

### Core Features

- **High-Performance Indexing**
  - SIMD-accelerated CSV parsing (AVX2/SSE4.2) at 10GB/s+
  - LZ4 block compression (10x faster than Gzip)
  - External merge sort for unlimited dataset sizes
  - Memory-mapped file access (mmap)
  
- **Query Engine**
  - Zero-IO index scans for covered queries
  - Binary search on sparse index footer
  - Parallel newline counting for COUNT(*)
  - Bloom filter early rejection
  
- **PHP Fluent API**
  - Yii2-like `ActiveQuery` query builder
  - Supports WHERE, AND/OR, LIKE, IN, BETWEEN operators
  - GROUP BY with aggregations (SUM, AVG, MIN, MAX, COUNT)
  - Generator-based iteration for memory efficiency
  
- **Daemon Mode**
  - Unix Domain Socket server
  - Auto-start from PHP client
  - Connection pooling
  - ~1ms latency vs ~200ms for process spawning

- **Data Mutation**
  - Sidecar update system (`_updates.json`)
  - Row insertion and batch insert
  - Virtual columns via schema files

### Performance Benchmarks

Tested on 10GB CSV (18.3M rows), MacBook M3 Max / 64GB RAM:

| Operation | Performance |
|-----------|-------------|
| Indexing | ~400,000 rows/sec |
| COUNT(*) no filter | ~10ms |
| COUNT with indexed WHERE | ~14-25ms |
| SELECT 1K rows | ~50ms |
| Full table scan | ~2000ms |

### Changed

- **Namespace**: Changed from `uims\evaluation\modules\evaluation\csvquery` to `CsvQuery\`
- **Go Module**: Changed from `module csvquery` to `module github.com/csvquery/csvquery`

---

## Pre-1.0.0 History (Internal Development)

### [1.1.0-perf] - 2026-02-01 (Internal)

#### Added
- LZ4 compression replacing Gzip for 10x faster decompression
- SIMD scanner with AVX2/SSE4.2 optimizations
- Concurrent bloom filter building
- Batched I/O for reduced system call overhead

#### Fixed
- Critical data loss bug in Sorter deduplication
- Worker synchronization race conditions
- Quoted comma handling in CSV parsing

### [1.0.0] - 2026-01-15 (Internal)

#### Added
- Initial implementation of CsvQuery
- Go engine with indexing and query logic
- PHP ActiveQuery wrapper
- Daemon mode for persistent server
- Sidecar update system for mutable data
