# CsvQuery Codebase Analysis & Optimization Report

## Overview

**CsvQuery** is a high-performance CSV query engine that treats massive CSV files (10GB - 1TB+) like searchable databases. It uses a **PHP + Go Sidecar Architecture** to achieve sub-millisecond query latencies.

### Architecture Summary

```
PHP Application → CsvQuery → ActiveQuery → GoBridge → Go Binary (via UDS/Process)
                                                          ↓
                                                    Index Files (.cidx)
                                                    CSV File (mmap)
```

### Key Technologies
- **SIMD**: AVX2/SSE4.2 accelerated CSV parsing (10GB/s+)
- **Indexing**: LZ4-compressed sparse indexes (.cidx files)
- **Storage**: mmap-based zero-copy file access
- **IPC**: Unix Domain Sockets (~1ms latency vs ~200ms for process spawning)

---

## Identified Optimization Opportunities

### 1. **Redundant File Handle Operations** ⚠️ HIGH IMPACT
**Location**: `src/php/Query/ActiveQuery.php:775-780`

**Issue**: In `runFullScan()`, the file is opened, immediately closed, then reopened unnecessarily.

```php
$handle = fopen($this->csvQuery->getCsvPath(), 'r');
// ... get headers/separator ...
fclose($handle);
$handle = fopen($this->csvQuery->getCsvPath(), 'r'); // Redundant!
```

**Impact**: Unnecessary system calls and file descriptor churn.

**Fix**: Remove the redundant open/close.

---

### 2. **Inefficient Array Operations in Loops** ⚠️ MEDIUM IMPACT
**Location**: `src/php/Query/ActiveQuery.php:839-840, 859-860`

**Issue**: `array_flip($this->select)` is called repeatedly inside loops for each row.

```php
if ($this->select) {
    $row = array_intersect_key($row, array_flip($this->select)); // Called per row!
}
```

**Impact**: O(n) operation repeated for every row, wasting CPU cycles.

**Fix**: Cache the flipped array once before the loop.

---

### 3. **Virtual Column Injection Inefficiency** ⚠️ MEDIUM IMPACT
**Location**: `src/php/Query/ActiveQuery.php:799-809`

**Issue**: Virtual column injection uses a foreach loop that could be optimized with array operations.

**Impact**: Minor, but could be faster with array_slice/array_pad.

**Fix**: Use `array_pad()` for cleaner, potentially faster code.

---

### 4. **Group Key Construction** ⚠️ LOW-MEDIUM IMPACT
**Location**: `src/php/Query/ActiveQuery.php:824-827`

**Issue**: String concatenation in a loop for group keys.

```php
$groupKey = '';
foreach ($this->groupBy as $col) {
    $groupKey .= ($row[$col] ?? '') . '|';
}
```

**Impact**: String concatenation creates new strings each iteration.

**Fix**: Use `implode()` with array_map for better performance.

---

### 5. **Metadata Caching** ⚠️ LOW IMPACT
**Location**: `src/php/Core/CsvQuery.php:555-563`

**Issue**: `getMeta()` reads and parses JSON file on every call without caching.

**Impact**: Repeated file I/O for metadata that rarely changes.

**Fix**: Cache metadata with invalidation on index operations.

---

### 6. **Condition Evaluation String Operations** ⚠️ LOW IMPACT
**Location**: `src/php/Query/ActiveQuery.php:935-936`

**Issue**: `preg_quote()` and regex operations could be optimized for common patterns.

**Impact**: Minor, but LIKE queries could benefit from pattern caching.

---

### 7. **File Handle Lifecycle** ✅ GOOD
**Location**: `src/php/Core/CsvQuery.php:416-437`

**Status**: File handle is properly cached and reused in `readRowAt()`. Good optimization already in place.

---

### 8. **Socket Connection Management** ✅ GOOD
**Location**: `src/php/Bridge/SocketClient.php`

**Status**: Singleton pattern with connection reuse is well implemented. Auto-reconnect logic is robust.

---

## Recommended Optimizations (Priority Order)

### Priority 1: Critical Fixes
1. ✅ Remove redundant file open/close in `runFullScan()`
2. ✅ Cache `array_flip($this->select)` before loops

### Priority 2: Performance Improvements
3. ✅ Optimize group key construction with `implode()`
4. ✅ Optimize virtual column injection with `array_pad()`

### Priority 3: Nice-to-Have
5. Cache metadata with invalidation
6. Pattern caching for LIKE queries

---

## Performance Impact Estimates

| Optimization | Expected Improvement | Complexity |
|-------------|---------------------|------------|
| Remove redundant fopen | 5-10% faster full scans | Low |
| Cache array_flip | 2-5% faster queries with select | Low |
| Optimize group key | 1-3% faster GROUP BY queries | Low |
| Optimize virtual columns | <1% improvement | Low |

---

## Code Quality Observations

### Strengths ✅
- Well-structured architecture with clear separation of concerns
- Good use of generators for memory efficiency
- Proper resource cleanup (file handles, sockets)
- Comprehensive error handling
- Good documentation

### Areas for Improvement
- Some code duplication in condition evaluation
- Could benefit from more aggressive caching strategies
- Some methods are quite long (could be refactored)

---

## Conclusion

The codebase is well-architected and performs well. The identified optimizations are primarily micro-optimizations that will provide incremental performance improvements, especially for high-throughput scenarios. The most impactful changes are removing redundant file operations and caching array operations that are repeated in loops.
