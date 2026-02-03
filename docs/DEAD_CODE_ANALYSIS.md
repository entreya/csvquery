# Dead Code Analysis Report

> **Analysis Date**: 2026-02-03  
> **Status**: Completed  
> **Action**: Proactive removal

---

## Analysis Methodology

1. Identified all private methods in PHP classes (39 methods found)
2. Searched for call references within the codebase
3. Identified code from parent 'uims' project
4. Verified removal candidates don't break the test suite

---

## Dead Code Identified

### PHP: BloomFilter.__construct_internal (REMOVED)

**File**: `src/BloomFilter.php:66-80`  
**Status**: ❌ Unused - REMOVED

```php
// This method was never called - loadFromFile() sets properties directly instead
private function __construct_internal(int $size, int $hashCount, int $count, string $bits)
{
    $this->size = $size;
    $this->hashCount = $hashCount;
    $this->count = $count;
    $this->bits = $bits;
}
```

**Proof**: The `loadFromFile()` method creates a dummy instance with `new self(1)` then manually sets properties, never calling `__construct_internal()`.

---

## Legacy References Cleaned

### UIMS Comment (REMOVED)

**File**: `src/CsvQuery.php:35`  
**Original**: `@author UIMS Evaluation Team`  
**Changed to**: `@author CsvQuery Team`

---

## Verified Active Internal Code

The following private methods are **actively used** and were NOT removed:

| Method | File | Called From |
|--------|------|-------------|
| `readHeaders()` | CsvQuery.php | `__construct()` |
| `loadUpdates()` | CsvQuery.php | `applyOverrides()` |
| `calculateFingerprint()` | CsvQuery.php | `validateIntegrity()` |
| `detectBinary()` | GoBridge.php | `__construct()` |
| `queryCli()` | GoBridge.php | `query()` fallback |
| `streamOutput()` | GoBridge.php | `queryCli()` |
| `countViaSpawn()` | GoBridge.php | `count()` fallback |
| `execute()` | GoBridge.php | Multiple methods |
| `buildCommand()` | GoBridge.php | `execute()` |
| `validateExecution()` | GoBridge.php | `execute()` |
| `ensureConnected()` | SocketClient.php | `query()` |
| `connect()` | SocketClient.php | `ensureConnected()` |
| `reconnect()` | SocketClient.php | `query()` on failure |
| `startDaemon()` | SocketClient.php | `ensureConnected()` |
| `buildWhere()` | Command.php | `getQuery()` |
| `quoteValue()` | Command.php | `buildWhere()` |
| `getPositions()` | BloomFilter.php | `add()`, `mightContain()` |
| All ActiveQuery privates | ActiveQuery.php | Various query execution paths |

---

## Go Dead Code Analysis

No dead code identified in Go packages. All packages are actively used:

- `indexer/`: Used by `index` command
- `query/`: Used by `query` command and daemon
- `server/`: Used by `daemon` command
- `writer/`: Used by `write` command
- `common/`: Shared types used by all packages
- `simd/`: Performance optimizations for scanner
- `schema/`: Virtual column support
- `update/`, `updatemgr/`: Update handling

---

## Summary

| Category | Found | Removed |
|----------|-------|---------|
| Unused private methods | 1 | 1 |
| Legacy comments | 1 | 1 |
| Orphaned files | 0 | 0 |
| Go dead code | 0 | 0 |

**Total dead code removed**: 2 items
