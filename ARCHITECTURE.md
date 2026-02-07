# CsvQuery Architecture

> High-performance CSV query engine with Go-powered indexing and Yii2-like fluent API

## Overview

CsvQuery combines a **PHP wrapper** with a **Go-powered binary** to enable sub-second queries on CSV files with billions of rows using disk-based indexes.

```mermaid
graph LR
    A[PHP Client] -->|Fluent API| B[CsvQuery\Core]
    B -->|Conditions| C[ActiveQuery]
    C -->|Execute| D[GoBridge]
    D -->|spawn/socket| E[Go Binary]
    E -->|Binary Search| F[.cidx Index]
    F --> G[CSV File]
```

---

## PHP Module Structure

```
src/php/
├── Core/                     # Entry point
│   └── CsvQuery.php          # Main class, index management
├── Query/                    # Query building
│   ├── ActiveQuery.php       # Fluent interface, conditions
│   └── Command.php           # SQL-like debug output
├── Bridge/                   # Go communication
│   ├── GoBridge.php          # Binary wrapper, process spawning
│   └── SocketClient.php      # Unix socket daemon client
└── Models/                   # Data wrappers
    ├── Row.php               # Row object with ArrayAccess
    ├── Cell.php              # Cell value wrapper
    └── Column.php            # Column metadata
```

### Module Responsibilities

| Module | Purpose |
|--------|---------|
| **Core** | Entry point, CSV handling, index lifecycle |
| **Query** | Fluent API, condition building, execution |
| **Bridge** | Process management, IPC with Go binary |
| **Models** | Data representation, type casting |

---

## Go Package Structure

```
src/go/
├── main.go                   # CLI entry point
└── internal/
    ├── common/               # Shared types (IndexRecord, IndexMeta)
    ├── indexer/              # CSV indexing pipeline
    ├── query/                # Query engine, index selection
    ├── server/               # Unix socket daemon
    ├── simd/                 # SIMD-optimized parsing
    ├── alter/                # Schema modifications
    ├── update/               # Row update operations
    ├── updatemgr/            # Update file management
    ├── writer/               # CSV write operations
    └── schema/               # Virtual columns, schema files
```

---

## Data Flow

### Query Execution

```
1. PHP: $csv->where(['COL' => 'value'])->all()
2. ActiveQuery builds condition structure
3. GoBridge spawns: csvquery query --where '{"COL":"value"}'
4. Go: Query engine selects best index via metadata
5. Go: Binary search on .cidx file → returns offsets
6. PHP: Reads rows at offsets from CSV
7. Returns Row objects (or arrays with asArray())
```

### Index Creation

```
1. PHP: $csv->createIndex(['COL1', 'COL2'])
2. GoBridge spawns: csvquery index --columns '["COL1","COL2"]'
3. Go: SIMD parser reads CSV in parallel
4. Go: External merge sort handles memory limits
5. Go: Writes .cidx binary index + _meta.json
```

---

## Import Syntax

Use full modular namespace paths:

```php
use Entreya\CsvQuery\Core\CsvQuery;
use Entreya\CsvQuery\Query\ActiveQuery;
use Entreya\CsvQuery\Bridge\GoBridge;
use Entreya\CsvQuery\Models\Row;
```

---

## Key Design Decisions

1. **PHP ↔ Go Bridge**: Heavy computation (indexing, searching) offloaded to Go for performance
2. **Streaming Results**: Generator-based iteration avoids loading all results into memory
3. **Socket vs Spawn**: Unix socket client provides faster queries for repeated operations
4. **External Sort**: Enables indexing of files larger than available RAM
5. **Modular Namespaces**: Clean separation of concerns with explicit module paths
