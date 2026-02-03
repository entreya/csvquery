# CsvQuery API Reference

> **Version**: 1.0.0  
> **Compatibility Contract**: All APIs documented here are **public and stable**.

---

## PHP API

### CsvQuery (Entry Point)

**Namespace**: `CsvQuery\CsvQuery`

#### Constructor

```php
public function __construct(string $csvPath, array $options = [])
```

**Options**:
| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `indexDir` | string | CSV directory | Index storage location |
| `separator` | string | `,` | CSV delimiter |
| `workers` | int | CPU count | Indexing parallelism |
| `memoryMB` | int | 500 | Memory per worker |
| `binaryPath` | string | auto-detect | Go binary path |

#### Index Management

| Method | Signature | Description |
|--------|-----------|-------------|
| `createIndex` | `(array $columns, bool $verbose = false, array $options = []): bool` | Create indexes |
| `hasIndex` | `(string\|array $column): bool` | Check index existence |
| `dropIndex` | `(string\|array $column): bool` | Remove specific index |
| `clearIndexes` | `(): int` | Remove all indexes |
| `getIndexPath` | `(string\|array $column): string` | Get index file path |
| `validateIntegrity` | `(): bool` | Check if indexes are stale |

#### Query Factory

| Method | Signature | Description |
|--------|-----------|-------------|
| `find` | `(): ActiveQuery` | Start new query |
| `where` | `(string\|array $column, mixed $value = null): ActiveQuery` | Query with WHERE |
| `andWhere` | `(string\|array $column, mixed $value = null): ActiveQuery` | Query with AND |

#### Data Access

| Method | Signature | Description |
|--------|-----------|-------------|
| `getHeaders` | `(): array` | Column names |
| `getHeaderMap` | `(): array` | Column name → index map |
| `getVirtualColumns` | `(): array` | Virtual columns config |
| `readRowAt` | `(int $offset): ?array` | Read row at byte offset |
| `getMeta` | `(): array` | Index metadata |

#### Data Modification

| Method | Signature | Description |
|--------|-----------|-------------|
| `insert` | `(array $row): void` | Insert single row |
| `batchInsert` | `(array $rows): void` | Insert multiple rows |
| `update` | `(array $attributes, array $conditions = []): int` | Update rows |
| `addColumn` | `(string $name, string $default = '', bool $materialize = false): void` | Add column |

#### Utilities

| Method | Signature | Description |
|--------|-----------|-------------|
| `getCsvPath` | `(): string` | CSV file path |
| `getSeparator` | `(): string` | CSV separator |
| `getIndexDir` | `(): string` | Index directory |
| `getGoBridge` | `(): GoBridge` | Get bridge instance |

---

### ActiveQuery (Query Builder)

**Namespace**: `CsvQuery\ActiveQuery`

#### Condition Methods

| Method | Signature | Description |
|--------|-----------|-------------|
| `where` | `(array\|string\|null $condition, mixed $value = null): self` | Set WHERE |
| `andWhere` | `(array\|string $condition, mixed $value = null): self` | Add AND |
| `orWhere` | `(array\|string $condition, mixed $value = null): self` | Add OR |
| `filterWhere` | `(array $condition): self` | WHERE ignoring empty |
| `andFilterWhere` | `(array $condition): self` | AND ignoring empty |
| `orFilterWhere` | `(array $condition): self` | OR ignoring empty |

#### Query Modifiers

| Method | Signature | Description |
|--------|-----------|-------------|
| `select` | `(array $columns): self` | Select columns |
| `orderBy` | `(array\|string $columns): self` | Set ORDER BY |
| `addOrderBy` | `(array\|string $columns): self` | Add ORDER BY |
| `groupBy` | `(array\|string $columns): self` | Set GROUP BY |
| `addGroupBy` | `(array\|string $columns): self` | Add GROUP BY |
| `limit` | `(int $limit): self` | Set LIMIT |
| `offset` | `(int $offset): self` | Set OFFSET |
| `indexBy` | `(string\|callable $column): self` | Index results |
| `asArray` | `(bool $value = true): self` | Return arrays |
| `debug` | `(bool $enable = true): self` | Enable debug |

#### Execution Methods

| Method | Signature | Description |
|--------|-----------|-------------|
| `all` | `(): array` | Get all results |
| `one` | `(): ?Row` | Get first result |
| `count` | `(string $q = '*', $db = null): int` | Count results |
| `sum` | `($column): float` | Sum column |
| `average` | `($column): float` | Average column |
| `min` | `($column): mixed` | Minimum value |
| `max` | `($column): mixed` | Maximum value |
| `exists` | `(): bool` | Check if results exist |
| `scalar` | `(): mixed` | First column, first row |
| `column` | `(): array` | First column values |
| `batch` | `(int $batchSize = 100): Generator` | Iterate batches |
| `each` | `(int $batchSize = 100): Generator` | Iterate rows |

#### Aggregation

| Method | Signature | Description |
|--------|-----------|-------------|
| `aggregate` | `(string $columnOrGroup, array $aggregations): array` | Multi-aggregation |
| `aggregateValue` | `($column, $func): mixed` | Single aggregation |

#### Analysis

| Method | Signature | Description |
|--------|-----------|-------------|
| `explain` | `(string $format = 'array'): array\|string` | Query plan |
| `getStats` | `(string $format = 'array'): array\|string` | Execution stats |
| `createCommand` | `($sql = null): Command` | Get SQL-like command |

---

### GoBridge (Go Interface)

**Namespace**: `CsvQuery\GoBridge`

| Method | Signature | Description |
|--------|-----------|-------------|
| `__construct` | `(array $options = [])` | Create bridge |
| `createIndex` | `(string $csvPath, string $outputDir, string $columnsJson, string $separator = ',', bool $verbose = false, array $options = []): bool` | Create index |
| `query` | `(string $csvPath, string $indexDir, array $where, int $limit = 0, int $offset = 0, bool $explain = false, ?string $groupBy = null, ?string $aggCol = null, ?string $aggFunc = null): Generator\|array` | Execute query |
| `count` | `(string $csvPath, string $indexDir, array $where): int` | Count records |
| `write` | `(string $csvPath, array $rows, array $headers = [], string $separator = ','): void` | Write rows |
| `alter` | `(string $csvPath, string $columnName, string $defaultValue, string $separator = ',', bool $materialize = false): void` | Add column |
| `update` | `(string $csvPath, string $setClause, ?string $whereJson = null, string $indexDir = ''): int` | Update rows |
| `execute` | `(array $args, bool $passthrough = false): bool` | Execute binary |
| `getVersion` | `(): string` | Get version |
| `getBinaryPath` | `(): string` | Get binary path |
| `getLastStderr` | `(): string` | Get last stderr |

---

### SocketClient (UDS Connection)

**Namespace**: `CsvQuery\SocketClient`

| Method | Signature | Description |
|--------|-----------|-------------|
| `getInstance` | `(string $binaryPath = '', string $indexDir = ''): self` | Get singleton |
| `configure` | `(string $binaryPath, string $indexDir = '', string $socketPath = ''): void` | Configure |
| `isAvailable` | `(): bool` | Check daemon |
| `reset` | `(): void` | Reset singleton |
| `query` | `(string $action, array $params = []): array` | Generic query |
| `count` | `(string $csvPath, array $where = []): int` | Count rows |
| `select` | `(string $csvPath, array $where = [], int $limit = 0, int $offset = 0): array` | Select rows |
| `groupBy` | `(string $csvPath, string $column, string $aggFunc = 'count', array $where = []): array` | Group by |
| `ping` | `(): bool` | Ping daemon |
| `status` | `(): array` | Daemon status |

---

### Models

#### Row

**Namespace**: `CsvQuery\Models\Row`  
**Implements**: `ArrayAccess`, `IteratorAggregate`, `JsonSerializable`

| Method | Signature | Description |
|--------|-----------|-------------|
| `__construct` | `($csvQuery, array $data, $lineNumber = null)` | Create row |
| `getColumn` | `($name): Column` | Get column wrapper |
| `getCell` | `($name): Cell` | Get cell wrapper |
| `toAssociativeArray` | `(): array` | As array |
| `getLineNumber` | `(): ?int` | Line number |
| `toJson` | `($options = 0): string` | As JSON |

#### Cell

**Namespace**: `CsvQuery\Models\Cell`

| Method | Signature | Description |
|--------|-----------|-------------|
| `getValue` | `(): mixed` | Raw value |
| `isEmpty` | `(): bool` | Is empty |
| `isNumeric` | `(): bool` | Is numeric |
| `asString` | `(): string` | As string |
| `asInt` | `($default = 0): int` | As integer |
| `asFloat` | `($default = 0.0): float` | As float |
| `asBool` | `(): bool` | As boolean |
| `validate` | `(array $rules): array` | Validate |

#### Column

**Namespace**: `CsvQuery\Models\Column`

| Method | Signature | Description |
|--------|-----------|-------------|
| `getValue` | `(): mixed` | Raw value |
| `getName` | `(): ?string` | Column name |
| `getIndex` | `(): int` | Column index |
| `getCell` | `(): Cell` | As Cell |
| `trim` | `(): string` | Trimmed value |
| `toUpper` | `(): string` | Uppercase |
| `toLower` | `(): string` | Lowercase |

---

### Support Classes

#### Command

**Namespace**: `CsvQuery\Command`

| Method | Signature | Description |
|--------|-----------|-------------|
| `__construct` | `(array $config)` | Create command |
| `getQuery` | `(): string` | Get SQL-like query |

#### BloomFilter

**Namespace**: `CsvQuery\BloomFilter`

| Method | Signature | Description |
|--------|-----------|-------------|
| `__construct` | `(int $expectedElements, float $fpRate = 0.01)` | Create filter |
| `loadFromFile` | `(string $path): ?self` | Load from file |
| `add` | `(string $key): void` | Add element |
| `mightContain` | `(string $key): bool` | Check membership |
| `serialize` | `(): string` | To binary |
| `saveToFile` | `(string $path): void` | Save to file |
| `getStats` | `(): array` | Statistics |

---

## Go CLI API

### Commands

```bash
csvquery <command> [options]
```

| Command | Description |
|---------|-------------|
| `index` | Create indexes from CSV |
| `query` | Execute queries |
| `daemon` | Start UDS server |
| `write` | Append data to CSV |
| `version` | Show version |

### Index Command

```bash
csvquery index --input <csv> [options]
```

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--input` | string | required | Input CSV file |
| `--output` | string | CSV dir | Output directory |
| `--columns` | JSON | `[]` | Columns to index |
| `--separator` | string | `,` | CSV separator |
| `--workers` | int | CPU count | Parallel workers |
| `--memory` | int | 500 | Memory limit (MB) |
| `--bloom` | float | 0.01 | Bloom FP rate |
| `--verbose` | bool | false | Verbose output |

### Query Command

```bash
csvquery query --csv <path> [options]
```

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--csv` | string | required | CSV file path |
| `--index-dir` | string | CSV dir | Index directory |
| `--where` | JSON | `{}` | Query conditions |
| `--limit` | int | 0 | Max results |
| `--offset` | int | 0 | Skip results |
| `--count` | bool | false | Count only |
| `--explain` | bool | false | Show plan |
| `--group-by` | string | - | Group column |
| `--agg-col` | string | - | Agg column |
| `--agg-func` | string | - | Agg function |

### Daemon Command

```bash
csvquery daemon [options]
```

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--socket` | string | `/tmp/csvquery.sock` | Socket path |
| `--csv` | string | - | Default CSV |
| `--index-dir` | string | - | Index directory |
| `--workers` | int | 50 | Max concurrency |

### Write Command

```bash
csvquery write --csv <path> [options]
```

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--csv` | string | required | CSV file |
| `--headers` | JSON | `[]` | Headers (new file) |
| `--data` | JSON | `[]` | Rows to write |
| `--separator` | string | `,` | CSV separator |

---

## WHERE Condition Syntax

### Hash Format (Implicit AND)

```php
['STATUS' => 'active', 'TYPE' => 'premium']
```

### Operator Format

```php
['>', 'SCORE', 80]
['BETWEEN', 'AGE', 18, 65]
['IN', 'CATEGORY', ['A', 'B', 'C']]
['LIKE', 'NAME', '%john%']
['IS', 'VALUE', null]
['IS NOT', 'VALUE', null]
```

**Supported Operators**: `=`, `!=`, `>`, `<`, `>=`, `<=`, `LIKE`, `IN`, `NOT IN`, `BETWEEN`, `NOT BETWEEN`, `IS`, `IS NOT`

### Nested Conditions

```php
['OR',
    ['STATUS' => 'active'],
    ['AND',
        ['>', 'SCORE', 90],
        ['TYPE' => 'vip']
    ]
]
```

---

## Configuration Files

### Index Metadata (`.meta.json`)

```json
{
  "columns": ["STATUS", "CATEGORY"],
  "created": "2026-02-03T10:00:00Z",
  "csvSize": 1073741824,
  "csvMtime": 1738585200,
  "csvHash": "abc123def456..."
}
```

### Update Sidecar (`_updates.json`)

```json
{
  "12345": {"STATUS": "INACTIVE"},
  "67890": {"SCORE": "95"}
}
```
