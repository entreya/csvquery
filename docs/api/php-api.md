# CsvQuery PHP API Reference

## Class: `CsvQuery`

Namespace: `CsvQuery`

The main entry point for interacting with CSV files.

### Constructor
```php
public function __construct(string $csvPath, array $options = [])
```
- `$csvPath`: Path to the .csv file.
- `$options`: Array of configuration options:
  - `indexDir`: Directory to store indexes (default: CSV directory).
  - `separator`: CSV field separator (default: `,`).
  - `workers`: Number of parallel workers for indexing (default: CPU count).
  - `memoryMB`: Memory limit per worker in MB (default: 500).
  - `binaryPath`: Path to `csvquery` binary (default: auto-detected).

### Methods

#### `createIndex`
```php
public function createIndex(array $columns, bool $verbose = false, array $options = []): bool
```
Creates indexes for the specified columns.
- `$columns`: Array of column names (e.g. `['COL1', 'COL2']`) or composite indexes (e.g. `[['COL1', 'COL2']]`).

#### `find`
```php
public function find(): ActiveQuery
```
Starts a new query builder instance.

#### `where`
```php
public function where(string|array $column, mixed $value = null): ActiveQuery
```
Shortcut to start a query with a condition.

#### `insert`
```php
public function insert(array $row): void
```
Append a single row to the CSV.

#### `batchInsert`
```php
public function batchInsert(array $rows): void
```
Append multiple rows efficiently.

#### `update`
```php
public function update(array $attributes, array $conditions = []): int
```
Update rows matching conditions. Note: Implements "Sidecar Updates" - original CSV remains immutable.

#### `addColumn`
```php
public function addColumn(string $name, string $default = '', bool $materialize = false): void
```
Add a new column to the schema.

---

## Class: `ActiveQuery`

Namespace: `CsvQuery`

Fluent interface for building queries.

### Methods

#### `select`
```php
public function select(array $columns): self
```
Specify which columns to return.

#### `where`
```php
public function where(array|string|null $condition, mixed $value = null): self
```
Add WHERE conditions. Supports:
- Hash format: `['col' => 'val']`
- Operator format: `['>', 'col', 10]`
- Complex format: `['OR', ['=', 'a', 1], ['=', 'b', 2]]`

#### `orderBy`
```php
public function orderBy(array|string $columns): self
```
Sort results. Example: `['col' => SORT_DESC]`.

#### `groupBy`
```php
public function groupBy(array|string $columns): self
```
Group results by column(s).

#### `limit`, `offset`
```php
public function limit(int $limit): self
public function offset(int $offset): self
```
Pagination controls.

#### `all`
```php
public function all(): array
```
Execute query and return all results as an array of `Row` objects (or arrays if `asArray(true)`).

#### `one`
```php
public function one(): array|Row|null
```
Execute query and return the first result.

#### `each`
```php
public function each(int $batchSize = 100): \Generator
```
Iterate over results memory-efficiently.

#### `count`
```php
public function count(): int
```
Return the count of matching rows. Strongly optimized to use indexes (Zero-IO) whenever possible.

#### `exists`
```php
public function exists(): bool
```
Check if any row matches the condition.

#### `explain`
```php
public function explain(string $format = 'array'): mixed
```
Return the query execution plan (IndexScan vs FullScan).
