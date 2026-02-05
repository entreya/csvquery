# CsvQuery Go CLI Reference

The `csvquery` binary allows interaction with the indexing engine directly from the command line.

## Usage

```bash
csvquery <command> [arguments]
```

## Commands

### `index`
Create indexes for a CSV file.

**Flags:**
- `--input` (string): Path to input CSV file **(Required)**.
- `--output` (string): Output directory for indexes (Default: same as input directory).
- `--columns` (string): JSON array of column names to index (Default: `[]`).
- `--separator` (string): CSV separator character (Default: `,`).
- `--workers` (int): Number of parallel workers (Default: CPU count).
- `--memory` (int): Memory limit per worker in MB (Default: 500).
- `--bloom` (float): Bloom filter false positive rate (Default: 0.01).
- `--verbose` (bool): Enable verbose output.

**Example:**
```bash
csvquery index --input data.csv --columns '["STATUS", "CATEGORY"]' --verbose
```

### `query`
Query a CSV file using available indexes.

**Flags:**
- `--csv` (string): Path to CSV file.
- `--index-dir` (string): Directory containing indexes (Default: CSV directory).
- `--where` (string): JSON object of conditions (Default: `{}`).
- `--limit` (int): Maximum results to return (Default: 0 - no limit).
- `--offset` (int): Skip first N results (Default: 0).
- `--count` (bool): Only output the count of matching rows.
- `--explain` (bool): Output the query execution plan instead of results.
- `--group-by` (string): Column to group by.
- `--agg-col` (string): Column to aggregate.
- `--agg-func` (string): Aggregation function (`sum`, `avg`, `min`, `max`, `count`).
- `--debug-headers` (bool): Debug raw headers.

**Example:**
```bash
csvquery query --csv data.csv --where '{"STATUS":"active"}' --count
```

### `daemon`
Start the Unix Domain Socket server for high-performance communication with the PHP client.

**Flags:**
- `--socket` (string): Path to the Unix socket (Default: `/tmp/csvquery.sock`).
- `--csv` (string): Path to CSV file.
- `--index-dir` (string): Index directory.
- `--workers` (int): Max concurrency (Default: 50).

**Example:**
```bash
csvquery daemon --socket /tmp/csvquery.sock
```

### `write`
Append data to a CSV file.

**Flags:**
- `--csv` (string): Path to CSV file **(Required)**.
- `--headers` (string): JSON array of headers (only used if creating a new file).
- `--data` (string): JSON array of rows (each row is an array of strings).
- `--separator` (string): CSV separator (Default: `,`).

**Example:**
```bash
csvquery write --csv data.csv --data '[["Value1", "Value2"]]'
```

### `version`
Show version information.

**Example:**
```bash
csvquery version
```
