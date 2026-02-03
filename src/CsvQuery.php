<?php
/**
 * CsvQuery - High-Performance CSV Query Engine
 *
 * A MySQL-inspired library for querying large CSV files (100+ crore rows)
 * with sub-second response times using disk-based indexes.
 *
 * Features:
 * - Yii2-like ActiveQuery fluent API
 * - MySQL-like speed via binary search
 * - Go-powered parallel indexing
 * - External merge sort for unlimited scale
 *
 * Usage:
 * ```php
 * $csv = new CsvQuery('data.csv');
 * $csv->createIndex(['EXAM_SESSION_ID', 'PROGRAMME_ID']);
 *
 * // Simple query
 * $results = $csv->where('EXAM_SESSION_ID', 'SESSION_001')->all();
 *
 * // Composite key query
 * $results = $csv->where(['EXAM_SESSION_ID', 'PROGRAMME_ID'], ['S001', 'P001'])->all();
 *
 * // Count only
 * $count = $csv->where('EXAM_SESSION_ID', 'SESSION_001')->count();
 *
 * // Iterate with generator
 * foreach ($csv->where('PROGRAMME_ID', 'P001')->each() as $row) {
 *     echo $row['ENROLMENT_NUMBER'];
 * }
 * ```
 *
 * @package CsvQuery
 * @author CsvQuery Team
 * @version 1.2.0
 */

declare(strict_types=1);

namespace CsvQuery;

/**
 * Main CsvQuery class - entry point for all operations.
 */
class CsvQuery
{
    /** @var string Path to CSV file */
    private string $csvPath;

    /** @var string Index directory */
    private string $indexDir;

    /** @var array CSV headers */
    private ?array $headers = null;

    /** @var array Column name to index mapping */
    private array $headerMap = [];
    
    /** @var array Map of Virtual Columns [Name => Default] */
    private array $virtualColumns = [];

    /** @var string CSV separator */
    private string $separator;

    /** @var GoBridge Go binary wrapper */
    private GoBridge $goBridge;

    /** @var resource|null File handle for CSV */
    private $fileHandle = null;

    /** @var array Map of [Offset => [Column => Value]] */
    private array $overrides = [];

    /** @var bool Whether updates have been loaded */
    private bool $updatesLoaded = false;

    /**
     * Create a CsvQuery instance.
     *
     * @param string $csvPath Path to CSV file
     * @param array $options Configuration options:
     *   - 'indexDir': Directory for index files (default: same as CSV)
     *   - 'separator': CSV separator (default: ',')
     *   - 'workers': Number of parallel workers (default: CPU count)
     *   - 'memoryMB': Memory limit per worker (default: 500)
     */
    public function __construct(string $csvPath, array $options = [])
    {
        if (!file_exists($csvPath)) {
            throw new \InvalidArgumentException("CSV file not found: $csvPath");
        }

        $this->csvPath = realpath($csvPath);
        $this->indexDir = $options['indexDir'] ?? dirname($this->csvPath);
        $this->separator = $options['separator'] ?? ',';

        $this->goBridge = new GoBridge([
            'workers' => $options['workers'] ?? 0,
            'memoryMB' => $options['memoryMB'] ?? 500,
            'binaryPath' => $options['binaryPath'] ?? null,
            'indexDir' => $this->indexDir,
        ]);

        // Read headers
        $this->readHeaders();
    }

    /**
     * Read CSV headers.
     */
    private function readHeaders(): void
    {
        $handle = fopen($this->csvPath, 'r');
        $line = fgets($handle);
        fclose($handle);

        $line = preg_replace('/^\xEF\xBB\xBF/', '', trim($line));
        $this->headers = str_getcsv($line, $this->separator);
        
        $schemaPath = dirname($this->csvPath) . '/' . basename($this->csvPath) . '_schema.json';
        if (file_exists($schemaPath)) {
            $schema = json_decode(file_get_contents($schemaPath), true);
            if (isset($schema['virtual_columns'])) {
                $virtualCols = array_keys($schema['virtual_columns']);
                sort($virtualCols); // Ensure deterministic order matches Go
                foreach ($virtualCols as $col) {
                    if (!in_array($col, $this->headers)) {
                        $this->headers[] = $col;
                        $this->virtualColumns[$col] = $schema['virtual_columns'][$col];
                    }
                }
            }
        }

        $this->headerMap = array_flip($this->headers);
    }

    /**
     * Get Virtual Columns defaults.
     * @return array [Name => DefaultValue]
     */
    public function getVirtualColumns(): array 
    {
        return $this->virtualColumns;
    }

    /**
     * Get CSV headers.
     *
     * @return array Column names
     */
    public function getHeaders(): array
    {
        return $this->headers;
    }

    /**
     * Create indexes for specified columns.
     *
     * Multiple indexes are created in a single CSV scan for efficiency.
     * Composite indexes use combined keys (e.g., ["COL1", "COL2"]).
     *
     * @param array $columns Column definitions. Examples:
     *   - ['COL1', 'COL2'] - Two single-column indexes
     *   - [['COL1', 'COL2']] - One composite index
     *   - ['COL1', ['COL2', 'COL3']] - Mixed
     * @param bool $verbose Enable verbose output
     * @param array $options Configuration overrides ('workers', 'memoryMB')
     * @return bool Success status
     */
    public function createIndex(array $columns, bool $verbose = false, array $options = []): bool
    {
        // Normalize columns to JSON format
        $normalized = [];
        foreach ($columns as $col) {
            if (is_string($col)) {
                $normalized[] = $col;
            } elseif (is_array($col)) {
                $colSorted = $col;
                sort($colSorted);
                $normalized[] = $colSorted;
            }
        }

        return $this->goBridge->createIndex(
            $this->csvPath,
            $this->indexDir,
            json_encode($normalized),
            $this->separator,
            $verbose,
            $options
        );
    }

    /**
     * Check if an index exists for a column.
     *
     * @param string|array $column Column name or composite columns
     * @return bool
     */
    public function hasIndex(string|array $column): bool
    {
        $indexPath = $this->getIndexPath($column);
        return file_exists($indexPath);
    }

    /**
     * Get index file path for a column.
     *
     * @param string|array $column Column name or composite columns
     * @return string Path to .didx file
     */
    public function getIndexPath(string|array $column): string
    {
        $csvFilename = pathinfo($this->csvPath, PATHINFO_FILENAME);
        if (is_array($column)) {
            sort($column);
            $name = implode('_', $column);
        } else {
            $name = $column;
        }
        return $this->indexDir . '/' . $csvFilename . '_' . $name . '.cidx';
    }

    /**
     * Start a new query.
     *
     * @return ActiveQuery
     */
    public function find(): ActiveQuery
    {
        return new ActiveQuery($this);
    }

    /**
     * Start a WHERE query (Yii2-like syntax).
     *
     * @param string|array $column Column name, associative array, or composite columns
     * @param mixed $value Value to match (optional)
     * @return ActiveQuery
     */
    public function where(string|array $column, mixed $value = null): ActiveQuery
    {
        return (new ActiveQuery($this))->where($column, $value);
    }

    /**
     * Add an AND condition to a new query.
     *
     * @param string|array $column Column name, associative array, or composite columns
     * @param mixed $value Value to match (optional)
     * @return ActiveQuery
     */
    public function andWhere(string|array $column, mixed $value = null): ActiveQuery
    {
        return $this->where($column, $value);
    }

    /**
     * Clear specific index or all indexes.
     *
     * @param string|array $column Column name(s)
     * @return bool True if successful
     */
    public function dropIndex(string|array $column): bool
    {
        $path = $this->getIndexPath($column);
        if (file_exists($path)) {
            return unlink($path);
        }
        return true; // Graceful return
    }

    /**
     * Clear all indexes in the index directory.
     *
     * @return int Number of deleted indexes
     */
    public function clearIndexes(): int
    {
        $count = 0;
        $csvFilename = pathinfo($this->csvPath, PATHINFO_FILENAME);
        $dir = rtrim($this->indexDir, '/');

        // Patterns to clear
        $patterns = [
            "$dir/{$csvFilename}_*.cidx",
            "$dir/{$csvFilename}_*.bloom",
            "$dir/{$csvFilename}_meta.json",
        ];

        // Also try to help migrate by clearing old-style files
        // worst case: read the old metadata file to find exactly what indexes existed
        $oldMetaPath = "$dir/csvquery_meta.json";
        if (file_exists($oldMetaPath)) {
            $oldMeta = json_decode(file_get_contents($oldMetaPath), true);
            if (!empty($oldMeta['indexes'])) {
                foreach (array_keys($oldMeta['indexes']) as $idxName) {
                    $patterns[] = "$dir/$idxName.cidx";
                    $patterns[] = "$dir/$idxName.bloom";
                }
            }
            $patterns[] = $oldMetaPath;
        }

        // Fallback: If meta is missing, use heuristic to find legacy composite indexes
        if (!file_exists($oldMetaPath)) {
            $candidates = glob("$dir/*.{cidx,bloom}", GLOB_BRACE);
            if ($candidates) {
                $validCols = array_flip($this->headers ?? []); // Map check is faster
                
                foreach ($candidates as $path) {
                    $basename = pathinfo($path, PATHINFO_FILENAME);
                    
                    // Skip if it successfully matches the NEW prefix format (handled above/already)
                    if (str_starts_with($basename, $csvFilename . '_')) {
                        continue;
                    }

                    // Check if the filename is a combination of OUR columns
                    // e.g. "COL1_COL2" -> parts ["COL1", "COL2"]
                    $parts = explode('_', $basename);
                    $isMatch = true;
                    foreach ($parts as $part) {
                        if (!isset($this->headerMap[$part]) && !isset($validCols[$part])) {
                            $isMatch = false; // Unknown column, likely another CSV's index
                            break;
                        }
                    }

                    if ($isMatch) {
                        $patterns[] = $path;
                    }
                }
            }
        }

        $processed = [];
        foreach ($patterns as $pattern) {
            $files = glob($pattern);
            if ($files) {
                foreach ($files as $file) {
                    if (isset($processed[$file])) continue;
                    if (unlink($file)) {
                        $count++;
                    }
                    $processed[$file] = true;
                }
            }
        }
        return $count;
    }



    /**
     * Get the CSV file path.
     *
     * @return string
     */
    public function getCsvPath(): string
    {
        return $this->csvPath;
    }

    /**
     * Get the separator.
     *
     * @return string
     */
    public function getSeparator(): string
    {
        return $this->separator;
    }

    /**
     * Get header map.
     *
     * @return array
     */
    public function getHeaderMap(): array
    {
        return $this->headerMap;
    }

    /**
     * Get the index directory path.
     *
     * @return string
     */
    public function getIndexDir(): string
    {
        return $this->indexDir;
    }

    /**
     * Get the Go bridge instance.
     *
     * @return GoBridge
     */
    public function getGoBridge(): GoBridge
    {
        return $this->goBridge;
    }

    /**
     * Read a row at a specific offset.
     *
     * @param int $offset Byte offset
     * @return array|null Row data
     */
    public function readRowAt(int $offset): ?array
    {
        if ($this->fileHandle === null) {
            $this->fileHandle = fopen($this->csvPath, 'r');
        }

        fseek($this->fileHandle, $offset);
        $line = fgets($this->fileHandle);

        if ($line === false) {
            return null;
        }

        $values = str_getcsv(trim($line), $this->separator);
        $row = array_combine($this->headers, $values) ?: null;

        if ($row) {
            return $this->applyOverrides($offset, $row);
        }

        return $row;
    }

    /**
     * Load sidecar updates into memory.
     */
    private function loadUpdates(): void
    {
        if ($this->updatesLoaded) {
            return;
        }

        $updatesPath = $this->csvPath . '_updates.json';
        if (file_exists($updatesPath)) {
            $json = file_get_contents($updatesPath);
            $data = json_decode($json, true);
            if (is_array($data) && isset($data['rows'])) {
                $this->overrides = $data['rows'];
            }
        }

        $this->updatesLoaded = true;
    }

    /**
     * Apply overrides to a row based on its offset.
     * 
     * @param int $offset Byte offset of the row
     * @param array $row Row data
     * @return array Modified row data
     */
    public function applyOverrides(int $offset, array $row): array
    {
        $this->loadUpdates();

        $offsetKey = (string)$offset;
        if (isset($this->overrides[$offsetKey])) {
            foreach ($this->overrides[$offsetKey] as $col => $val) {
                $row[$col] = $val;
            }
        }

        return $row;
    }



    /**
     * Get index metadata (selectivity, file sizes, etc.)
     *
     * @return array
     */
    /**
     * Insert a single row.
     *
     * @param array $row Associative array of data
     * @return void
     */
    public function insert(array $row): void
    {
        $this->batchInsert([$row]);
    }

    /**
     * Insert multiple rows.
     *
     * @param array $rows Array of associative arrays
     * @return void
     */
    public function batchInsert(array $rows): void
    {
        if (empty($rows)) {
            return;
        }

        $headers = $this->getHeaders();
        
        // Handle new file case
        if (empty($headers)) {
            $headers = array_keys(reset($rows));
            $this->headers = $headers;
        }

        // Map associative keys to ordered rows
        $orderedRows = [];
        foreach ($rows as $row) {
            $orderedRow = [];
            foreach ($headers as $col) {
                // Use empty string for missing columns
                $orderedRow[] = (string)($row[$col] ?? '');
            }
            $orderedRows[] = $orderedRow;
        }

        $this->goBridge->write($this->csvPath, $orderedRows, $headers, $this->separator);
    }

    /**
     * Add a new column to the CSV.
     * WARNING: This rewrites the entire file.
     *
     * @param string $name Column name
     * @param string $default Default value
     * @return void
     */
    public function addColumn(string $name, string $default = '', bool $materialize = false): void
    {
        // 1. Alter Schema via Go (Handles rewrite and index cleanup)
        $this->goBridge->alter($this->csvPath, $name, $default, $this->separator, $materialize);
        
        // 2. Refresh Headers
        $this->readHeaders();
    }

    /**
     * Get metadata if available.
     *
     * @return array
     */
    public function getMeta(): array
    {
        $csvFilename = pathinfo($this->csvPath, PATHINFO_FILENAME);
        $metaPath = "{$this->indexDir}/{$csvFilename}_meta.json";
        if (file_exists($metaPath)) {
            return json_decode(file_get_contents($metaPath), true) ?? [];
        }
        return [];
    }

    /**
     * Validate the integrity of existing indexes.
     * Checks if CSV size, mtime or sample hash has changed.
     *
     * @return bool True if index is valid, False if stale
     */
    public function validateIntegrity(): bool
    {
        $meta = $this->getMeta();
        if (empty($meta)) {
            return false;
        }

        // 1. Basic checks (Fast)
        $currentSize = filesize($this->csvPath);
        $currentMtime = filemtime($this->csvPath);

        if ($currentSize !== ($meta['csvSize'] ?? 0)) {
            return false;
        }

        // If mtime is identical, we can trust it (mostly)
        if ($currentMtime === ($meta['csvMtime'] ?? 0)) {
            return true;
        }

        // 2. Cryptographic DNA check (Deep)
        $currentHash = $this->calculateFingerprint();
        return $currentHash === ($meta['csvHash'] ?? '');
    }

    /**
     * Calculate a multi-point sample hash of the CSV file.
     * Matches the logic in the Go indexer.
     *
     * @return string SHA-1 hash
     */
    private function calculateFingerprint(): string
    {
        $size = filesize($this->csvPath);
        $sampleSize = 512 * 1024; // 512KB

        $handle = fopen($this->csvPath, 'rb');
        $ctx = hash_init('sha1');

        // 1. Start
        hash_update($ctx, fread($handle, $sampleSize));

        // 2. Middle
        if ($size > $sampleSize * 3) {
            fseek($handle, (int)(($size / 2) - ($sampleSize / 2)));
            hash_update($ctx, fread($handle, $sampleSize));
        }

        // 3. End
        if ($size > $sampleSize) {
            fseek($handle, max(0, $size - $sampleSize));
            hash_update($ctx, fread($handle, $sampleSize));
        }

        fclose($handle);
        return hash_final($ctx);
    }


    /**
     * Update rows.
     * 
     * @param array $attributes Key-value pairs to update (e.g. ['STATUS' => 'INACTIVE'])
     * @param array $conditions Conditions to identify rows (e.g. ['ID' => '123'])
     * @return int Number of rows updated
     */
    public function update(array $attributes, array $conditions = []): int
    {
        if (empty($attributes)) {
             return 0;
        }

        // Build SET clause
        $setParts = [];
        foreach ($attributes as $k => $v) {
            $setParts[] = "$k=$v";
        }
        $setClause = implode(',', $setParts);
        
        // Build WHERE clause
        $whereJson = null;
        if (!empty($conditions)) {
            $whereJson = json_encode($conditions);
        }

        $count = $this->goBridge->update($this->csvPath, $setClause, $whereJson, $this->indexDir);
        if ($count > 0) {
            $this->updatesLoaded = false;
        }

        return $count;
    }

    /**
     * Clean up resources.
     */
    public function __destruct()
    {
        if ($this->fileHandle !== null) {
            fclose($this->fileHandle);
        }
    }
}
