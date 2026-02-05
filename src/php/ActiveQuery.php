<?php

/**
 * ActiveQuery - Intelligent Query Engine for CSV
 *
 * This class mimics yii\db\ActiveQuery and yii\db\Query.
 * It provides a fluent interface for building and executing queries on CSV files.
 */

declare(strict_types=1);

namespace CsvQuery;

use CsvQuery\Models\Row;

class ActiveQuery
{
    /** @var CsvQuery Parent instance */
    private CsvQuery $csvQuery;

    /** @var array|null Columns to select */
    private ?array $select = null;

    /** @var array|string|null Conditions */
    private array|string|null $where = null;

    /** @var array Group by columns */
    private array $groupBy = [];

    /** @var array Order by columns */
    private array $orderBy = [];

    /** @var int|null Maximum results */
    private ?int $limit = null;

    /** @var int Offset for pagination */
    private int $offset = 0;

    /** @var string|callable|null Column to index results by */
    private $indexBy = null;

    /** @var bool Whether to return results as arrays */
    private bool $asArray = false;

    /**
     * Create a new ActiveQuery.
     */
    public function __construct(CsvQuery $csvQuery)
    {
        $this->csvQuery = $csvQuery;
    }

    /**
     * Sets the SELECT part of the query.
     */
    public function select(array $columns): self
    {
        $this->select = $columns;
        return $this;
    }

    /**
     * Sets the WHERE part of the query.
     */
    public function where(array|string|null $condition, mixed $value = null): self
    {
        if (is_array($condition) && is_array($value)) {
            $this->where = [$condition, $value];
        } elseif (is_string($condition) && $value !== null) {
            $this->where = ['=', $condition, $value];
        } else {
            $this->where = $condition;
        }
        return $this;
    }

    /**
     * Adds an additional WHERE condition to the existing one.
     */
    public function andWhere(array|string $condition, mixed $value = null): self
    {
        if (is_array($condition) && is_array($value)) {
            $condition = [$condition, $value];
        } elseif (is_string($condition) && $value !== null) {
            $condition = ['=', $condition, $value];
        }
        if ($this->where === null) {
            $this->where = $condition;
        } else {
            $this->where = ['and', $this->where, $condition];
        }
        return $this;
    }

    /**
     * Adds an additional WHERE condition to the existing one using OR.
     */
    public function orWhere(array|string $condition, mixed $value = null): self
    {
        if (is_array($condition) && is_array($value)) {
            $condition = [$condition, $value];
        } elseif (is_string($condition) && $value !== null) {
            $condition = ['=', $condition, $value];
        }
        if ($this->where === null) {
            $this->where = $condition;
        } else {
            $this->where = ['or', $this->where, $condition];
        }
        return $this;
    }

    /**
     * Sets the WHERE part of the query but ignores empty operands.
     */
    public function filterWhere(array $condition): self
    {
        $condition = $this->filterCondition($condition);
        if ($condition !== []) {
            $this->where($condition);
        }
        return $this;
    }

    /**
     * Adds an additional WHERE condition but ignores empty operands.
     */
    public function andFilterWhere(array $condition): self
    {
        $condition = $this->filterCondition($condition);
        if ($condition !== []) {
            $this->andWhere($condition);
        }
        return $this;
    }

    /**
     * Adds an additional WHERE condition but ignores empty operands using OR.
     */
    public function orFilterWhere(array $condition): self
    {
        $condition = $this->filterCondition($condition);
        if ($condition !== []) {
            $this->orWhere($condition);
        }
        return $this;
    }

    /**
     * Sets the ORDER BY part of the query.
     */
    public function orderBy(array|string $columns): self
    {
        $this->orderBy = $this->normalizeOrderBy($columns);
        return $this;
    }

    /**
     * Adds additional ORDER BY columns.
     */
    public function addOrderBy(array|string $columns): self
    {
        $columns = $this->normalizeOrderBy($columns);
        foreach ($columns as $name => $direction) {
            $this->orderBy[$name] = $direction;
        }
        return $this;
    }

    /**
     * Set debug mode on the underlying bridge.
     */
    public function debug(bool $enable = true): self
    {
        $this->csvQuery->getGoBridge()->debug = $enable;
        return $this;
    }

    /**
     * Sets the GROUP BY part of the query.
     */
    public function groupBy(array|string $columns): self
    {
        $this->groupBy = (array)$columns;
        return $this;
    }

    /**
     * Adds additional GROUP BY columns.
     */
    public function addGroupBy(array|string $columns): self
    {
        $columns = (array)$columns;
        foreach ($columns as $column) {
            $this->groupBy[] = $column;
        }
        return $this;
    }

    /**
     * Sets the indexBy property.
     */
    public function indexBy(string|callable $column): self
    {
        $this->indexBy = $column;
        return $this;
    }

    /**
     * Sets the asArray property.
     */
    public function asArray(bool $value = true): self
    {
        $this->asArray = $value;
        return $this;
    }

    /**
     * Sets the LIMIT part of the query.
     */
    public function limit(int $limit): self
    {
        $this->limit = $limit;
        return $this;
    }

    /**
     * Sets the OFFSET part of the query.
     */
    public function offset(int $offset): self
    {
        $this->offset = $offset;
        return $this;
    }

    /**
     * Creates a command instance.
     *
     * @return Command
     */
    public function createCommand($sql = null): Command
    {
        $this->validateColumns();
        
        return new Command([
            'tableName' => basename($this->csvQuery->getCsvPath()),
            'select' => $this->select,
            'where' => $this->where,
            'orderBy' => $this->orderBy,
            'groupBy' => $this->groupBy,
            'limit' => $this->limit,
            'offset' => $this->offset,
        ]);
    }

    /**
     * Executes query and returns all results.
     */
    public function all(): array
    {
        $results = [];
        foreach ($this->each() as $row) {
            if ($this->indexBy !== null) {
                $val = ($row instanceof Row) ? $row->toAssociativeArray() : $row;
                $key = is_callable($this->indexBy) ? ($this->indexBy)($val) : $val[$this->indexBy];
                $results[$key] = $row;
            } else {
                $results[] = $row;
            }
        }
        return $results;
    }

    /**
     * Executes query and returns a single row.
     */
    public function one(): array|Row|null
    {
        $oldLimit = $this->limit;
        $this->limit = 1;
        $row = null;
        foreach ($this->each() as $r) {
            $row = $r;
            break;
        }
        $this->limit = $oldLimit;
        return $row;
    }

    public function average($column)
    {
        return $this->aggregateValue($column, 'avg');
    }

    public function min($column)
    {
        return $this->aggregateValue($column, 'min');
    }

    public function max($column)
    {
        return $this->aggregateValue($column, 'max');
    }

    public function sum($column)
    {
        return $this->aggregateValue($column, 'sum');
    }

    private function aggregateValue($column, $func)
    {
        $this->groupBy = []; 
        $results = $this->aggregate($column, [$column => $func]);
        return reset($results) ?: 0;
    }

    public function aggregate(string $columnOrGroup, array $aggregations): array
    {
        $this->validateColumns();
        $func = reset($aggregations);
        $aggCol = key($aggregations);
        if ($func === 'count') $aggCol = $columnOrGroup;

        $goWhere = $this->formatWhereForGo($this->where);
        return $this->csvQuery->getGoBridge()->query(
            csvPath: $this->csvQuery->getCsvPath(),
            indexDir: $this->csvQuery->getIndexDir(),
            where: $goWhere,
            limit: 0,
            offset: 0,
            groupBy: !empty($this->groupBy) ? implode(',', $this->groupBy) : $columnOrGroup,
            aggCol: $aggCol,
            aggFunc: (string)$func
        );
    }

    /**
     * Returns the number of records.
     */
    public function count(string $q = '*', $db = null): int
    {
        // If query is simple enough, use Go count
        $plan = $this->buildExecutionPlan();

        // Use Go engine if:
        // 1. It's an IndexScan (with valid index)
        // 2. OR it's a full table count (no where, no group, no order) - Go has optimized path
        $canUseGo = ($plan['type'] === 'IndexScan' && empty($this->groupBy) && empty($this->orderBy)) ||
                    (empty($this->where) && empty($this->groupBy) && empty($this->orderBy));

        if ($canUseGo) {
            return $this->csvQuery->getGoBridge()->count(
                $this->csvQuery->getCsvPath(),
                $this->csvQuery->getIndexDir(),
                $this->formatWhereForGo($this->where)
            );
        }

        // Otherwise full scan count
        $count = 0;
        foreach ($this->each() as $row) {
            $count++;
        }
        return $count;
    }

    /**
     * Checks if results exist.
     */
    public function exists(): bool
    {
        return $this->one() !== null;
    }

    /**
     * Returns the value of the first column in the first row.
     */
    public function scalar(): mixed
    {
        $row = $this->one();
        if ($row instanceof Row) {
            $data = $row->toAssociativeArray();
            return reset($data);
        }
        return $row ? reset($row) : null;
    }

    /**
     * Returns values of the first column.
     */
    public function column(): array
    {
        $results = [];
        foreach ($this->each() as $row) {
            if ($row instanceof Row) {
                $data = $row->toAssociativeArray();
                $results[] = reset($data);
            } else {
                $results[] = reset($row);
            }
        }
        return $results;
    }

    /**
     * Iterates over results in batches.
     */
    public function batch(int $batchSize = 100): \Generator
    {
        $batch = [];
        foreach ($this->each() as $row) {
            $batch[] = $row;
            if (count($batch) >= $batchSize) {
                yield $batch;
                $batch = [];
            }
        }
        if ($batch !== []) {
            yield $batch;
        }
    }

    /**
     * Iterates over results one by one.
     */
    public function each(int $batchSize = 100): \Generator
    {
        $plan = $this->buildExecutionPlan();
        
        if ($plan['type'] === 'IndexScan') {
            yield from $this->runIndexScan();
        } else {
            yield from $this->runFullScan();
        }
    }

    /**
     * Explains the query execution plan.
     */
    public function explain(string $format = 'array'): mixed
    {
        $plan = $this->buildExecutionPlan();
        
        return match (strtolower($format)) {
            'json' => json_encode($plan, JSON_PRETTY_PRINT),
            'table' => $this->formatAsTable($plan),
            'tree' => $this->formatAsTree($plan),
            default => $plan,
        };
    }

    /**
     * Get execution statistics from the last query.
     * 
     * Returns timing metrics captured from the Go engine (via stderr).
     * Supported formats: 'array', 'json', 'table'.
     *
     * @param string $format Output format ('array', 'json', 'table')
     * @return array|string
     */
    public function getStats(string $format = 'array'): array|string
    {
        $stderr = $this->csvQuery->getGoBridge()->getLastStderr();
        $stats = [];
        
        if (preg_match('/Time-Execution: (.*)/', $stderr, $m)) {
            $stats['execution'] = trim($m[1]);
        }
        if (preg_match('/Time-Fetching: (.*)/', $stderr, $m)) {
            $stats['fetching'] = trim($m[1]);
        }
        if (preg_match('/Time-Total: (.*)/', $stderr, $m)) {
            $stats['total'] = trim($m[1]);
        }
        
        return match (strtolower($format)) {
            'json' => json_encode($stats, JSON_PRETTY_PRINT),
            'table' => $this->formatStatsAsTable($stats),
            default => $stats,
        };
    }

    private function formatStatsAsTable(array $stats): string
    {
        $lines = [];
        $lines[] = "+------------------+-----------------------------+";
        $lines[] = "| Metric           | Value                       |";
        $lines[] = "+------------------+-----------------------------+";
        foreach ($stats as $key => $val) {
            $lines[] = sprintf("| %-16s | %-27s |", ucfirst($key), $val);
        }
        $lines[] = "+------------------+-----------------------------+";
        return implode("\n", $lines);
    }

    // --- Private Helpers ---

    private function validateColumns(): void
    {
        $map = $this->csvQuery->getHeaderMap();
        $usedColumns = $this->getUsedColumns();

        foreach ($usedColumns as $col) {
            if (!isset($map[$col])) {
                throw new \InvalidArgumentException("Column not found: $col");
            }
        }
    }

    private function getUsedColumns(): array
    {
        $columns = [];

        // Select
        if ($this->select) {
            foreach ($this->select as $col) {
                $columns[] = $col;
            }
        }

        // Group By
        foreach ($this->groupBy as $col) {
            $columns[] = $col;
        }

        // Order By
        foreach (array_keys($this->orderBy) as $col) {
            $columns[] = $col;
        }

        // Where
        $columns = array_merge($columns, $this->getColumnsFromCondition($this->where));

        return array_unique($columns);
    }

    private function getColumnsFromCondition($condition): array
    {
        if (empty($condition)) {
            return [];
        }

        if (is_array($condition)) {
            // Custom format: [['col1', 'col2'], ['val1', 'val2']]
            if (isset($condition[0]) && is_array($condition[0]) && isset($condition[1]) && is_array($condition[1])) {
                return array_merge($condition[0], $this->getColumnsFromCondition($condition[2] ?? []));
            }

            // Hash format: ['col' => 'val']
            if (!isset($condition[0])) {
                return array_keys($condition);
            }

            // Operator format: ['op', 'col', 'val'] or ['and', cond1, cond2]
            $op = strtolower((string)$condition[0]);
            
            // Logic operators
            if (in_array($op, ['and', 'or', 'not'])) {
                $cols = [];
                for ($i = 1; $i < count($condition); $i++) {
                    $cols = array_merge($cols, $this->getColumnsFromCondition($condition[$i]));
                }
                return $cols;
            }

            // Simple operators: ['=', 'col', 'val']
            if (isset($condition[1]) && is_string($condition[1])) {
                return [$condition[1]];
            }
        }

        return [];
    }

    private function buildExecutionPlan(): array
    {
        $this->validateColumns();

        // Check if index is stale or doesn't match the current CSV
        if (!$this->csvQuery->validateIntegrity()) {
            return [
                'type' => 'FullScan',
                'index' => null,
                'reason' => 'Indexes are stale or belong to a different CSV. Perform createIndex() to refresh.',
                'cost' => 'High'
            ];
        }

        $availableIndexes = $this->detectPotentialIndexes();
        
        if ($availableIndexes !== []) {
            $meta = $this->csvQuery->getMeta();
            $totalRows = $meta['totalRows'] ?? 0;
            
            $bestIndex = $availableIndexes[0];
            $bestScore = PHP_INT_MAX;

            foreach ($availableIndexes as $idx) {
                $stats = $meta['indexes'][$idx] ?? null;
                $distinctCount = $stats['distinctCount'] ?? 1;
                
                // Base score: Avg rows per key. Lower is better.
                // If we don't know totalRows, assume a large number (e.g. 1M) for comparison
                $baseRows = ($totalRows > 0) ? $totalRows : 1000000;
                $avgRowsPerKey = $baseRows / max(1, $distinctCount);
                
                // Multiplier for composite indexes
                // A composite index like A_B_C is much better than single A
                // Count columns in the index name
                $colCount = count(explode('_', $idx));
                $score = $avgRowsPerKey / pow(10, $colCount - 1);

                if ($score < $bestScore) {
                    $bestScore = $score;
                    $bestIndex = $idx;
                }
            }

            return [
                'type' => 'IndexScan',
                'index' => $bestIndex,
                'reason' => 'Index selected based on selectivity & composite coverage (Score: ' . round($bestScore, 2) . ').',
                'cost' => $bestScore < 100 ? 'Low' : ($bestScore < 5000 ? 'Medium' : 'High')
            ];
        }

        return [
            'type' => 'FullScan',
            'index' => null,
            'reason' => 'No suitable index found or query too complex for primary index scan.',
            'cost' => 'High'
        ];
    }

    private function detectPotentialIndexes(): array
    {
        $matches = [];
        $candidates = $this->extractEqualityColumns($this->where);
        
        // Add groupBy columns as candidates if no where clause or to augment
        if (!empty($this->groupBy)) {
            if (is_array($this->groupBy)) {
                $candidates[] = $this->groupBy; // Composite candidate
            } else {
                $candidates[] = $this->groupBy;
            }
        }

        foreach ($candidates as $col) {
            if (is_array($col)) {
                sort($col);
                $indexName = implode('_', $col);
            } else {
                $indexName = $col;
            }
            
            if ($this->csvQuery->hasIndex($col)) { // Pass raw $col, hasIndex handles sorting now
                $matches[] = $indexName;
            }
        }
        return $matches;
    }

    private function extractEqualityColumns($condition): array
    {
        if (empty($condition)) return [];

        if (is_array($condition)) {
            // Custom format: [['col1', 'col2'], ['val1', 'val2']]
            if (isset($condition[0]) && is_array($condition[0]) && isset($condition[1]) && is_array($condition[1])) {
                return [$condition[0]]; // return the array of columns as a single candidate
            }

            if (!isset($condition[0])) { // Hash format
                $keys = array_keys($condition);
                if (count($keys) > 1) {
                    return array_merge([$keys], $keys);
                }
                return $keys;
            }
            // Operator format - only handle basic things for index selection
            $op = strtolower((string)$condition[0]);
            if ($op === 'and') {
                $res = [];
                for ($i = 1; $i < count($condition); $i++) {
                    $res = array_merge($res, $this->extractEqualityColumns($condition[$i]));
                }
                return array_unique($res, SORT_REGULAR);
            }
            if ($op === '=') {
                return (array)$condition[1];
            }
        }
        return [];
    }

    private function runIndexScan(): \Generator
    {
        $goWhere = $this->formatWhereForGo($this->where);
        $isGrouping = !empty($this->groupBy);
        
        $results = $this->csvQuery->getGoBridge()->query(
            csvPath: $this->csvQuery->getCsvPath(),
            indexDir: $this->csvQuery->getIndexDir(),
            where: $goWhere,
            limit: $this->limit ?? 0,
            offset: $this->offset,
            groupBy: $isGrouping ? implode(',', $this->groupBy) : null
        );

        // If we are grouping, the results are the data (aggregated by Go)
        if ($isGrouping) {
            foreach ($results as $key => $row) {
                // For simple GroupBy (distinct), we want the key as the value
                // Go engine returns { "Key": Count } default
                
                // Handle JSON key quotes if they exist (until Scanner is fully updated or for legacy indexes)
                // If key is like "\"val\"" (escaped quote), strip them
                /*
                if (strlen($key) >= 2 && $key[0] === '"' && $key[strlen($key)-1] === '"') {
                     $key = substr($key, 1, -1);
                }
                */
                // Actually, json_decode handles the outer quotes. But if the CONTENT has quotes...
                // e.g. CSV value is `"foo"`. Index key is `"foo"`. JSON key is `"\"foo\""`. PHP key is `"foo"`.
                // We likely want `foo` (unquoted).
                
                $val = (string)$key;
                if (strlen($val) >= 2 && $val[0] === '"' && $val[strlen($val)-1] === '"') {
                    $val = substr($val, 1, -1);
                }

                // If multiple group columns, we'd need to split. But Go engine currently returns single key.
                // Assumption: User grouping by single column or we using first column name.
                $colName = is_array($this->groupBy) ? reset($this->groupBy) : $this->groupBy;
                
                $data = [$colName => $val];
                
                yield $this->asArray ? $data : new Row($this->csvQuery, $data);
            }
            return;
        }

        // Otherwise, results are offsets (Generator or Array) that must be hydrated
        $count = 0;
        foreach ($results as $result) {
            // Expected format: ['offset' => 123, 'line' => 1]
            // Or if simple int handling was somehow active (robustness)
            $offset = is_array($result) ? ($result['offset'] ?? null) : $result;
            
            if ($offset === null) continue;

            $row = $this->csvQuery->readRowAt((int)$offset);
            if ($row) {
                // Post-filter in case Go didn't handle all conditions
                if ($this->applyFilter($row)) {
                    if ($this->select) {
                        $row = array_intersect_key($row, array_flip($this->select));
                    }
                    // Use line number if available
                    $line = is_array($result) ? ($result['line'] ?? null) : null;
                    yield $this->asArray ? $row : new Row($this->csvQuery, $row, $line);
                    
                    $count++;
                    if ($this->limit !== null && $count >= $this->limit) break;
                }
            }
        }
    }

    private function runFullScan(): \Generator
    {
        $handle = fopen($this->csvQuery->getCsvPath(), 'r');
        $headers = $this->csvQuery->getHeaders();
        $separator = $this->csvQuery->getSeparator();
        
        fclose($handle);
    $handle = fopen($this->csvQuery->getCsvPath(), 'r');
    
    // Skip header and track offset
    fgets($handle); 
    
    $count = 0;
    $skipped = 0;
    $currentLineNumber = 0;
    $groups = [];
    
    while (true) {
        $rowOffset = ftell($handle);
        $line = fgets($handle);
        if ($line === false) break;

        $currentLineNumber++;
        $data = str_getcsv(trim($line), $separator);
        
        // Inject Virtual Columns if data is shorter than headers
        if (count($data) < count($headers)) {
            $virtuals = $this->csvQuery->getVirtualColumns();
            // Append virtuals that are in headers but not in data
            // Assumption: Virtuals are always at the end (due to sort in readHeaders)
            // We just append defaults for the missing count
            foreach ($headers as $idx => $header) {
                if ($idx >= count($data)) {
                    $data[] = $virtuals[$header] ?? '';
                }
            }
        }

        if (count($data) !== count($headers)) {
            // Still mismatch? Skip or log?
            continue; 
        }

        $row = array_combine($headers, $data);
        if ($row === false) continue;

        // Apply persistent updates
        $row = $this->csvQuery->applyOverrides($rowOffset, $row);

        if ($this->applyFilter($row)) {
                if ($this->groupBy) {
                    $groupKey = '';
                    foreach ($this->groupBy as $col) {
                        $groupKey .= ($row[$col] ?? '') . '|';
                    }
                    if (!isset($groups[$groupKey])) {
                        $groups[$groupKey] = $row; // Just keep the first one for grouping now
                    }
                    continue;
                }

                if ($this->offset > 0 && $skipped < $this->offset) {
                    $skipped++;
                    continue;
                }
                
                if ($this->select) {
                    $row = array_intersect_key($row, array_flip($this->select));
                }
                
                yield $this->asArray ? $row : new Row($this->csvQuery, $row, $currentLineNumber);
                $count++;
                
                if ($this->limit !== null && $count >= $this->limit) {
                    break;
                }
            }
        }

        if ($this->groupBy) {
            foreach ($groups as $row) {
                if ($this->offset > 0 && $skipped < $this->offset) {
                    $skipped++;
                    continue;
                }
                if ($this->select) {
                    $row = array_intersect_key($row, array_flip($this->select));
                }
                yield $this->asArray ? $row : new Row($this->csvQuery, $row);
                $count++;
                if ($this->limit !== null && $count >= $this->limit) break;
            }
        }
        fclose($handle);
    }

    private function applyFilter(array $row): bool
    {
        if ($this->where === null) return true;
        return $this->evaluateCondition($this->where, $row);
    }

    private function evaluateCondition($condition, array $row): bool
    {
        if (is_string($condition)) {
            return true; 
        }

        if (is_array($condition)) {
            // Custom format: [['col1', 'col2'], ['val1', 'val2']]
            if (isset($condition[0]) && is_array($condition[0]) && isset($condition[1]) && is_array($condition[1])) {
                foreach ($condition[0] as $i => $col) {
                    if (!isset($row[$col]) || $row[$col] != $condition[1][$i]) return false;
                }
                return true;
            }

            if (!isset($condition[0])) { // Hash format ['col' => 'val']
                foreach ($condition as $col => $val) {
                    if (!isset($row[$col]) || $row[$col] != $val) return false;
                }
                return true;
            }

            // Operator format ['op', 'col', 'val']
            $op = strtolower((string)$condition[0]);
            switch ($op) {
                case 'and':
                    for ($i = 1; $i < count($condition); $i++) {
                        if (!$this->evaluateCondition($condition[$i], $row)) return false;
                    }
                    return true;
                case 'or':
                    for ($i = 1; $i < count($condition); $i++) {
                        if ($this->evaluateCondition($condition[$i], $row)) return true;
                    }
                    return false;
                case '=':
                    return isset($row[$condition[1]]) && $row[$condition[1]] == $condition[2];
                case '>':
                    return isset($row[$condition[1]]) && $row[$condition[1]] > $condition[2];
                case '<':
                    return isset($row[$condition[1]]) && $row[$condition[1]] < $condition[2];
                case '>=':
                    return isset($row[$condition[1]]) && $row[$condition[1]] >= $condition[2];
                case '<=':
                    return isset($row[$condition[1]]) && $row[$condition[1]] <= $condition[2];
                case '!=':
                case '<>':
                    return isset($row[$condition[1]]) && $row[$condition[1]] != $condition[2];
                case 'is':
                    if ($condition[2] === null) {
                        return !isset($row[$condition[1]]) || $row[$condition[1]] === null || $row[$condition[1]] === '';
                    }
                    return isset($row[$condition[1]]) && $row[$condition[1]] == $condition[2];
                case 'is not':
                    if ($condition[2] === null) {
                        return isset($row[$condition[1]]) && $row[$condition[1]] !== null && $row[$condition[1]] !== '';
                    }
                    return !isset($row[$condition[1]]) || $row[$condition[1]] != $condition[2];
                case 'like':
                    if (!isset($row[$condition[1]])) return false;
                    $pattern = str_replace(['%', '_'], ['.*', '.'], preg_quote((string)$condition[2], '/'));
                    return (bool)preg_match('/^' . $pattern . '$/i', (string)$row[$condition[1]]);
            }
        }
        return true;
    }

    private function formatWhereForGo($condition): array
    {
        if (empty($condition)) {
            return [];
        }

        // Check if simple hash format ['col' => 'val'] (AND by default)
        // We can pass this directly to Go if it strictly only contains col=>val
        if ($this->isSimpleHash($condition)) {
            return $condition;
        }

        // Otherwise build strict tree
        return $this->buildFilterTree($condition);
    }

    private function isSimpleHash(array $condition): bool
    {
        if (isset($condition[0])) return false; // Indexed array implies operator format
        foreach ($condition as $k => $v) {
            if (is_int($k)) return false;
            // If value is array, it might be IN operator (not supported by simple hash in Go yet? Go supports simple Eq)
            if (is_array($v)) return false; 
        }
        return true;
    }

    private function buildFilterTree($condition): array
    {
        if (!is_array($condition)) {
            return [];
        }

        // Hash format: ['col' => 'val'] -> AND(Eq(col, val)...)
        if (!isset($condition[0])) {
            $children = [];
            foreach ($condition as $col => $val) {
                $children[] = [
                    'operator' => '=',
                    'column' => $col,
                    'value' => $val
                ];
            }
            if (count($children) === 1) {
                return $children[0];
            }
            return [
                'operator' => 'AND',
                'children' => $children
            ];
        }

        // Operator format: ['op', ...]
        $op = strtoupper((string)$condition[0]);

        // Hande Logical Operators
        if ($op === 'AND' || $op === 'OR') {
            $children = [];
            for ($i = 1; $i < count($condition); $i++) {
                $child = $this->buildFilterTree($condition[$i]);
                if (!empty($child)) {
                    $children[] = $child;
                }
            }
            return [
                'operator' => $op,
                'children' => $children
            ];
        }

        // Handle NOT (special case, maybe transform common patterns or defer)
        // Go engine doesn't have partial logical NOT support yet.
        // Support specific NOT like ['not', ['=', col, val]] -> ['!=', col, val]
        if ($op === 'NOT') {
            if (isset($condition[1])) {
                 $child = $condition[1];
                 // Check if child is simple operator
                 if (is_array($child) && isset($child[0])) {
                     $childOp = strtoupper((string)$child[0]);
                     if ($childOp === '=') return ['operator' => '!=', 'column' => $child[1], 'value' => $child[2]];
                     if ($childOp === 'IN') return ['operator' => 'NOT IN', 'column' => $child[1], 'value' => $child[2]]; // Not IN not supported in Go yet?
                     // For 'IS NULL' -> 'IS NOT NULL'
                     // But usually users write ['is not', col, null]
                 }
            }
            // Fallback: Ignored or Error? For now ignore or risk partial results
            return [];
        }

        // Comparison Operators
        $col = $condition[1] ?? '';
        $val = $condition[2] ?? null;

        return match ($op) {
            '=' => ['operator' => '=', 'column' => $col, 'value' => $val],
            '!=' => ['operator' => '!=', 'column' => $col, 'value' => $val],
            '<>' => ['operator' => '!=', 'column' => $col, 'value' => $val],
            '>' => ['operator' => '>', 'column' => $col, 'value' => $val],
            '<' => ['operator' => '<', 'column' => $col, 'value' => $val],
            '>=' => ['operator' => '>=', 'column' => $col, 'value' => $val],
            '<=' => ['operator' => '<=', 'column' => $col, 'value' => $val],
            'LIKE' => ['operator' => 'LIKE', 'column' => $col, 'value' => $val],
            'IS' => ($val === null) ? ['operator' => 'IS NULL', 'column' => $col] : ['operator' => '=', 'column' => $col, 'value' => $val],
            'IS NOT' => ($val === null) ? ['operator' => 'IS NOT NULL', 'column' => $col] : ['operator' => '!=', 'column' => $col, 'value' => $val],
            default => [] // Unknown operator
        };
    }


    private function filterCondition(array $condition): array
    {
        if (!is_array($condition)) return $condition;

        if (!isset($condition[0])) {
            // Hash format
            foreach ($condition as $name => $value) {
                if ($this->isEmpty($value)) {
                    unset($condition[$name]);
                }
            }
            return $condition;
        }

        // Operator format
        $operator = array_shift($condition);
        switch (strtolower((string)$operator)) {
            case 'and':
            case 'or':
                foreach ($condition as $i => $operand) {
                    $sub = $this->filterCondition($operand);
                    if ($this->isEmpty($sub)) {
                        unset($condition[$i]);
                    } else {
                        $condition[$i] = $sub;
                    }
                }
                if ($condition === []) return [];
                array_unshift($condition, $operator);
                return $condition;
            default:
                // operator, operand1, operand2, ...
                if (isset($condition[1]) && $this->isEmpty($condition[1])) {
                    return [];
                }
                array_unshift($condition, $operator);
                return $condition;
        }
    }

    private function isEmpty($value): bool
    {
        return $value === '' || $value === [] || $value === null || (is_string($value) && trim($value) === '');
    }

    private function normalizeOrderBy(array|string $columns): array
    {
        if (is_array($columns)) return $columns;
        
        $results = [];
        $columns = preg_split('/\s*,\s*/', trim($columns), -1, PREG_SPLIT_NO_EMPTY);
        foreach ($columns as $column) {
            if (preg_match('/^(.*?)\s+(asc|desc)$/i', $column, $matches)) {
                $results[$matches[1]] = strtolower($matches[2]) === 'desc' ? SORT_DESC : SORT_ASC;
            } else {
                $results[$column] = SORT_ASC;
            }
        }
        return $results;
    }

    private function formatAsTable(array $plan): string
    {
        $lines = [];
        $lines[] = "+------------------+-----------------------------+";
        $lines[] = "| Plan Property    | Value                       |";
        $lines[] = "+------------------+-----------------------------+";
        foreach ($plan as $key => $val) {
            $lines[] = sprintf("| %-16s | %-27s |", ucfirst($key), is_scalar($val) ? (string)$val : json_encode($val));
        }
        $lines[] = "+------------------+-----------------------------+";
        return implode("\n", $lines);
    }

    private function formatAsTree(array $plan): string
    {
        return "Query\n" . 
               "└── " . $plan['type'] . "\n" .
               "    ├── Reason: " . $plan['reason'] . "\n" .
               "    └── Index: " . ($plan['index'] ?? 'None');
    }
}
