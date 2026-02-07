<?php

/**
 * Command - Mimics Yii2 Command for SQL debugging.
 *
 * @package Entreya\CsvQuery\Query
 */

declare(strict_types=1);

namespace Entreya\CsvQuery\Query;

class Command
{
    private array $params;
    private string $tableName;
    private ?array $select;
    private array|string|null $where;
    private array $orderBy;
    private array $groupBy;
    private ?int $limit;
    private int $offset;

    public function __construct(array $config)
    {
        foreach ($config as $key => $value) {
            if (property_exists($this, $key)) {
                $this->$key = $value;
            }
        }
    }

    /**
     * Returns the mimic of SQL query for debugging.
     */
    public function getQuery(): string
    {
        $select = empty($this->select) ? '*' : implode(', ', $this->select);
        $sql = "SELECT {$select} FROM `{$this->tableName}`";

        if (!empty($this->where)) {
            $sql .= " WHERE " . $this->buildWhere($this->where);
        }

        if (!empty($this->groupBy)) {
            $sql .= " GROUP BY " . implode(', ', $this->groupBy);
        }

        if (!empty($this->orderBy)) {
            $orderParts = [];
            foreach ($this->orderBy as $col => $dir) {
                $orderParts[] = "{$col} " . ($dir === SORT_DESC ? 'DESC' : 'ASC');
            }
            $sql .= " ORDER BY " . implode(', ', $orderParts);
        }

        if ($this->limit !== null) {
            $sql .= " LIMIT " . $this->limit;
        }

        if ($this->offset > 0) {
            $sql .= " OFFSET " . $this->offset;
        }

        return $sql;
    }

    private function buildWhere($condition): string
    {
        if (is_string($condition)) {
            return $condition;
        }

        if (is_array($condition)) {
            if (!isset($condition[0])) { // Hash format
                $parts = [];
                foreach ($condition as $col => $val) {
                    $parts[] = "`{$col}` = " . $this->quoteValue($val);
                }
                return implode(' AND ', $parts);
            }

            // Operator format
            $operator = strtoupper((string)array_shift($condition));
            switch ($operator) {
                case 'AND':
                case 'OR':
                    $parts = [];
                    foreach ($condition as $operand) {
                        $parts[] = '(' . $this->buildWhere($operand) . ')';
                    }
                    return implode(" {$operator} ", $parts);
                case '=':
                case '>':
                case '<':
                case '>=':
                case '<=':
                case '!=':
                case 'LIKE':
                case 'IS':
                case 'IS NOT':
                    return "`{$condition[0]}` {$operator} " . $this->quoteValue($condition[1]);
                case 'IN':
                case 'NOT IN':
                    $values = is_array($condition[1]) ? $condition[1] : [$condition[1]];
                    $quotedValues = array_map([$this, 'quoteValue'], $values);
                    return "`{$condition[0]}` {$operator} (" . implode(', ', $quotedValues) . ")";
                case 'BETWEEN':
                case 'NOT BETWEEN':
                    return "`{$condition[0]}` {$operator} " . $this->quoteValue($condition[1]) . " AND " . $this->quoteValue($condition[2]);
            }
        }

        return '';
    }

    private function quoteValue($value): string
    {
        if (is_numeric($value)) {
            return (string)$value;
        }
        if (is_null($value)) {
            return 'NULL';
        }
        return "'" . str_replace("'", "''", (string)$value) . "'";
    }
}
