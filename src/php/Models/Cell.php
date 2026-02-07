<?php

namespace Entreya\CsvQuery\Models;

/**
 * Cell - Represents a single cell in a CsvQuery result set.
 */
class Cell
{
    private $row;
    private $columnIndex;
    private $value;
    private $columnName;

    public function __construct($row, $columnIndex, $value, $columnName = null)
    {
        $this->row = $row;
        $this->columnIndex = $columnIndex;
        $this->value = $value;
        $this->columnName = $columnName;
    }

    public function getValue() { return $this->value; }
    public function getColumnIndex() { return $this->columnIndex; }
    public function getColumnName() { return $this->columnName; }
    public function getRow() { return $this->row; }

    public function isEmpty() { return $this->value === null || $this->value === ''; }
    public function isNumeric() { return is_numeric($this->value); }
    public function asString() { return (string)$this->value; }
    public function asInt($default = 0) { return is_numeric($this->value) ? (int)$this->value : $default; }
    public function asFloat($default = 0.0) { return is_numeric($this->value) ? (float)$this->value : $default; }
    public function asBool() {
        $v = strtolower(trim((string)$this->value));
        return in_array($v, ['1', 'true', 'yes', 'on', 'y'], true);
    }

    public function validate(array $rules)
    {
        $errors = [];
        foreach ($rules as $rule) {
            $parts = explode(':', $rule, 2);
            $ruleName = $parts[0];
            $param = $parts[1] ?? null;

            switch ($ruleName) {
                case 'required': if ($this->isEmpty()) $errors[] = "Field is required"; break;
                case 'numeric': if (!$this->isEmpty() && !$this->isNumeric()) $errors[] = "Value must be numeric"; break;
                case 'email': if (!$this->isEmpty() && !filter_var($this->value, FILTER_VALIDATE_EMAIL)) $errors[] = "Invalid email"; break;
                case 'min': if ($param !== null && strlen((string)$this->value) < (int)$param) $errors[] = "Too short"; break;
                case 'max': if ($param !== null && strlen((string)$this->value) > (int)$param) $errors[] = "Too long"; break;
            }
        }
        return ['valid' => empty($errors), 'errors' => $errors];
    }

    public function __toString() { return $this->asString(); }
}
