<?php

namespace CsvQuery\Models;

/**
 * Column - Represents a column-oriented view of a row's data.
 */
class Column
{
    private $row;
    private $index;
    private $value;
    private $name;

    public function __construct($row, $index, $value, $name = null)
    {
        $this->row = $row;
        $this->index = $index;
        $this->value = $value;
        $this->name = $name;
    }

    public function getValue() { return $this->value; }
    public function getName() { return $this->name; }
    public function getIndex() { return $this->index; }
    
    public function getCell() {
        return new Cell($this->row, $this->index, $this->value, $this->name);
    }

    public function trim() { return trim((string)$this->value); }
    public function toUpper() { return strtoupper((string)$this->value); }
    public function toLower() { return strtolower((string)$this->value); }
    
    public function __toString() { return (string)$this->value; }
}
