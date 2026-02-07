<?php

namespace Entreya\CsvQuery\Models;

use ArrayAccess;
use IteratorAggregate;
use ArrayIterator;
use Traversable;

use JsonSerializable;

/**
 * Row - Represents a single record in CsvQuery result set.
 */
class Row implements ArrayAccess, IteratorAggregate, JsonSerializable
{
    private $csvQuery;
    private $data;
    private $lineNumber;

    public function __construct($csvQuery, array $data, $lineNumber = null)
    {
        $this->csvQuery = $csvQuery;
        $this->data = $data;
        $this->lineNumber = $lineNumber;
    }

    public function getColumn($name)
    {
        $map = $this->csvQuery->getHeaderMap();
        $index = is_int($name) ? $name : ($map[$name] ?? null);
        if ($index === null) return null;
        
        $colName = is_int($name) ? ($this->csvQuery->getHeaders()[$index] ?? null) : $name;
        return new Column($this, $index, $this->data[$colName] ?? $this->data[$index] ?? null, $colName);
    }

    public function getCell($name)
    {
        return $this->getColumn($name)->getCell();
    }

    public function toAssociativeArray()
    {
        return $this->data;
    }

    public function getLineNumber() { return $this->lineNumber; }

    /** Magic access */
    public function __get($name)
    {
        return $this->data[$name] ?? null;
    }

    /** ArrayAccess */
    public function offsetExists($offset): bool { return isset($this->data[$offset]); }
    public function offsetGet($offset): mixed { return $this->data[$offset] ?? null; }
    public function offsetSet($offset, $value): void { $this->data[$offset] = $value; }
    public function offsetUnset($offset): void { unset($this->data[$offset]); }

    public function getIterator(): Traversable
    {
        return new ArrayIterator($this->data);
    }

    public function jsonSerialize(): mixed
    {
        return $this->data;
    }

    public function toJson($options = 0) { return json_encode($this->data, $options); }
    public function __toString() { return $this->toJson(); }
}
