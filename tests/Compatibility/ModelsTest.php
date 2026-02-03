<?php

declare(strict_types=1);

namespace CsvQuery\Tests\Compatibility;

use PHPUnit\Framework\TestCase;
use CsvQuery\CsvQuery;
use CsvQuery\Models\Row;
use CsvQuery\Models\Cell;
use CsvQuery\Models\Column;

/**
 * Compatibility tests for Model classes.
 * 
 * Tests Row, Cell, and Column wrapper classes.
 * CRITICAL: These tests must pass before and after any refactoring.
 */
class ModelsTest extends TestCase
{
    private static string $csvPath;
    private static ?CsvQuery $csv = null;

    public static function setUpBeforeClass(): void
    {
        self::$csvPath = __DIR__ . '/../fixtures/sample.csv';
        self::$csv = new CsvQuery(self::$csvPath);
    }

    // ========================================
    // Row Tests
    // ========================================

    public function testRowCreation(): void
    {
        $row = self::$csv->find()->one();
        $this->assertNotNull($row);
    }

    public function testRowArrayAccess(): void
    {
        $row = self::$csv->find()->one();
        if ($row instanceof Row) {
            $this->assertTrue(isset($row['ID']));
            $this->assertEquals('1', $row['ID']);
        } else {
            $this->assertIsArray($row);
        }
    }

    public function testRowMagicGet(): void
    {
        $row = self::$csv->find()->one();
        if ($row instanceof Row) {
            $this->assertEquals('Alice Smith', $row->NAME);
        }
    }

    public function testRowToAssociativeArray(): void
    {
        $row = self::$csv->find()->one();
        if ($row instanceof Row) {
            $arr = $row->toAssociativeArray();
            $this->assertIsArray($arr);
            $this->assertArrayHasKey('ID', $arr);
            $this->assertArrayHasKey('NAME', $arr);
        }
    }

    public function testRowIteration(): void
    {
        $row = self::$csv->find()->one();
        if ($row instanceof Row) {
            $count = 0;
            foreach ($row as $key => $value) {
                $count++;
            }
            $this->assertEquals(5, $count); // 5 columns
        }
    }

    public function testRowJsonSerialize(): void
    {
        $row = self::$csv->find()->one();
        if ($row instanceof Row) {
            $json = json_encode($row);
            $this->assertIsString($json);
            $decoded = json_decode($json, true);
            $this->assertArrayHasKey('ID', $decoded);
        }
    }

    public function testRowToJson(): void
    {
        $row = self::$csv->find()->one();
        if ($row instanceof Row) {
            $json = $row->toJson();
            $this->assertIsString($json);
        }
    }

    public function testRowToString(): void
    {
        $row = self::$csv->find()->one();
        if ($row instanceof Row) {
            $str = (string) $row;
            $this->assertIsString($str);
        }
    }

    public function testRowGetColumn(): void
    {
        $row = self::$csv->find()->one();
        if ($row instanceof Row) {
            $col = $row->getColumn('NAME');
            $this->assertInstanceOf(Column::class, $col);
        }
    }

    public function testRowGetCell(): void
    {
        $row = self::$csv->find()->one();
        if ($row instanceof Row) {
            $cell = $row->getCell('NAME');
            $this->assertInstanceOf(Cell::class, $cell);
        }
    }

    // ========================================
    // Cell Tests
    // ========================================

    public function testCellGetValue(): void
    {
        $row = self::$csv->find()->one();
        if ($row instanceof Row) {
            $cell = $row->getCell('SCORE');
            $this->assertEquals('95', $cell->getValue());
        }
    }

    public function testCellIsEmpty(): void
    {
        $row = self::$csv->find()->one();
        if ($row instanceof Row) {
            $cell = $row->getCell('NAME');
            $this->assertFalse($cell->isEmpty());
        }
    }

    public function testCellIsNumeric(): void
    {
        $row = self::$csv->find()->one();
        if ($row instanceof Row) {
            $scoreCell = $row->getCell('SCORE');
            $nameCell = $row->getCell('NAME');
            
            $this->assertTrue($scoreCell->isNumeric());
            $this->assertFalse($nameCell->isNumeric());
        }
    }

    public function testCellAsInt(): void
    {
        $row = self::$csv->find()->one();
        if ($row instanceof Row) {
            $cell = $row->getCell('SCORE');
            $this->assertEquals(95, $cell->asInt());
        }
    }

    public function testCellAsFloat(): void
    {
        $row = self::$csv->find()->one();
        if ($row instanceof Row) {
            $cell = $row->getCell('SCORE');
            $this->assertEquals(95.0, $cell->asFloat());
        }
    }

    public function testCellAsString(): void
    {
        $row = self::$csv->find()->one();
        if ($row instanceof Row) {
            $cell = $row->getCell('ID');
            $this->assertEquals('1', $cell->asString());
        }
    }

    public function testCellAsBool(): void
    {
        $row = self::$csv->find()->one();
        if ($row instanceof Row) {
            $cell = $row->getCell('ID');
            $this->assertTrue($cell->asBool()); // '1' should be true
        }
    }

    public function testCellToString(): void
    {
        $row = self::$csv->find()->one();
        if ($row instanceof Row) {
            $cell = $row->getCell('NAME');
            $this->assertEquals('Alice Smith', (string) $cell);
        }
    }

    public function testCellValidate(): void
    {
        $row = self::$csv->find()->one();
        if ($row instanceof Row) {
            $cell = $row->getCell('NAME');
            $result = $cell->validate(['required']);
            $this->assertTrue($result['valid']);
        }
    }

    // ========================================
    // Column Tests
    // ========================================

    public function testColumnGetValue(): void
    {
        $row = self::$csv->find()->one();
        if ($row instanceof Row) {
            $col = $row->getColumn('NAME');
            $this->assertEquals('Alice Smith', $col->getValue());
        }
    }

    public function testColumnGetName(): void
    {
        $row = self::$csv->find()->one();
        if ($row instanceof Row) {
            $col = $row->getColumn('NAME');
            $this->assertEquals('NAME', $col->getName());
        }
    }

    public function testColumnGetIndex(): void
    {
        $row = self::$csv->find()->one();
        if ($row instanceof Row) {
            $col = $row->getColumn('NAME');
            $this->assertEquals(1, $col->getIndex()); // NAME is at index 1
        }
    }

    public function testColumnGetCell(): void
    {
        $row = self::$csv->find()->one();
        if ($row instanceof Row) {
            $col = $row->getColumn('NAME');
            $cell = $col->getCell();
            $this->assertInstanceOf(Cell::class, $cell);
        }
    }

    public function testColumnTrim(): void
    {
        $row = self::$csv->find()->one();
        if ($row instanceof Row) {
            $col = $row->getColumn('NAME');
            $this->assertEquals('Alice Smith', $col->trim());
        }
    }

    public function testColumnToUpper(): void
    {
        $row = self::$csv->find()->one();
        if ($row instanceof Row) {
            $col = $row->getColumn('STATUS');
            $this->assertEquals('ACTIVE', $col->toUpper());
        }
    }

    public function testColumnToLower(): void
    {
        $row = self::$csv->find()->one();
        if ($row instanceof Row) {
            $col = $row->getColumn('STATUS');
            $this->assertEquals('active', $col->toLower());
        }
    }

    public function testColumnToString(): void
    {
        $row = self::$csv->find()->one();
        if ($row instanceof Row) {
            $col = $row->getColumn('NAME');
            $this->assertEquals('Alice Smith', (string) $col);
        }
    }
}
