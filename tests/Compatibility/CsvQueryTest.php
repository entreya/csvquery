<?php

declare(strict_types=1);

namespace Entreya\CsvQuery\Tests\Compatibility;

use PHPUnit\Framework\TestCase;
use Entreya\CsvQuery\Core\CsvQuery;
use Entreya\CsvQuery\Query\ActiveQuery;

/**
 * Compatibility tests for CsvQuery entry point.
 * 
 * These tests ensure backward compatibility of all public APIs.
 * CRITICAL: These tests must pass before and after any refactoring.
 */
class CsvQueryTest extends TestCase
{
    private static string $csvPath;
    private static string $indexDir;
    private static ?CsvQuery $csv = null;

    public static function setUpBeforeClass(): void
    {
        self::$csvPath = __DIR__ . '/../fixtures/sample.csv';
        self::$indexDir = sys_get_temp_dir() . '/csvquery_test_' . getmypid();
        
        if (!is_dir(self::$indexDir)) {
            mkdir(self::$indexDir, 0755, true);
        }
        
        self::$csv = new CsvQuery(self::$csvPath, [
            'indexDir' => self::$indexDir,
        ]);
    }

    public static function tearDownAfterClass(): void
    {
        // Clean up temp indexes recursively
        if (is_dir(self::$indexDir)) {
            self::removeDirRecursive(self::$indexDir);
        }
        self::$csv = null;
    }

    /**
     * Recursively remove a directory.
     */
    private static function removeDirRecursive(string $dir): void
    {
        if (!is_dir($dir)) {
            return;
        }

        $files = array_diff(scandir($dir), ['.', '..']);
        foreach ($files as $file) {
            $path = "$dir/$file";
            is_dir($path) ? self::removeDirRecursive($path) : unlink($path);
        }

        rmdir($dir);
    }

    // ========================================
    // Constructor Tests
    // ========================================

    public function testConstructorWithValidPath(): void
    {
        $csv = new CsvQuery(self::$csvPath);
        $this->assertInstanceOf(CsvQuery::class, $csv);
    }

    public function testConstructorWithOptions(): void
    {
        $csv = new CsvQuery(self::$csvPath, [
            'indexDir' => self::$indexDir,
            'separator' => ',',
        ]);
        $this->assertEquals(self::$indexDir, $csv->getIndexDir());
        $this->assertEquals(',', $csv->getSeparator());
    }

    public function testConstructorWithInvalidPath(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        new CsvQuery('/nonexistent/file.csv');
    }

    // ========================================
    // Header Methods
    // ========================================

    public function testGetHeaders(): void
    {
        $headers = self::$csv->getHeaders();
        $this->assertIsArray($headers);
        $this->assertEquals(['ID', 'NAME', 'STATUS', 'SCORE', 'CATEGORY'], $headers);
    }

    public function testGetHeaderMap(): void
    {
        $map = self::$csv->getHeaderMap();
        $this->assertIsArray($map);
        $this->assertEquals(0, $map['ID']);
        $this->assertEquals(1, $map['NAME']);
        $this->assertEquals(2, $map['STATUS']);
    }

    public function testGetCsvPath(): void
    {
        $this->assertEquals(realpath(self::$csvPath), realpath(self::$csv->getCsvPath()));
    }

    public function testGetSeparator(): void
    {
        $this->assertEquals(',', self::$csv->getSeparator());
    }

    public function testGetIndexDir(): void
    {
        $this->assertEquals(self::$indexDir, self::$csv->getIndexDir());
    }

    // ========================================
    // Query Factory Methods
    // ========================================

    public function testFindReturnsActiveQuery(): void
    {
        $query = self::$csv->find();
        $this->assertInstanceOf(ActiveQuery::class, $query);
    }

    public function testWhereReturnsActiveQuery(): void
    {
        $query = self::$csv->where(['STATUS' => 'active']);
        $this->assertInstanceOf(ActiveQuery::class, $query);
    }

    public function testAndWhereReturnsActiveQuery(): void
    {
        $query = self::$csv->andWhere(['STATUS' => 'active']);
        $this->assertInstanceOf(ActiveQuery::class, $query);
    }

    // ========================================
    // Index Management
    // ========================================

    public function testCreateIndex(): void
    {
        $result = self::$csv->createIndex(['STATUS']);
        $this->assertTrue($result);
    }

    public function testHasIndexAfterCreate(): void
    {
        // Ensure index exists from previous test
        if (!self::$csv->hasIndex('STATUS')) {
            self::$csv->createIndex(['STATUS']);
        }
        $this->assertTrue(self::$csv->hasIndex('STATUS'));
    }

    public function testHasIndexForNonexistent(): void
    {
        $this->assertFalse(self::$csv->hasIndex('NONEXISTENT_COLUMN'));
    }

    public function testGetIndexPath(): void
    {
        $path = self::$csv->getIndexPath('STATUS');
        $this->assertIsString($path);
        $this->assertStringContainsString('status', $path);
    }

    public function testDropIndex(): void
    {
        // Create then drop
        self::$csv->createIndex(['CATEGORY']);
        $this->assertTrue(self::$csv->hasIndex('CATEGORY'));
        
        $result = self::$csv->dropIndex('CATEGORY');
        $this->assertTrue($result);
    }

    // ========================================
    // Utility Methods
    // ========================================

    public function testGetGoBridge(): void
    {
        $bridge = self::$csv->getGoBridge();
        $this->assertInstanceOf(\Entreya\CsvQuery\Bridge\GoBridge::class, $bridge);
    }

    public function testGetMeta(): void
    {
        $meta = self::$csv->getMeta();
        $this->assertIsArray($meta);
    }

    public function testValidateIntegrity(): void
    {
        // After fresh index creation, should be valid
        self::$csv->createIndex(['STATUS']);
        $result = self::$csv->validateIntegrity();
        $this->assertIsBool($result);
    }
}
