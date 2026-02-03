<?php

declare(strict_types=1);

/**
 * CsvQuery PHP Benchmark Suite
 * 
 * Run: php benchmarks/php/benchmark.php
 */

require_once __DIR__ . '/../../vendor/autoload.php';

use CsvQuery\CsvQuery;

class Benchmark
{
    private string $csvPath;
    private string $indexDir;
    private int $rowCount = 10000;
    
    public function __construct()
    {
        $this->indexDir = sys_get_temp_dir() . '/csvquery_bench_' . getmypid();
        $this->csvPath = $this->indexDir . '/benchmark.csv';
        
        if (!is_dir($this->indexDir)) {
            mkdir($this->indexDir, 0755, true);
        }
    }

    public function run(): void
    {
        echo "\n╔══════════════════════════════════════════════════════════════╗\n";
        echo "║           CsvQuery PHP Benchmark Suite                       ║\n";
        echo "╚══════════════════════════════════════════════════════════════╝\n\n";

        $this->generateTestData();
        
        $this->benchmark('Indexing Single Column', fn() => $this->benchmarkIndexing());
        $this->benchmark('Simple Query (no index)', fn() => $this->benchmarkSimpleQuery(false));
        $this->benchmark('Simple Query (with index)', fn() => $this->benchmarkSimpleQuery(true));
        $this->benchmark('Count Query', fn() => $this->benchmarkCount());
        $this->benchmark('Query Builder Chaining', fn() => $this->benchmarkQueryBuilder());
        
        $this->cleanup();
        
        echo "\n✓ Benchmark complete\n\n";
    }

    private function generateTestData(): void
    {
        echo "Generating {$this->rowCount} test rows...\n";
        
        $categories = ['A', 'B', 'C', 'D', 'E'];
        $statuses = ['active', 'inactive', 'pending'];
        
        $handle = fopen($this->csvPath, 'w');
        fputcsv($handle, ['ID', 'NAME', 'STATUS', 'SCORE', 'CATEGORY']);
        
        for ($i = 1; $i <= $this->rowCount; $i++) {
            fputcsv($handle, [
                $i,
                "User_{$i}",
                $statuses[array_rand($statuses)],
                rand(50, 100),
                $categories[array_rand($categories)],
            ]);
        }
        
        fclose($handle);
        echo "✓ Generated {$this->csvPath}\n\n";
    }

    private function benchmark(string $name, callable $fn): void
    {
        $iterations = 5;
        $times = [];
        
        for ($i = 0; $i < $iterations; $i++) {
            $start = microtime(true);
            $fn();
            $times[] = microtime(true) - $start;
        }
        
        $avg = array_sum($times) / count($times);
        $min = min($times);
        $max = max($times);
        
        printf("%-35s  avg: %8.3fms  min: %8.3fms  max: %8.3fms\n",
            $name,
            $avg * 1000,
            $min * 1000,
            $max * 1000
        );
    }

    private function benchmarkIndexing(): void
    {
        $csv = new CsvQuery($this->csvPath, ['indexDir' => $this->indexDir]);
        $csv->clearIndexes();
        $csv->createIndex(['STATUS']);
    }

    private function benchmarkSimpleQuery(bool $useIndex): void
    {
        $csv = new CsvQuery($this->csvPath, ['indexDir' => $this->indexDir]);
        
        if ($useIndex && !$csv->hasIndex('STATUS')) {
            $csv->createIndex(['STATUS']);
        }
        
        $results = $csv->where(['STATUS' => 'active'])->limit(100)->all();
    }

    private function benchmarkCount(): void
    {
        $csv = new CsvQuery($this->csvPath, ['indexDir' => $this->indexDir]);
        $count = $csv->find()->where(['STATUS' => 'active'])->count();
    }

    private function benchmarkQueryBuilder(): void
    {
        $csv = new CsvQuery($this->csvPath, ['indexDir' => $this->indexDir]);
        
        // Complex chained query
        $query = $csv->find()
            ->select(['ID', 'NAME', 'STATUS'])
            ->where(['STATUS' => 'active'])
            ->andWhere(['>', 'SCORE', 70])
            ->orderBy(['SCORE' => SORT_DESC])
            ->limit(50);
        
        $results = $query->all();
    }

    private function cleanup(): void
    {
        // Clean up temp files
        if (file_exists($this->csvPath)) {
            unlink($this->csvPath);
        }
        array_map('unlink', glob($this->indexDir . '/*'));
        rmdir($this->indexDir);
    }
}

(new Benchmark())->run();
