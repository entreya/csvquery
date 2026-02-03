<?php
/**
 * CsvQuery Quick Start Example
 * 
 * This example demonstrates basic usage of the CsvQuery library.
 */

declare(strict_types=1);

require_once __DIR__ . '/../vendor/autoload.php';

use CsvQuery\CsvQuery;

// Configuration
$csvPath = __DIR__ . '/sample.csv';
$indexDir = __DIR__ . '/indexes';

// Create sample CSV if it doesn't exist
if (!file_exists($csvPath)) {
    echo "Creating sample CSV file...\n";
    
    $headers = ['ID', 'NAME', 'STATUS', 'SCORE', 'CATEGORY'];
    $rows = [];
    
    $statuses = ['active', 'inactive', 'pending'];
    $categories = ['A', 'B', 'C', 'D'];
    
    for ($i = 1; $i <= 10000; $i++) {
        $rows[] = [
            $i,
            "User_$i",
            $statuses[array_rand($statuses)],
            rand(50, 100),
            $categories[array_rand($categories)],
        ];
    }
    
    $fp = fopen($csvPath, 'w');
    fputcsv($fp, $headers);
    foreach ($rows as $row) {
        fputcsv($fp, $row);
    }
    fclose($fp);
    
    echo "Created sample CSV with " . count($rows) . " rows\n\n";
}

// Ensure index directory exists
if (!is_dir($indexDir)) {
    mkdir($indexDir, 0755, true);
}

try {
    // Initialize CsvQuery
    echo "=== CsvQuery Quick Start ===\n\n";
    
    $csv = new CsvQuery($csvPath, [
        'indexDir' => $indexDir,
    ]);
    
    echo "CSV loaded: " . basename($csvPath) . "\n";
    echo "Headers: " . implode(', ', $csv->getHeaders()) . "\n\n";
    
    // Create indexes (one-time operation)
    echo "--- Creating Indexes ---\n";
    $start = microtime(true);
    
    if (!$csv->hasIndex('STATUS')) {
        $csv->createIndex(['STATUS', 'CATEGORY'], true);
        echo sprintf("Indexes created in %.2f ms\n\n", (microtime(true) - $start) * 1000);
    } else {
        echo "Indexes already exist, skipping...\n\n";
    }
    
    // Basic count
    echo "--- Basic Queries ---\n";
    
    $start = microtime(true);
    $totalCount = $csv->find()->count();
    echo sprintf("Total rows: %d (%.2f ms)\n", $totalCount, (microtime(true) - $start) * 1000);
    
    // Filtered count
    $start = microtime(true);
    $activeCount = $csv->find()->where(['STATUS' => 'active'])->count();
    echo sprintf("Active rows: %d (%.2f ms)\n", $activeCount, (microtime(true) - $start) * 1000);
    
    // Select with conditions
    echo "\n--- Select Query ---\n";
    
    $start = microtime(true);
    $results = $csv->find()
        ->select(['ID', 'NAME', 'SCORE'])
        ->where(['STATUS' => 'active'])
        ->andWhere(['>', 'SCORE', 90])
        ->orderBy(['SCORE' => SORT_DESC])
        ->limit(5)
        ->all();
    
    echo sprintf("Top 5 active users with score > 90 (%.2f ms):\n", (microtime(true) - $start) * 1000);
    foreach ($results as $row) {
        echo sprintf("  - %s: %s (Score: %d)\n", $row['ID'], $row['NAME'], $row['SCORE']);
    }
    
    // Aggregation
    echo "\n--- Aggregations ---\n";
    
    $start = microtime(true);
    $avgScore = $csv->find()->where(['STATUS' => 'active'])->average('SCORE');
    echo sprintf("Average score (active): %.2f (%.2f ms)\n", $avgScore, (microtime(true) - $start) * 1000);
    
    // Group By
    echo "\n--- Group By ---\n";
    
    $start = microtime(true);
    $stats = $csv->find()->groupBy('CATEGORY')->count();
    echo sprintf("Rows per category (%.2f ms):\n", (microtime(true) - $start) * 1000);
    if (is_array($stats)) {
        foreach ($stats as $category => $count) {
            echo "  - $category: $count\n";
        }
    }
    
    // Explain query plan
    echo "\n--- Query Plan ---\n";
    $plan = $csv->find()->where(['STATUS' => 'active'])->explain();
    echo "Strategy: " . ($plan['strategy'] ?? 'unknown') . "\n";
    echo "Index used: " . ($plan['index'] ?? 'none') . "\n";
    
    echo "\n=== Done! ===\n";
    
} catch (\Exception $e) {
    echo "Error: " . $e->getMessage() . "\n";
    exit(1);
}
