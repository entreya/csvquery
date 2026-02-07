<?php
/**
 * Baseline benchmark script using native PHP fgetcsv.
 * Usage: php benchmarks/php_baseline.php <csv_file>
 */

if ($argc < 2) {
    echo "Usage: php php_baseline.php <csv_file>\n";
    exit(1);
}

$csvFile = $argv[1];
$count = 0;

if (($handle = fopen($csvFile, "r")) !== FALSE) {
    while (($data = fgetcsv($handle, 1000, ",")) !== FALSE) {
        $count++;
    }
    fclose($handle);
}

echo "Processed $count rows.\n";
