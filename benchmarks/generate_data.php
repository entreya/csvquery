<?php
/**
 * Data Generator for Benchmarking
 * Generates a 1 million row CSV.
 */

$file = 'benchmarks/large_data.csv';
$rows = 1000000;
$handle = fopen($file, 'w');

// Headers
fputcsv($handle, ['ID', 'NAME', 'EMAIL', 'DEPARTMENT', 'SALARY', 'JOIN_DATE']);

$depts = ['Engineering', 'Sales', 'Marketing', 'Support', 'HR'];

for ($i = 1; $i <= $rows; $i++) {
    fputcsv($handle, [
        $i,
        "User_$i",
        "user$i@example.com",
        $depts[array_rand($depts)],
        rand(30000, 150000),
        date('Y-m-d', strtotime("-" . rand(1, 3650) . " days"))
    ]);
    
    if ($i % 100000 === 0) {
        echo "Generated $i rows...\n";
    }
}

fclose($handle);
echo "✅ Generated $file with $rows rows.\n";
