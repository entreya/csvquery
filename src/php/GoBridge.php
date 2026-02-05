<?php

/**
 * GoBridge - PHP wrapper for the CsvQuery Go binary.
 *
 * Handles:
 * - OS/architecture detection
 * - Binary selection
 * - Process execution
 * - Error handling
 *
 * @package CsvQuery
 */

declare(strict_types=1);

namespace CsvQuery;

/**
 * Bridge between PHP and the Go csvquery binary.
 */
class GoBridge
{
    /** @var string Path to Go binary */
    private string $binaryPath;

    /** @var int Number of workers */
    private int $workers;

    /** @var int Memory per worker in MB */
    private int $memoryMB;

    /** @var string Last stderr output for profiling */
    private string $lastStderr = '';

    /** @var bool Whether to use Unix socket for queries */
    private bool $useSocket = true;

    /** @var string Index directory for socket client */
    private string $indexDir = '';

    /**
     * Get the last stderr output (useful for profiling).
     *
     * @return string
     */
    public function getLastStderr(): string
    {
        return $this->lastStderr;
    }


    /**
     * Create a Go bridge.
     *
     * @param array $options Configuration:
     *   - 'workers': Number of workers (0 = auto)
     *   - 'memoryMB': Memory per worker
     *   - 'binaryPath': Override binary path
     *   - 'useSocket': Use Unix socket (default: true)
     *   - 'indexDir': Index directory for socket client
     */
    public function __construct(array $options = [])
    {
        $this->workers = $options['workers'] ?? 0;
        $this->memoryMB = $options['memoryMB'] ?? 500;
        $this->useSocket = $options['useSocket'] ?? true;
        $this->indexDir = $options['indexDir'] ?? '';

        if (isset($options['binaryPath'])) {
            $this->binaryPath = $options['binaryPath'];
        } else {
            $this->binaryPath = $this->detectBinary();
        }

        if (!file_exists($this->binaryPath)) {
            throw new \RuntimeException("CsvQuery binary not found: {$this->binaryPath}");
        }

        // Configure socket client if enabled
        if ($this->useSocket) {
            SocketClient::configure($this->binaryPath, $this->indexDir);
        }
    }


    /**
     * Detect the correct binary for the current OS/architecture.
     *
     * @return string Path to binary
     */
    private function detectBinary(): string
    {
        $binDir = dirname(__DIR__, 2) . '/bin';

        // Detect OS
        $os = match (PHP_OS_FAMILY) {
            'Darwin' => 'darwin',
            'Windows' => 'windows',
            default => 'linux',
        };

        // Detect architecture
        $arch = match (php_uname('m')) {
            'arm64', 'aarch64' => 'arm64',
            default => 'amd64',
        };

        $ext = $os === 'windows' ? '.exe' : '';
        $binary = "{$binDir}/csvquery_{$os}_{$arch}{$ext}";

        // Fall back to default if specific binary not found
        if (!file_exists($binary)) {
            $defaultBinary = "{$binDir}/csvquery{$ext}";
            if (file_exists($defaultBinary)) {
                return $defaultBinary;
            }
        }
        return $binary;
    }

    /**
     * Create indexes for a CSV file.
     *
     * @param string $csvPath Path to CSV file
     * @param string $outputDir Output directory
     * @param string $columnsJson JSON array of columns
     * @param string $separator CSV separator
     * @param bool $verbose Enable verbose output
     * @return bool Success status
     */
    public function createIndex(
        string $csvPath,
        string $outputDir,
        string $columnsJson,
        string $separator = ',',
        bool $verbose = false,
        array $options = []
    ): bool {
        $args = [
            'index',
            '--input', escapeshellarg($csvPath),
            '--output', escapeshellarg($outputDir),
            '--columns', escapeshellarg($columnsJson),
            '--separator', escapeshellarg($separator),
        ];

        $workers = $options['workers'] ?? $this->workers;
        if ($workers > 0) {
            $args[] = '--workers';
            $args[] = (string) $workers;
        }

        $memoryMB = $options['memoryMB'] ?? $this->memoryMB;
        if ($memoryMB > 0) {
            $args[] = '--memory';
            $args[] = (string) $memoryMB;
        }

        if ($verbose) {
            $args[] = '--verbose';
        }

        return $this->execute($args, $verbose);
    }

    /**
     * Query an index and stream matching offsets.
     *
     * Returns a Generator that reads offsets one at a time from Go process.
     * Memory efficient - never loads all offsets into RAM.
     *
     * @param string $indexDir Directory containing .didx files
     * @param array $where Column => Value conditions
     * @param int $limit Maximum results (0 = no limit)
     * @param int $offset Skip first N results
     * @return \Generator<array{offset: int, line: int}>
     * @param bool $explain Enable query plan explanation
     * @param string|null $groupBy Column to group by for aggregation
     * @param string|null $aggCol Column to aggregate
     * @param string|null $aggFunc Aggregation function (e.g., 'count', 'sum', 'avg')
     * @return \Generator<array{offset: int, line: int}>|array
     */
    public function query(
        string $csvPath,
        string $indexDir,
        array $where,
        int $limit = 0,
        int $offset = 0,
        bool $explain = false,
        ?string $groupBy = null,
        ?string $aggCol = null,
        ?string $aggFunc = null
    ): \Generator|array {
        $binaryPath = $this->getBinaryPath();
        if (!$binaryPath) {
            throw new \RuntimeException("CsvQuery binary not found");
        }

        $args = [
            'query',
            '--csv', $csvPath,
            '--index-dir', $indexDir,
            '--where', json_encode(empty($where) ? new \stdClass() : $where),
        ];

        if ($limit > 0) {
            $args[] = '--limit';
            $args[] = (string) $limit;
        }

        if ($offset > 0) {
            $args[] = '--offset';
            $args[] = (string) $offset;
        }

        if ($explain) {
            $args[] = '--explain';
        }

        if (!empty($groupBy)) {
            $args[] = '--group-by';
            $args[] = $groupBy;
        }

        if (!empty($aggCol)) {
            $args[] = '--agg-col';
            $args[] = $aggCol;
        }

        if (!empty($aggFunc)) {
            $args[] = '--agg-func';
            $args[] = $aggFunc;
        }

        $escapedArgs = array_map('escapeshellarg', $args);
        $executionString = escapeshellcmd($this->binaryPath) . ' ' . implode(' ', $escapedArgs);

        // If Grouping or Explaining, we expect a JSON response, not a stream
        if ($explain || !empty($groupBy) || !empty($aggFunc)) {
            $output = [];
            $exitCode = 0;
            
            if ($this->debug) {
                echo "[DEBUG] Executing (JSON): $executionString\n";
            }

            // Execute preventing shell expansion but capturing output
            exec($executionString . ' 2>&1', $output, $exitCode);
            
            if ($this->debug) {
                 echo "[DEBUG] Exit Code: $exitCode\n";
                 echo "[DEBUG] Raw Output: " . implode("\n", $output) . "\n";
            }

            $err = implode("\n", $output);
            $this->validateExecution($exitCode, $err);
            // Fallback (validateExecution throws, but if logic changes)
            if ($exitCode !== 0) {
                 throw new \RuntimeException("Aggregation Failed: $err");
            }
            
            // Extract the first valid JSON line. 
            // The Go binary might output stats or other info.
            $json = '';
            foreach ($output as $line) {
                if (str_starts_with(trim($line), '{') || str_starts_with(trim($line), '[')) {
                    $json = $line;
                    break;
                }
            }
            
            $data = json_decode($json, true);
            
            if (json_last_error() !== JSON_ERROR_NONE) {
                 if ($this->debug) {
                     echo "[DEBUG] JSON Decode Error: " . json_last_error_msg() . "\n";
                     echo "[DEBUG] JSON Content: $json\n";
                 }
                 return [];
            }
            
            return $data;
        }

        return $this->streamOutput($executionString);
    }

    private function queryCli(string $executionString): \Generator 
    {
        return $this->streamOutput($executionString);
    }

    /* Old streamOutput logic kept for CLI fallback */
    private function streamOutput(string $executionString): \Generator
    {
        $descriptors = [
            0 => ['pipe', 'r'],
            1 => ['pipe', 'w'], // stdout
            2 => ['pipe', 'w'], // stderr
        ];

        $process = proc_open($executionString, $descriptors, $processPipes);

        if ($this->debug) {
            echo "[DEBUG] Executing: $executionString\n";
        }

        if (!is_resource($process)) {
            throw new \RuntimeException("Failed to start process");
        }

        fclose($processPipes[0]);

        while (($line = fgets($processPipes[1])) !== false) {
            $line = trim($line);
            if ($line === '') continue;

            $parts = explode(',', $line);
            if (count($parts) >= 2) {
                yield [
                    'offset' => (int) $parts[0],
                    'line' => (int) $parts[1],
                ];
            }
        }

        $stderr = stream_get_contents($processPipes[2]);
        $this->lastStderr = $stderr;
        fclose($processPipes[1]);
        fclose($processPipes[2]);

        $exitCode = proc_close($process);

        if ($exitCode !== 0 && $stderr !== '') {
            $this->validateExecution($exitCode, $stderr);
        }
    }

    /**
     * Count matching records (fast - Go only counts, no data transfer).
     *
     * @param string $csvPath Path to CSV
     * @param string $indexDir Directory containing .cidx files
     * @param array $where Column => Value conditions
     * @return int Number of matches
     */
    public function count(string $csvPath, string $indexDir, array $where): int
    {
        // Try socket first if enabled
        if ($this->useSocket) {
            try {
                return SocketClient::getInstance()->count($csvPath, $where);
            } catch (\Exception $e) {
                // Fallback to spawn on socket errors
                if ($this->debug) {
                    echo "[DEBUG] Socket error, falling back to spawn: " . $e->getMessage() . "\n";
                }
            }
        }

        // Fallback to spawn
        return $this->countViaSpawn($csvPath, $indexDir, $where);
    }

    /**
     * Count via spawning Go process (fallback).
     */
    private function countViaSpawn(string $csvPath, string $indexDir, array $where): int
    {
        $args = [
            'query',
            '--csv', $csvPath,
            '--index-dir', $indexDir,
            '--where', json_encode(empty($where) ? new \stdClass() : $where),
            '--count',
        ];

        $cmd = array_map('escapeshellarg', $args);
        $commandStr = escapeshellcmd($this->binaryPath) . ' ' . implode(' ', $cmd);

        $output = [];
        $exitCode = 0;
        
        if ($this->debug) {
            echo "[DEBUG] Executing: $commandStr\n";
        }
        
        exec($commandStr . ' 2>&1', $output, $exitCode);

        $err = implode("\n", $output);
        $this->validateExecution($exitCode, $err);
        
        if ($exitCode !== 0) {
            throw new \RuntimeException("csvquery count failed: " . $err);
        }

        return (int) trim($output[0] ?? '0');
    }



    /**
     * Execute the Go binary.
     *
     * @param array $args Command line arguments
     * @param bool $passthrough Pass output directly to stdout
     * @return bool Success status
     */
    private function execute(array $args, bool $passthrough = false): bool
    {
        $executionString = escapeshellcmd($this->binaryPath) . ' ' . implode(' ', $args);

        $descriptors = [
            0 => ['pipe', 'r'],  // stdin
            1 => ['pipe', 'w'],  // stdout
            2 => ['pipe', 'w'],  // stderr
        ];

        $process = proc_open($executionString, $descriptors, $processPipes);

        if (!is_resource($process)) {
            throw new \RuntimeException("Failed to start csvquery process");
        }

        // Close stdin
        fclose($processPipes[0]);

        // Read output
        $stdout = '';
        $stderr = '';

        while (true) {
            $read = [$processPipes[1], $processPipes[2]];
            $write = null;
            $except = null;

            if (false === stream_select($read, $write, $except, 1)) {
                break;
            }

            foreach ($read as $pipe) {
                $data = fread($pipe, 8192);
                if ($data === false || $data === '') {
                    continue;
                }

                if ($pipe === $processPipes[1]) {
                    $stdout .= $data;
                    if ($passthrough) {
                        echo $data;
                    }
                } else {
                    $stderr .= $data;
                    if ($passthrough) {
                        fwrite(STDERR, $data);
                    }
                }
            }

            // Check if both pipes are closed
            if (feof($processPipes[1]) && feof($processPipes[2])) {
                break;
            }
        }

        $this->lastStderr = $stderr;

        fclose($processPipes[1]);
        fclose($processPipes[2]);

        $exitCode = proc_close($process);

        if ($exitCode !== 0) {
            throw new \RuntimeException(
                "csvquery failed with code $exitCode: $stderr"
            );
        }

        return true;
    }

    /**
     * Get the path to the Go binary.
     *
     * @return string
     */
    public function getBinaryPath(): string
    {
        return $this->binaryPath;
    }

    /**
     * Build command string.
     *
     * @param array $args
     * @return string
     */
    private function buildCommand(array $args): string
    {
        $escapedArgs = array_map('escapeshellarg', $args);
        return escapeshellcmd($this->binaryPath) . ' ' . implode(' ', $escapedArgs);
    }

    /**
     * Write rows to CSV.
     *
     * @param string $csvPath Path to CSV
     * @param array $rows Rows to write (array of arrays)
     * @param array $headers Headers (optional, for new file)
     * @param string $separator CSV separator
     * @return void
     * @throws \RuntimeException If write fails
     */
    public function write(string $csvPath, array $rows, array $headers = [], string $separator = ','): void
    {
        $args = [
            'write',
            '--csv', $csvPath,
            '--data', json_encode($rows),
            '--separator', $separator
        ];

        if (!empty($headers)) {
            $args[] = '--headers';
            $args[] = json_encode($headers);
        }

        $executionString = $this->buildCommand($args);
        
        // Execute
        $process = proc_open($executionString, [
            0 => ['pipe', 'r'], // stdin
            1 => ['pipe', 'w'], // stdout
            2 => ['pipe', 'w'], // stderr
        ], $processPipes);

        if (!is_resource($process)) {
            throw new \RuntimeException("Failed to launch Go binary");
        }

        fclose($processPipes[0]);
        $stdout = stream_get_contents($processPipes[1]);
        $stderr = stream_get_contents($processPipes[2]);
        fclose($processPipes[1]);
        fclose($processPipes[2]);

        $exitCode = proc_close($process);

        if ($exitCode !== 0) {
            throw new \RuntimeException("Go write failed: $stderr");
        }
    }
    
    /**
     * Enable or disable debug mode.
     * When enabled, prints the command execution string.
     */
    public bool $debug = false;

    /**
     * Alter CSV Schema (Add Column).
     *
     * @param string $csvPath Path to CSV
     * @param string $columnName New column name
     * @param string $defaultValue Default value
     * @param string $separator CSV separator
     * @return void
     * @throws \RuntimeException If alter fails
     */
    public function alter(
        string $csvPath,
        string $columnName,
        string $defaultValue,
        string $separator = ',',
        bool $materialize = false
    ): void {
        $args = [
            'alter',
            '--csv', $csvPath,
            '--add-column', $columnName,
            '--default', $defaultValue,
            '--separator', $separator
        ];

        if ($materialize) {
            $args[] = '--materialize';
        }

        $command = $this->buildCommand($args);
        
        // Execute blocking
        exec($command . ' 2>&1', $output, $exitCode);

        if ($exitCode !== 0) {
             throw new \RuntimeException("Schema Alteration Failed: " . implode("\n", $output));
        }
    }

    public function update(string $csvPath, string $setClause, ?string $whereJson = null, string $indexDir = '') : int
    {
        $args = [
            'update',
            '--csv', $csvPath,
            '--set', $setClause,
        ];
        if ($whereJson) {
            $args[] = '--where';
            $args[] = $whereJson;
        }
        if ($indexDir) {
            $args[] = '--index-dir';
            $args[] = $indexDir;
        }

        $command = $this->buildCommand($args);
        exec($command . ' 2>&1', $output, $exitCode);

        if ($exitCode !== 0) {
            throw new \RuntimeException("Update Failed: " . implode("\n", $output));
        }

        // Capture the count from output (ignoring metrics from stderr)
        foreach (array_reverse($output) as $line) {
            $line = trim($line);
            if (is_numeric($line)) {
                return (int)$line;
            }
        }

        return 0;
    }

    /**
     * Get version information.
     *
     * @return string
     */
    public function getVersion(): string
    {
        $descriptors = [
            0 => ['pipe', 'r'],
            1 => ['pipe', 'w'],
            2 => ['pipe', 'w'],
        ];

        $executionString = escapeshellcmd($this->binaryPath) . ' version';
        $process = proc_open($executionString, $descriptors, $processPipes);

        if (!is_resource($process)) {
            return 'unknown';
        }

        fclose($processPipes[0]);
        $output = stream_get_contents($processPipes[1]);
        fclose($processPipes[1]);
        fclose($processPipes[2]);
        proc_close($process);

        return trim($output);
    }
    /**
     * Helper to validate Go binary execution.
     * Throws specific exceptions for known errors.
     */
    private function validateExecution(int $exitCode, string $stderr): void
    {
        if ($exitCode === 0) {
            return;
        }

        // Check for Column Not Found error
        // Matches: column 'XYZ' not found
        if (preg_match("/column '(.+?)' not found/i", $stderr, $matches)) {
            $col = $matches[1];
            // Clean up the error message to be very readable
            throw new \InvalidArgumentException("Evaluation Error: Column '$col' does not exist in the CSV headers. Please check the spelling or CSV file integrity.\nDetails: $stderr");
        }

        throw new \RuntimeException("CsvQuery failed: $stderr");
    }
}

