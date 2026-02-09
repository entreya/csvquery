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
 * @package CsvQuery\Bridge
 */

declare(strict_types=1);

namespace Entreya\CsvQuery\Bridge;

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
        // From src/php/Bridge/ go up 3 levels to reach project root/bin
        $binDir = dirname(__DIR__, 3) . '/bin';

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
            '--input', $csvPath,
            '--output', $outputDir,
            '--columns', $columnsJson,
            '--separator', $separator,
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

        // Try socket first if enabled
        if ($this->useSocket) {
            try {
                $client = SocketClient::getInstance();
                
                // If Grouping or Explaining, we expect a JSON response
                if ($explain || !empty($groupBy) || !empty($aggFunc)) {
                    $params = [
                        'csv' => $csvPath,
                        'where' => $where,
                        'limit' => $limit,
                        'offset' => $offset,
                        'groupBy' => $groupBy,
                        'aggCol' => $aggCol,
                        'aggFunc' => $aggFunc,
                    ];
                    if ($explain) {
                        $params['explain'] = true;
                    }
                    $response = $client->query('query', $params);
                    return $response['data'] ?? [];
                }

                // Standard select
                $rows = $client->select($csvPath, $where, $limit, $offset);
                return $this->arrayToGenerator($rows);
            } catch (\Exception $e) {
                // Fallback to spawn
            }
        }

        // If Grouping or Explaining, we expect a JSON response, not a stream
        if ($explain || !empty($groupBy) || !empty($aggFunc)) {
            // Re-construct for exec() fallback if needed, BUT prefer proc_open for consistency
            // However, exec() returns output array easily. 
            // Let's implement captureOutput using array execute for consistency.
            
            $json = $this->executeCapture($args);
            if (empty($json)) {
                 return [];
            }
            return json_decode($json, true) ?? [];
        }

        return $this->streamOutput($args);
    }

    /**
     * Convert an array of rows to a Generator for API compatibility.
     */
    private function arrayToGenerator(array $rows): \Generator
    {
        foreach ($rows as $row) {
            yield $row;
        }
    }

    private function queryCli(array $args): \Generator 
    {
        return $this->streamOutput($args);
    }

    /* Old streamOutput logic kept for CLI fallback */
    private function streamOutput(array $args): \Generator
    {
        $command = array_merge([$this->binaryPath], $args);

         if ($this->debug) {
            echo "[DEBUG] Executing: " . implode(" ", array_map('escapeshellarg', $command)) . "\n";
        }

        $descriptors = [
            0 => ['pipe', 'r'],
            1 => ['pipe', 'w'], // stdout
            2 => ['pipe', 'w'], // stderr
        ];

        // PHP 7.4+ supports array for command to bypass shell
        $process = proc_open($command, $descriptors, $processPipes);

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

        // Use executeCapture to get output securely
        $output = $this->executeCapture($args);
        
        // Output might contain debug logs, find the last number or clean numeric line
        // Actually executeCapture returns *only* stdout json if we parse correctly?
        // No, executeCapture (new method) should return raw stdout string?
        // Or I can parse lines.
        // Let's treat executeCapture as returning the full stdout as string.
        
        // Wait, I need to define executeCapture first.
        
        return (int) trim($output);
    }



    /**
     * Execute the Go binary.
     *
     * @param array $args Command line arguments
     * @param bool $passthrough Pass output directly to stdout
     * @return bool Success status
     */
    /**
     * Execute the Go binary.
     */
    private function execute(array $args, bool $passthrough = false): bool
    {
        $command = array_merge([$this->binaryPath], $args);

        $descriptors = [
            0 => ['pipe', 'r'],  // stdin
            1 => ['pipe', 'w'],  // stdout
            2 => ['pipe', 'w'],  // stderr
        ];

        // Windows-specific options for proc_open mostly relate to bypassing shell,
        // which array-args argument does automatically in newer PHP versions.
        $process = proc_open($command, $descriptors, $processPipes);

        if (!is_resource($process)) {
            throw new \RuntimeException("Failed to start csvquery process");
        }

        // Close stdin immediately as we don't write to it
        fclose($processPipes[0]);

        // Set streams to non-blocking mode for manuals polling loop
        stream_set_blocking($processPipes[1], false);
        stream_set_blocking($processPipes[2], false);

        $stdout = '';
        $stderr = '';
        $isWindows = PHP_OS_FAMILY === 'Windows';
        $exitCodeFromStatus = null;

        while (true) {
            $read = [];
            if (!feof($processPipes[1])) $read[] = $processPipes[1];
            if (!feof($processPipes[2])) $read[] = $processPipes[2];

            if (empty($read)) {
                if ($exitCodeFromStatus === null) {
                    $status = proc_get_status($process);
                    if (!$status['running']) {
                        $exitCodeFromStatus = $status['exitcode'];
                    }
                }
                break;
            }

            $ready = false;

            if ($isWindows) {
                // Windows: stream_select() works ONLY on sockets, not process file handles.
                // Fallback: poll with adaptive sleep
                $ready = true;
            } else {
                // Linux/Mac: Use stream_select for efficiency
                $write = null;
                $except = null;
                // Wait up to 200ms
                $result = stream_select($read, $write, $except, 0, 200000);
                if ($result === false) break;
                $ready = ($result > 0);
            }

            $gotData = false;

            if ($ready) {
                foreach ([$processPipes[1], $processPipes[2]] as $pipe) {
                    // Read chunk (non-blocking) - 64KB buffer
                    $data = fread($pipe, 65536);
                    
                    if ($data !== false && $data !== '') {
                        $gotData = true;
                        if ($pipe === $processPipes[1]) {
                            $stdout .= $data;
                            if ($passthrough) echo $data;
                        } else {
                            $stderr .= $data;
                            if ($passthrough) {
                                fwrite(STDERR, $data);
                            }
                        }
                    }
                }
                
                // If we didn't get data but streams are supposedly open, check if process died
                if (!$gotData && !$isWindows) {
                    // On Linux/Mac, stream_select returned > 0 but fread got nothing? 
                    // Usually means EOF or error, but let's check status just in case.
                    $status = proc_get_status($process);
                    if (!$status['running']) {
                        $exitCodeFromStatus = $status['exitcode'];
                        // Gather any remaining bytes
                        foreach ([$processPipes[1], $processPipes[2]] as $pipe) {
                             while (($data = fread($pipe, 8192)) !== false && $data !== '') {
                                 if ($pipe === $processPipes[1]) {
                                     $stdout .= $data;
                                     if ($passthrough) echo $data;
                                 } else {
                                     $stderr .= $data;
                                     if ($passthrough) fwrite(STDERR, $data);
                                 }
                             }
                        }
                        break;
                    }
                }
            }

            // Windows-specific handling:
            // If data was received, loop immediately (no sleep) to drain buffer.
            // If NO data received, check process status and sleep.
            if ($isWindows) {
                if (!$gotData) {
                    $status = proc_get_status($process);
                    if (!$status['running']) {
                        $exitCodeFromStatus = $status['exitcode'];
                        // Consume remaining
                         foreach ([$processPipes[1], $processPipes[2]] as $pipe) {
                             while (($data = fread($pipe, 8192)) !== false && $data !== '') {
                                 if ($pipe === $processPipes[1]) {
                                     $stdout .= $data;
                                     if ($passthrough) echo $data;
                                 } else {
                                     $stderr .= $data;
                                     if ($passthrough) fwrite(STDERR, $data);
                                 }
                             }
                        }
                        break;
                    }
                    // Process still running but no data -> Sleep briefly
                    usleep(5000); // 5ms sleep
                }
            }
        }


        $this->lastStderr = $stderr;

        fclose($processPipes[1]);
        fclose($processPipes[2]);

        $exitCode = proc_close($process);

        // If proc_close returned -1 (error/unknown) but we captured the exit code earlier, use it.
        if ($exitCode === -1 && $exitCodeFromStatus !== null) {
            $exitCode = $exitCodeFromStatus;
        }

        if ($exitCode !== 0) {
            throw new \RuntimeException(
                "csvquery failed with code $exitCode: $stderr"
            );
        }

        return true;
    }

    /**
     * Execute and capture output (replacement for exec).
     */
    private function executeCapture(array $args): string
    {
        $command = array_merge([$this->binaryPath], $args);

        if ($this->debug) {
            echo "[DEBUG] Executing: " . implode(" ", array_map('escapeshellarg', $command)) . "\n";
        }

        $descriptors = [
            0 => ['pipe', 'r'],
            1 => ['pipe', 'w'],
            2 => ['pipe', 'w'],
        ];

        $process = proc_open($command, $descriptors, $processPipes);
        if (!is_resource($process)) {
            throw new \RuntimeException("Failed to launch process");
        }

        fclose($processPipes[0]);

        $stdout = stream_get_contents($processPipes[1]);
        $stderr = stream_get_contents($processPipes[2]);
        
        fclose($processPipes[1]);
        fclose($processPipes[2]);

        $exitCode = proc_close($process);

        if ($exitCode !== 0) {
            // Check for aggregation json on stdout even if exit code is non-zero?
            // No, strictly fail if exit code is non-zero
            $this->validateExecution($exitCode, $stderr);
            // Ideally we throw:
            throw new \RuntimeException("Execution failed: $stderr");
        }
        
        // Find JSON in stdout if mixed with other output (optional logic kept from before)
        // But for countViaSpawn we just want stdout.
        
        return trim($stdout);
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
    /**
     * Build command string. (Deprecated - do not use for execution)
     * Kept only if needed for debug logging, but better to log array.
     */
    private function buildCommand(array $args): string
    {
        throw new \BadMethodCallException("buildCommand is deprecated. Use direct execution with array.");
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

        // Execute via execute() (passthrough false)
        // execute() implementation above handles array args
        $this->execute($args);
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

        $this->execute($args);
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

        $output = $this->executeCapture($args);
        
        // Parse output for count (last line typically)
        $lines = explode("\n", trim($output));
        foreach (array_reverse($lines) as $line) {
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
        return $this->executeCapture(['version']);
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

