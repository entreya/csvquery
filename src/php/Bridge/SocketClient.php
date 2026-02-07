<?php

declare(strict_types=1);

/**
 * SocketClient - Persistent Unix socket client for CsvQuery.
 * 
 * Provides fast communication with the Go daemon via Unix Domain Socket.
 * Auto-starts the daemon if not running.
 */

namespace Entreya\CsvQuery\Bridge;

class SocketClient
{
    /** @var string Default socket path */
    private const DEFAULT_SOCKET = '/tmp/csvquery.sock';
    
    /** @var int Startup timeout in milliseconds */
    private const STARTUP_TIMEOUT_MS = 2000;
    
    /** @var int Query timeout in seconds */
    private const QUERY_TIMEOUT_SEC = 30;
    
    /** @var SocketClient|null Singleton instance */
    private static ?SocketClient $instance = null;
    
    /** @var resource|null Socket connection */
    private $socket = null;
    
    /** @var string Socket path */
    private string $socketPath;
    
    /** @var string Path to Go binary */
    private string $binaryPath;
    
    /** @var string Index directory */
    private string $indexDir;
    
    /** @var bool Debug mode */
    public bool $debug = false;

    /**
     * Private constructor for singleton.
     */
    private function __construct(string $binaryPath, string $indexDir = '', string $socketPath = '')
    {
        $this->binaryPath = $binaryPath;
        $this->indexDir = $indexDir;
        $this->socketPath = $socketPath ?: (getenv('CSVQUERY_SOCKET') ?: self::DEFAULT_SOCKET);
    }

    /**
     * Get or create singleton instance.
     */
    public static function getInstance(string $binaryPath = '', string $indexDir = ''): self
    {
        if (self::$instance === null) {
            if ($binaryPath === '') {
                throw new \RuntimeException('SocketClient requires binaryPath on first call');
            }
            self::$instance = new self($binaryPath, $indexDir);
        }
        return self::$instance;
    }

    /**
     * Configure the singleton instance.
     */
    public static function configure(string $binaryPath, string $indexDir = '', string $socketPath = ''): void
    {
        self::$instance = new self($binaryPath, $indexDir, $socketPath);
    }

    /**
     * Check if daemon is available (socket exists).
     */
    public static function isAvailable(): bool
    {
        $socketPath = getenv('CSVQUERY_SOCKET') ?: self::DEFAULT_SOCKET;
        return file_exists($socketPath);
    }

    /**
     * Reset the singleton (for testing).
     */
    public static function reset(): void
    {
        if (self::$instance !== null && self::$instance->socket !== null) {
            @fclose(self::$instance->socket);
        }
        self::$instance = null;
    }

    /**
     * Execute a query against the daemon.
     * 
     * @param string $action Action type: count, select, groupby, ping, status
     * @param array $params Query parameters
     * @return array Response data
     * @throws \RuntimeException On communication error
     */
    public function query(string $action, array $params = []): array
    {
        $this->ensureConnected();

        $request = array_merge(['action' => $action], $params);
        $json = json_encode($request);

        if ($this->debug) {
            echo "[SocketClient] Request: $json\n";
        }

        // Send request
        $written = @fwrite($this->socket, $json . "\n");
        if ($written === false) {
            // Connection broken, try reconnect once
            $this->reconnect();
            $written = @fwrite($this->socket, $json . "\n");
            if ($written === false) {
                throw new \RuntimeException('Failed to write to socket after reconnect');
            }
        }
        fflush($this->socket);

        // Read response
        stream_set_timeout($this->socket, self::QUERY_TIMEOUT_SEC);
        $response = @fgets($this->socket);

        if ($response === false) {
            // Check if it's a timeout or broken pipe
            $info = stream_get_meta_data($this->socket);
            if ($info['timed_out']) {
                throw new \RuntimeException('Query timeout');
            }
            // Connection broken, try reconnect once
            $this->reconnect();
            throw new \RuntimeException('Connection lost, please retry');
        }

        if ($this->debug) {
            echo "[SocketClient] Response: $response\n";
        }

        $data = json_decode(trim($response), true);
        if ($data === null) {
            throw new \RuntimeException('Invalid JSON response: ' . $response);
        }

        if (!empty($data['error'])) {
            throw new \RuntimeException('Daemon error: ' . $data['error']);
        }

        return $data;
    }

    /**
     * Count matching rows.
     */
    public function count(string $csvPath, array $where = []): int
    {
        $result = $this->query('count', [
            'csv' => $csvPath,
            'where' => $where,
        ]);
        return (int)($result['count'] ?? 0);
    }

    /**
     * Select matching rows (returns offsets).
     */
    public function select(string $csvPath, array $where = [], int $limit = 0, int $offset = 0): array
    {
        $result = $this->query('select', [
            'csv' => $csvPath,
            'where' => $where,
            'limit' => $limit,
            'offset' => $offset,
        ]);
        return $result['rows'] ?? [];
    }

    /**
     * Group by with aggregation.
     */
    public function groupBy(string $csvPath, string $column, string $aggFunc = 'count', array $where = []): array
    {
        $result = $this->query('groupby', [
            'csv' => $csvPath,
            'groupBy' => $column,
            'aggFunc' => $aggFunc,
            'where' => $where,
        ]);
        return $result['groups'] ?? [];
    }

    /**
     * Ping the daemon.
     */
    public function ping(): bool
    {
        try {
            $result = $this->query('ping');
            return !empty($result['pong']);
        } catch (\Exception $e) {
            return false;
        }
    }

    /**
     * Get daemon status.
     */
    public function status(): array
    {
        return $this->query('status');
    }

    /**
     * Ensure socket is connected, auto-start daemon if needed.
     */
    private function ensureConnected(): void
    {
        if ($this->socket !== null && is_resource($this->socket)) {
            // Check if socket is still valid
            if (!@feof($this->socket)) {
                return;
            }
            @fclose($this->socket);
            $this->socket = null;
        }

        // Check if socket file exists
        if (!file_exists($this->socketPath)) {
            $this->startDaemon();
        }

        $this->connect();
    }

    /**
     * Connect to the socket.
     */
    private function connect(): void
    {
        $this->socket = @stream_socket_client(
            'unix://' . $this->socketPath,
            $errno,
            $errstr,
            5.0
        );

        if ($this->socket === false) {
            throw new \RuntimeException("Failed to connect to socket: [$errno] $errstr");
        }

        stream_set_blocking($this->socket, true);
    }

    /**
     * Reconnect to the socket.
     */
    private function reconnect(): void
    {
        if ($this->socket !== null) {
            @fclose($this->socket);
            $this->socket = null;
        }

        // Wait a bit for daemon to recover if it crashed
        usleep(100000); // 100ms

        if (!file_exists($this->socketPath)) {
            $this->startDaemon();
        }

        $this->connect();
    }

    /**
     * Start the Go daemon in the background.
     */
    private function startDaemon(): void
    {
        if (!file_exists($this->binaryPath)) {
            throw new \RuntimeException("Go binary not found: {$this->binaryPath}");
        }

        // Build command
        $cmd = escapeshellarg($this->binaryPath) . ' daemon';
        $cmd .= ' --socket ' . escapeshellarg($this->socketPath);
        if ($this->indexDir !== '') {
            $cmd .= ' --index-dir ' . escapeshellarg($this->indexDir);
        }

        // Start in background, redirect output to debug log (absolute path next to script)
        $logPath = dirname(__DIR__) . '/daemon.log';
        $cmd .= ' > ' . escapeshellarg($logPath) . ' 2>&1 &';

        if ($this->debug) {
            echo "[SocketClient] Starting daemon: $cmd\n";
        }

        // Execute
        exec($cmd);

        // Wait for socket to appear
        $startTime = microtime(true) * 1000;
        $timeout = self::STARTUP_TIMEOUT_MS;

        while (!file_exists($this->socketPath)) {
            $elapsed = (microtime(true) * 1000) - $startTime;
            if ($elapsed > $timeout) {
                throw new \RuntimeException(
                    "Daemon failed to start within {$timeout}ms. " .
                    "Socket not found: {$this->socketPath}"
                );
            }
            usleep(50000); // 50ms
        }

        // Additional small delay to ensure it's ready
        usleep(100000); // 100ms
    }

    /**
     * Close the connection on destruction.
     */
    public function __destruct()
    {
        if ($this->socket !== null) {
            @fclose($this->socket);
        }
    }
}
