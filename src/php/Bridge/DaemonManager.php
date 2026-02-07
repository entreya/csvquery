<?php

declare(strict_types=1);

/**
 * DaemonManager - Manages the lifecycle of the CsvQuery Go daemon.
 *
 * Handles:
 * - Binary detection
 * - Process startup/shutdown
 * - PID tracking
 * - Cross-platform support (Unix Sockets vs TCP)
 *
 * @package Entreya\CsvQuery\Bridge
 */

namespace Entreya\CsvQuery\Bridge;

class DaemonManager
{
    private string $binaryPath;
    private ?int $pid = null;
    private ?string $socketUrl = null;
    private $process = null;
    private string $pidFile;

    public function __construct(?string $binaryPath = null)
    {
        $this->binaryPath = $binaryPath ?? self::detectBinary();
        $this->pidFile = sys_get_temp_dir() . '/csvquery_daemon.pid';
    }

    /**
     * Detect the correct binary for the current OS/architecture.
     */
    public static function detectBinary(): string
    {
        // Up 3 levels from src/php/Bridge to root
        $binDir = dirname(__DIR__, 3) . '/bin';

        $os = match (PHP_OS_FAMILY) {
            'Darwin' => 'darwin',
            'Windows' => 'windows',
            default => 'linux',
        };

        $arch = match (php_uname('m')) {
            'arm64', 'aarch64' => 'arm64',
            default => 'amd64',
        };

        $ext = $os === 'windows' ? '.exe' : '';
        $binary = "{$binDir}/csvquery_{$os}_{$arch}{$ext}";

        if (!file_exists($binary)) {
            // Fallback to generic name
            $default = "{$binDir}/csvquery{$ext}";
            if (file_exists($default)) {
                return $default;
            }
            throw new \RuntimeException("CsvQuery binary not found: $binary");
        }

        return $binary;
    }

    /**
     * Start the daemon.
     *
     * @param array $options Configuration options (workers, etc.)
     * @return string The socket URL (unix://... or tcp://...)
     */
    public function start(array $options = []): string
    {
        if ($this->isRunning()) {
            return $this->socketUrl;
        }

        $cmd = [escapeshellarg($this->binaryPath), 'daemon'];

        // Determine mode based on OS
        $isWindows = PHP_OS_FAMILY === 'Windows';
        
        if ($isWindows) {
            // Windows: Use TCP with random port
            $cmd[] = '--port 0'; 
            $cmd[] = '--host 127.0.0.1';
        } else {
            // Unix: Use Socket
            $socketPath = '/tmp/csvquery_' . uniqid() . '.sock';
            $cmd[] = '--socket ' . escapeshellarg($socketPath);
            $this->socketUrl = 'unix://' . $socketPath;
        }

        if (!empty($options['workers'])) {
            $cmd[] = '--workers ' . (int)$options['workers'];
        }

        if (!empty($options['indexDir'])) {
            $cmd[] = '--index-dir ' . escapeshellarg($options['indexDir']);
        }

        $commandLine = implode(' ', $cmd);
        
        // Start process
        $descriptors = [
            0 => ['pipe', 'r'], // stdin
            1 => ['pipe', 'w'], // stdout (to parse port if needed)
            2 => ['pipe', 'w'], // stderr
        ];

        $this->process = proc_open($commandLine, $descriptors, $pipes);

        if (!is_resource($this->process)) {
            throw new \RuntimeException("Failed to start daemon: $commandLine");
        }

        $status = proc_get_status($this->process);
        $this->pid = $status['pid'];
        
        // Save PID
        file_put_contents($this->pidFile, (string)$this->pid);

        // Wait for startup and parse output
        $startTime = microtime(true);
        $timeout = 2.0;
        $started = false;

        // Set non-blocking
        stream_set_blocking($pipes[1], false);
        stream_set_blocking($pipes[2], false);

        $outputBuffer = '';
        
        while (microtime(true) - $startTime < $timeout) {
            $out = stream_get_contents($pipes[1]);
            $err = stream_get_contents($pipes[2]);
            
            if ($out) {
                $outputBuffer .= $out;
                // Check for startup message
                // "CsvQuery Daemon started on tcp (127.0.0.1:54321)"
                // "CsvQuery Daemon started on unix (/tmp/...)"
                if (strpos($outputBuffer, 'CsvQuery Daemon started on') !== false) {
                    $started = true;
                    
                    // Parse port for Windows
                    if ($isWindows && preg_match('/started on tcp \((.+?)\)/', $outputBuffer, $matches)) {
                        $this->socketUrl = 'tcp://' . $matches[1];
                    }
                    break;
                }
            }
            
            if ($err) {
                // Log stderr if needed
            }

            usleep(50000); // 50ms
        }

        if (!$started) {
            $this->stop();
            throw new \RuntimeException("Daemon failed to start within timeout. Output: $outputBuffer");
        }
        
        // Save Address
        file_put_contents(sys_get_temp_dir() . '/csvquery_daemon.addr', $this->socketUrl);
        
        // Keep stdout/stderr open? No, we might block if buffer fails.
        // But we want to keep process running.
        // We can close pipes if we don't need them.
        fclose($pipes[0]);
        fclose($pipes[1]);
        fclose($pipes[2]);
        
        return $this->socketUrl;
    }

    /**
     * Stop the daemon.
     */
    public function stop(): void
    {
        if ($this->pid) {
            $isWindows = PHP_OS_FAMILY === 'Windows';
            
            if ($isWindows) {
                exec("taskkill /F /PID {$this->pid} 2>&1");
            } else {
                posix_kill($this->pid, SIGTERM);
                // Wait/Force kill?
            }
            
            $this->pid = null;
            if (file_exists($this->pidFile)) {
                unlink($this->pidFile);
            }
            
            $addrFile = sys_get_temp_dir() . '/csvquery_daemon.addr';
            if (file_exists($addrFile)) {
                @unlink($addrFile);
            }
        }

        if (is_resource($this->process)) {
            proc_close($this->process);
            $this->process = null;
        }
        
        // Cleanup socket file if unix
        if ($this->socketUrl && str_starts_with($this->socketUrl, 'unix://')) {
            $path = substr($this->socketUrl, 7);
            if (file_exists($path)) {
                @unlink($path);
            }
        }
    }

    /**
     * Check if daemon is running.
     */
    public function isRunning(): bool
    {
        if ($this->process && is_resource($this->process)) {
            $status = proc_get_status($this->process);
            return $status['running'];
        }
        
        // Check PID file as fallback?
        if (file_exists($this->pidFile)) {
            $pid = (int)file_get_contents($this->pidFile);
            if ($pid > 0) {
                 // Check if valid
                 if (PHP_OS_FAMILY === 'Windows') {
                     // exec tasklist? expensive.
                     // Assume running if we didn't start it? 
                     // No, DaemonManager manages *its* child.
                 } else {
                     return posix_kill($pid, 0);
                 }
            }
        }

        return false;
    }
    
    public function __destruct()
    {
        $this->stop();
    }
}
