#!/usr/bin/env php
<?php
/**
 * Entreya CsvQuery - Cross-Platform Build Script
 * 
 * Automatically detects OS/Architecture and compiles the Go binary.
 * 
 * Usage:
 *   php scripts/build.php           # Build for current platform
 *   php scripts/build.php --all     # Build for all platforms
 *   php scripts/build.php --clean   # Remove all binaries
 */

declare(strict_types=1);

namespace Entreya\CsvQuery\Build;

class Builder
{
    private const GO_SRC_DIR = __DIR__ . '/../src/go';
    private const BIN_DIR = __DIR__ . '/../bin';
    
    private const PLATFORMS = [
        ['os' => 'darwin',  'arch' => 'arm64', 'ext' => ''],
        ['os' => 'darwin',  'arch' => 'amd64', 'ext' => ''],
        ['os' => 'linux',   'arch' => 'amd64', 'ext' => ''],
        ['os' => 'linux',   'arch' => 'arm64', 'ext' => ''],
        ['os' => 'windows', 'arch' => 'amd64', 'ext' => '.exe'],
    ];

    private bool $verbose;

    public function __construct(bool $verbose = true)
    {
        $this->verbose = $verbose;
    }

    /**
     * Detect current OS.
     */
    public function detectOS(): string
    {
        return match (PHP_OS_FAMILY) {
            'Darwin' => 'darwin',
            'Windows' => 'windows',
            default => 'linux',
        };
    }

    /**
     * Detect current architecture.
     */
    public function detectArch(): string
    {
        $machine = php_uname('m');
        return match ($machine) {
            'arm64', 'aarch64' => 'arm64',
            default => 'amd64',
        };
    }

    /**
     * Check if Go is installed.
     */
    public function checkGoInstalled(): bool
    {
        $output = [];
        $returnCode = 0;
        exec('go version 2>&1', $output, $returnCode);
        return $returnCode === 0;
    }

    /**
     * Get Go version.
     */
    public function getGoVersion(): ?string
    {
        $output = [];
        exec('go version 2>&1', $output);
        return $output[0] ?? null;
    }

    /**
     * Build binary for specific platform.
     */
    public function build(string $os, string $arch): bool
    {
        $ext = $os === 'windows' ? '.exe' : '';
        $binaryName = "csvquery_{$os}_{$arch}{$ext}";
        $outputPath = self::BIN_DIR . "/{$binaryName}";

        $this->log("Building {$binaryName}...");

        // Ensure bin directory exists
        if (!is_dir(self::BIN_DIR)) {
            mkdir(self::BIN_DIR, 0755, true);
        }

        // Build command
        $env = sprintf('GOOS=%s GOARCH=%s CGO_ENABLED=0', $os, $arch);
        $cmd = sprintf(
            'cd %s && %s go build -ldflags="-s -w" -o %s .',
            escapeshellarg(self::GO_SRC_DIR),
            $env,
            escapeshellarg($outputPath)
        );

        // Windows doesn't support env vars the same way
        if (PHP_OS_FAMILY === 'Windows') {
            $cmd = sprintf(
                'cd /d %s && set GOOS=%s && set GOARCH=%s && set CGO_ENABLED=0 && go build -ldflags="-s -w" -o %s .',
                escapeshellarg(self::GO_SRC_DIR),
                $os,
                $arch,
                escapeshellarg($outputPath)
            );
        }

        $output = [];
        $returnCode = 0;
        exec($cmd . ' 2>&1', $output, $returnCode);

        if ($returnCode !== 0) {
            $this->log("  ✗ Failed: " . implode("\n", $output), 'error');
            return false;
        }

        $size = filesize($outputPath);
        $sizeFormatted = $this->formatBytes($size);
        $this->log("  ✓ Built {$binaryName} ({$sizeFormatted})");
        
        return true;
    }

    /**
     * Build for current platform only.
     */
    public function buildCurrent(): bool
    {
        return $this->build($this->detectOS(), $this->detectArch());
    }

    /**
     * Build for all platforms.
     */
    public function buildAll(): array
    {
        $results = [];
        foreach (self::PLATFORMS as $platform) {
            $key = "{$platform['os']}/{$platform['arch']}";
            $results[$key] = $this->build($platform['os'], $platform['arch']);
        }
        return $results;
    }

    /**
     * Clean all binaries.
     */
    public function clean(): void
    {
        $this->log("Cleaning binaries...");
        $files = glob(self::BIN_DIR . '/csvquery_*');
        foreach ($files as $file) {
            unlink($file);
            $this->log("  Deleted: " . basename($file));
        }
        $this->log("  ✓ Clean complete");
    }

    /**
     * Format bytes to human readable.
     */
    private function formatBytes(int $bytes): string
    {
        $units = ['B', 'KB', 'MB', 'GB'];
        $i = 0;
        while ($bytes >= 1024 && $i < count($units) - 1) {
            $bytes /= 1024;
            $i++;
        }
        return round($bytes, 2) . ' ' . $units[$i];
    }

    /**
     * Log message.
     */
    private function log(string $message, string $level = 'info'): void
    {
        if (!$this->verbose) return;
        
        $prefix = match ($level) {
            'error' => "\033[31m",
            'success' => "\033[32m",
            default => "",
        };
        $reset = $level !== 'info' ? "\033[0m" : "";
        
        echo $prefix . $message . $reset . PHP_EOL;
    }
}

// ============================================================================
// CLI Entry Point
// ============================================================================

if (php_sapi_name() !== 'cli') {
    die("This script must be run from the command line.\n");
}

$builder = new Builder();

// Header
echo "\n";
echo "╔══════════════════════════════════════════════════════╗\n";
echo "║  Entreya CsvQuery - Build Script                     ║\n";
echo "╚══════════════════════════════════════════════════════╝\n\n";

// Check Go installation
if (!$builder->checkGoInstalled()) {
    echo "\033[31m✗ Error: Go is not installed or not in PATH.\033[0m\n\n";
    echo "Please install Go from: https://go.dev/dl/\n";
    echo "After installation, ensure 'go' is in your PATH.\n\n";
    exit(1);
}

echo "Go version: " . $builder->getGoVersion() . "\n";
echo "Host OS: " . $builder->detectOS() . "\n";
echo "Host Arch: " . $builder->detectArch() . "\n\n";

// Parse arguments
$args = array_slice($argv, 1);
$buildAll = in_array('--all', $args);
$clean = in_array('--clean', $args);

if ($clean) {
    $builder->clean();
    exit(0);
}

if ($buildAll) {
    echo "Building for all platforms...\n\n";
    $results = $builder->buildAll();
    
    $success = array_filter($results);
    $failed = array_diff_key($results, $success);
    
    echo "\n";
    echo "Summary: " . count($success) . "/" . count($results) . " platforms built successfully.\n";
    
    if (!empty($failed)) {
        echo "Failed: " . implode(', ', array_keys($failed)) . "\n";
        exit(1);
    }
} else {
    echo "Building for current platform...\n\n";
    if (!$builder->buildCurrent()) {
        exit(1);
    }
}

echo "\n✓ Build complete!\n\n";
exit(0);
