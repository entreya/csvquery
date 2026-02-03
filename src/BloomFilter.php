<?php

/**
 * BloomFilter - Fast negative lookups for CsvQuery.
 *
 * A Bloom filter answers "Is this key DEFINITELY NOT in the set?"
 * with 100% accuracy. False positives are possible at the configured rate.
 *
 * Memory savings:
 * - 1 billion keys at 1% FP rate ≈ 1.25 GB (vs 80+ GB hash table)
 *
 * @package CsvQuery
 */

declare(strict_types=1);

namespace CsvQuery;

/**
 * Probabilistic set for fast negative lookups.
 */
class BloomFilter
{
    /** @var string Bit array as string */
    private string $bits;

    /** @var int Size in bits */
    private int $size;

    /** @var int Number of hash functions */
    private int $hashCount;

    /** @var int Number of elements */
    private int $count;

    /**
     * Create a new Bloom filter.
     *
     * @param int $expectedElements Expected number of elements
     * @param float $fpRate Desired false positive rate (0.01 = 1%)
     */
    public function __construct(int $expectedElements, float $fpRate = 0.01)
    {
        if ($expectedElements < 1) {
            $expectedElements = 1;
        }
        if ($fpRate <= 0 || $fpRate >= 1) {
            $fpRate = 0.01;
        }

        // Calculate optimal size: m = -n * ln(p) / (ln(2)^2)
        $m = (int) ceil(-$expectedElements * log($fpRate) / (log(2) ** 2));
        $m = max(1024, $m);
        $m = ((int) ceil($m / 8)) * 8; // Round to bytes

        // Calculate optimal hash count: k = (m/n) * ln(2)
        $k = (int) round(($m / $expectedElements) * log(2));
        $k = max(1, min(10, $k)); // Cap at 10 hashes

        $this->size = $m;
        $this->hashCount = $k;
        $this->count = 0;
        $this->bits = str_repeat("\0", (int) ceil($m / 8));
    }

    /**
     * Load from file.
     *
     * @param string $path Path to .bloom file
     * @return self|null
     */
    public static function loadFromFile(string $path): ?self
    {
        if (!file_exists($path)) {
            return null;
        }

        $data = file_get_contents($path);
        if (strlen($data) < 24) {
            return null;
        }

        // Read header (24 bytes)
        $size = self::unpackInt64(substr($data, 0, 8));
        $hashCount = self::unpackInt64(substr($data, 8, 8));
        $count = self::unpackInt64(substr($data, 16, 8));
        $bits = substr($data, 24);

        $bloom = new self(1); // Dummy constructor
        $bloom->size = $size;
        $bloom->hashCount = $hashCount;
        $bloom->count = $count;
        $bloom->bits = $bits;

        return $bloom;
    }

    /**
     * Add an element.
     *
     * @param string $key Element to add
     */
    public function add(string $key): void
    {
        foreach ($this->getPositions($key) as $pos) {
            $byteIdx = (int) floor($pos / 8);
            $bitIdx = $pos % 8;
            $this->bits[$byteIdx] = chr(ord($this->bits[$byteIdx]) | (1 << $bitIdx));
        }
        $this->count++;
    }

    /**
     * Check if an element might be in the set.
     *
     * @param string $key Element to check
     * @return bool false = definitely not in set, true = possibly in set
     */
    public function mightContain(string $key): bool
    {
        foreach ($this->getPositions($key) as $pos) {
            $byteIdx = (int) floor($pos / 8);
            $bitIdx = $pos % 8;
            if ((ord($this->bits[$byteIdx]) & (1 << $bitIdx)) === 0) {
                return false; // Definitely not in set
            }
        }
        return true; // Possibly in set
    }

    /**
     * Get hash positions for a key.
     * Uses double hashing compatible with Go implementation.
     *
     * @param string $key Key to hash
     * @return array<int> Bit positions
     */
    private function getPositions(string $key): array
    {
        // First hash: CRC32 of key
        $h1 = crc32($key);

        // Second hash: CRC32 of reversed key + salt
        $h2 = crc32(strrev($key) . 'salt');

        $positions = [];
        for ($i = 0; $i < $this->hashCount; $i++) {
            $combined = $h1 + $i * $h2;
            if ($combined < 0) {
                $combined = -$combined;
            }
            $positions[] = $combined % $this->size;
        }

        return $positions;
    }

    /**
     * Serialize to bytes.
     *
     * @return string Binary data
     */
    public function serialize(): string
    {
        $header = '';
        $header .= pack('J', $this->size);     // 8 bytes
        $header .= pack('J', $this->hashCount); // 8 bytes
        $header .= pack('J', $this->count);    // 8 bytes

        return $header . $this->bits;
    }

    /**
     * Save to file.
     *
     * @param string $path File path
     */
    public function saveToFile(string $path): void
    {
        file_put_contents($path, $this->serialize());
    }

    /**
     * Get statistics.
     *
     * @return array
     */
    public function getStats(): array
    {
        return [
            'size' => $this->size,
            'hashCount' => $this->hashCount,
            'count' => $this->count,
            'memoryBytes' => strlen($this->bits) + 24,
        ];
    }

    /**
     * Unpack big-endian 64-bit integer.
     *
     * @param string $bytes 8 bytes
     * @return int
     */
    private static function unpackInt64(string $bytes): int
    {
        $result = unpack('J', $bytes);
        return $result[1];
    }
}
