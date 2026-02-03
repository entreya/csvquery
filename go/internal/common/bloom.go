// Package main - Bloom Filter for CsvQuery
//
// BloomFilter provides fast negative lookups with configurable false positive rate.
// It answers: "Is this key DEFINITELY NOT in the set?" with 100% accuracy.
//
// Memory usage (for 1% false positive rate):
//   - 10 million keys: ~12.5 MB
//   - 100 million keys: ~125 MB
//   - 1 billion keys: ~1.25 GB
//
// This is much smaller than a hash table (which would need ~80 GB for 1B keys).
//
// The algorithm uses double hashing with CRC32 for compatibility with PHP.
package common

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
)

// BloomFilter implements a space-efficient probabilistic set
type BloomFilter struct {
	bits      []byte // Bit array
	size      int    // Size in bits
	hashCount int    // Number of hash functions
	count     int    // Number of elements added
}

// NewBloomFilter creates a bloom filter optimized for expected elements and FP rate
//
// Parameters:
//   - n: Expected number of elements
//   - fpRate: Desired false positive rate (0.01 = 1%)
//
// The optimal parameters are calculated using:
//   - m (bits) = -n * ln(p) / (ln(2)^2)
//   - k (hashes) = (m/n) * ln(2)
func NewBloomFilter(n int, fpRate float64) *BloomFilter {
	if n < 1 {
		n = 1
	}
	if fpRate <= 0 {
		fpRate = 0.01
	}

	// Calculate optimal size: m = -n * ln(p) / (ln(2)^2)
	// ln(2)^2 ≈ 0.4804
	// For 1% FP rate: m ≈ 9.6n bits
	m := int(-float64(n) * ln(fpRate) / 0.4804)
	if m < 1024 {
		m = 1024
	}
	m = ((m + 7) / 8) * 8 // Round to bytes

	// Calculate optimal hash count: k = (m/n) * ln(2)
	// ln(2) ≈ 0.693
	k := int(float64(m) / float64(n) * 0.693)
	if k < 1 {
		k = 1
	}
	if k > 10 {
		k = 10 // Cap at 10 hashes for performance
	}

	return &BloomFilter{
		bits:      make([]byte, m/8),
		size:      m,
		hashCount: k,
		count:     0,
	}
}

// ln returns natural logarithm (approximation sufficient for bloom filter)
func ln(x float64) float64 {
	// Use log approximation: ln(x) = 2.302585 * log10(x)
	// For our use case, we can use a simpler calculation
	if x == 0.01 {
		return -4.605 // ln(0.01)
	}
	if x == 0.001 {
		return -6.907 // ln(0.001)
	}
	// General approximation
	result := 0.0
	for x > 1 {
		x /= 2.718
		result += 1
	}
	return result + (x - 1)
}

// Add inserts a key into the filter
func (bf *BloomFilter) Add(key string) {
	// Inline getPositions logic to avoid allocs
	// First hash: CRC32 of key
	keyBytes := []byte(key)
	h1 := crc32.ChecksumIEEE(keyBytes)

	// Second hash: CRC32 of reversed key + salt
	var buf [256]byte
	reversed := appendReversed(buf[:0], keyBytes)
	reversed = append(reversed, "salt"...)
	h2 := crc32.ChecksumIEEE(reversed)

	for i := 0; i < bf.hashCount; i++ {
		combined := int(h1) + i*int(h2)
		if combined < 0 {
			combined = -combined
		}
		pos := combined % bf.size
		byteIdx := pos / 8
		bitIdx := pos % 8
		bf.bits[byteIdx] |= (1 << bitIdx)
	}
	bf.count++
}

// MightContain checks if a key might be in the set
//
// Returns:
//   - false: Key is DEFINITELY NOT in the set (100% accurate)
//   - true: Key MIGHT be in the set (with configured false positive rate)
func (bf *BloomFilter) MightContain(key string) bool {
	// Inline getPositions logic
	keyBytes := []byte(key)
	h1 := crc32.ChecksumIEEE(keyBytes)

	var buf [256]byte
	reversed := appendReversed(buf[:0], keyBytes)
	reversed = append(reversed, "salt"...)
	h2 := crc32.ChecksumIEEE(reversed)

	for i := 0; i < bf.hashCount; i++ {
		combined := int(h1) + i*int(h2)
		if combined < 0 {
			combined = -combined
		}
		pos := combined % bf.size
		byteIdx := pos / 8
		bitIdx := pos % 8
		if (bf.bits[byteIdx] & (1 << bitIdx)) == 0 {
			return false // Definitely not in set
		}
	}
	return true // Possibly in set
}

// appendReversed works on []byte to avoid []rune alloc for ASCII keys
func appendReversed(dst []byte, s []byte) []byte {
	start := len(dst)
	dst = append(dst, s...)
	// Reverse the appended part
	for i, j := start, len(dst)-1; i < j; i, j = i+1, j-1 {
		dst[i], dst[j] = dst[j], dst[i]
	}
	return dst
}

// Serialize converts the bloom filter to bytes for storage
//
// Binary format (24 byte header + bits):
//   - Bytes 0-7: size (int64)
//   - Bytes 8-15: hashCount (int64)
//   - Bytes 16-23: count (int64)
//   - Bytes 24+: bit array
func (bf *BloomFilter) Serialize() []byte {
	header := make([]byte, 24)
	binary.BigEndian.PutUint64(header[0:8], uint64(bf.size))
	binary.BigEndian.PutUint64(header[8:16], uint64(bf.hashCount))
	binary.BigEndian.PutUint64(header[16:24], uint64(bf.count))

	return append(header, bf.bits...)
}

// Deserialize creates a bloom filter from serialized bytes
func DeserializeBloom(data []byte) *BloomFilter {
	if len(data) < 24 {
		return nil
	}

	size := int(binary.BigEndian.Uint64(data[0:8]))
	hashCount := int(binary.BigEndian.Uint64(data[8:16]))
	count := int(binary.BigEndian.Uint64(data[16:24]))

	return &BloomFilter{
		bits:      data[24:],
		size:      size,
		hashCount: hashCount,
		count:     count,
	}
}

// GetStats returns bloom filter statistics
func (bf *BloomFilter) GetStats() (size, hashCount, count int) {
	return bf.size, bf.hashCount, bf.count
}

// GetMemoryUsage returns memory usage in bytes
func (bf *BloomFilter) GetMemoryUsage() int {
	return len(bf.bits) + 24 // bits + header
}

// LoadBloomFilter reads a bloom filter from a file
func LoadBloomFilter(path string) (*BloomFilter, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	bloom := DeserializeBloom(data)
	if bloom == nil {
		return nil, fmt.Errorf("invalid bloom filter data")
	}
	return bloom, nil
}

// LoadBloomFilterMmap loads the bloom filter using mmap for zero-copy access
func LoadBloomFilterMmap(path string) (*BloomFilter, func(), error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}

	// Mmap the file
	data, err := MmapFile(f)
	if err != nil {
		f.Close()
		return nil, nil, err
	}
	// We can close the file descriptor immediately after mmap
	f.Close()

	bloom := DeserializeBloom(data)
	if bloom == nil {
		MunmapFile(data)
		return nil, nil, fmt.Errorf("invalid bloom filter data")
	}

	cleanup := func() {
		MunmapFile(data)
	}

	return bloom, cleanup, nil
}
