package common

import (
	"encoding/binary"
	"io"
	"time"
)

// RecordSize is the fixed size of each record in the index file
const RecordSize = 64 + 8 + 8 // Key(64) + Offset(8) + Line(8) = 80 bytes

// IndexRecord represents a single index entry
// optimized for zero-allocation and memory alignment
type IndexRecord struct {
	Key    [64]byte // Fixed 64-byte key (no pointer, no heap alloc)
	Offset int64    // Byte offset in CSV
	Line   int64    // Line number (1-based)
}

// IndexMeta holds metadata about indexes
type IndexMeta struct {
	CapturedAt time.Time             `json:"capturedAt"`
	TotalRows  int64                 `json:"totalRows"`
	CsvSize    int64                 `json:"csvSize"`
	CsvMtime   int64                 `json:"csvMtime"`
	CsvHash    string                `json:"csvHash"`
	Indexes    map[string]IndexStats `json:"indexes"`
}

type IndexStats struct {
	DistinctCount int64 `json:"distinctCount"`
	FileSize      int64 `json:"fileSize"`
}

// ReadRecord reads a single IndexRecord into the provided pointer
// Returns io.EOF if end of stream
func ReadRecord(reader io.Reader) (IndexRecord, error) {
	// We read 80 bytes at once: 64 (Key) + 8 (Offset) + 8 (Line)
	// Optimization: Stack allocated buffer
	var buf [RecordSize]byte
	if _, err := io.ReadFull(reader, buf[:]); err != nil {
		return IndexRecord{}, err
	}

	return IndexRecord{
		Key:    *(*[64]byte)(buf[0:64]),
		Offset: int64(binary.BigEndian.Uint64(buf[64:72])),
		Line:   int64(binary.BigEndian.Uint64(buf[72:80])),
	}, nil
}

// ReadBatchRecords reads count records into a slice
// Optimized for minimum system calls (one read for all records)
func ReadBatchRecords(r io.Reader, count int) ([]IndexRecord, error) {
	totalBytes := count * RecordSize
	buf := make([]byte, totalBytes)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}

	recs := make([]IndexRecord, count)
	for i := 0; i < count; i++ {
		offset := i * RecordSize
		recs[i] = IndexRecord{
			Key:    *(*[64]byte)(buf[offset : offset+64]),
			Offset: int64(binary.BigEndian.Uint64(buf[offset+64 : offset+72])),
			Line:   int64(binary.BigEndian.Uint64(buf[offset+72 : offset+80])),
		}
	}
	return recs, nil
}

// WriteRecord writes a single IndexRecord to a writer
func WriteRecord(w io.Writer, rec IndexRecord) error {
	var buf [RecordSize]byte

	// Copy Key (64 bytes)
	copy(buf[0:64], rec.Key[:])

	// Write Offset (8 bytes)
	binary.BigEndian.PutUint64(buf[64:72], uint64(rec.Offset))

	// Write Line (8 bytes)
	binary.BigEndian.PutUint64(buf[72:80], uint64(rec.Line))

	_, err := w.Write(buf[:])
	return err
}

// WriteBatchRecords writes a slice of records in a single write call
func WriteBatchRecords(w io.Writer, recs []IndexRecord) error {
	if len(recs) == 0 {
		return nil
	}

	// Allocate a single buffer for all records
	// Note: For very large batches, this might alloc too much,
	// but usage in flushChunk (~64KB-256KB) is fine.
	totalSize := len(recs) * RecordSize
	buf := make([]byte, totalSize)

	for i, rec := range recs {
		offset := i * RecordSize
		copy(buf[offset:offset+64], rec.Key[:])
		binary.BigEndian.PutUint64(buf[offset+64:offset+72], uint64(rec.Offset))
		binary.BigEndian.PutUint64(buf[offset+72:offset+80], uint64(rec.Line))
	}

	_, err := w.Write(buf)
	return err
}
