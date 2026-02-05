package common

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"io"

	"github.com/pierrec/lz4/v4"
)

const (
	// MagicCIDX is the magic header for the compressed index file
	MagicCIDX = "CIDX"
	// BlockTargetSize is the target size for uncompressed blocks (64KB)
	BlockTargetSize = 64 * 1024
)

// BlockMeta holds metadata for a single compressed block
type BlockMeta struct {
	StartKey    string `json:"startKey"`    // The first key in the block
	Offset      int64  `json:"offset"`      // Byte offset in the .cidx file where the block starts
	Length      int64  `json:"length"`      // Length of the compressed block in bytes
	RecordCount int64  `json:"recordCount"` // Number of records in this block (for fast COUNT)
	IsDistinct  bool   `json:"isDistinct"`  // Optimized: true if block contains only 1 unique key
}

// SparseIndex represents the footer of the .cidx file
type SparseIndex struct {
	Blocks []BlockMeta `json:"blocks"`
}

// BlockWriter handles writing compressed blocks to an io.Writer
type BlockWriter struct {
	w           io.Writer
	buffer      []IndexRecord
	currentSize int
	sparseIndex SparseIndex
	offset      int64
	lw          *lz4.Writer
	rawBuf      bytes.Buffer
	compBuf     bytes.Buffer
}

// NewBlockWriter creates a new BlockWriter
func NewBlockWriter(w io.Writer) (*BlockWriter, error) {
	// Write Magic Header
	n, err := w.Write([]byte(MagicCIDX))
	if err != nil {
		return nil, err
	}
	// Create lz4 writer once
	lw := lz4.NewWriter(io.Discard)
	// Apply fastest compression and 64K Block size preference
	// Use Apply pattern for options in v4
	_ = lw.Apply(lz4.BlockSizeOption(lz4.Block64Kb))

	return &BlockWriter{
		w:      w,
		buffer: make([]IndexRecord, 0, 1000), // Pre-allocate some space
		offset: int64(n),
		lw:     lw,
	}, nil
}

// WriteRecord adds a record to the buffer and flushes to disk if full across blocks
func (bw *BlockWriter) WriteRecord(rec IndexRecord) error {
	bw.buffer = append(bw.buffer, rec)
	// Approximate size check: Key length + 16 bytes for offsets
	bw.currentSize += len(rec.Key) + 16

	if bw.currentSize >= BlockTargetSize {
		return bw.FlushBlock()
	}
	return nil
}

// FlushBlock compresses the current buffer and writes it as a block
func (bw *BlockWriter) FlushBlock() error {
	if len(bw.buffer) == 0 {
		return nil
	}

	// 1. Serialize buffer to bytes
	bw.rawBuf.Reset()
	// Use batch write for speed
	if err := WriteBatchRecords(&bw.rawBuf, bw.buffer); err != nil {
		return err
	}

	// 2. Compress with LZ4
	bw.compBuf.Reset()
	bw.lw.Reset(&bw.compBuf)
	if _, err := bw.lw.Write(bw.rawBuf.Bytes()); err != nil {
		return err
	}
	if err := bw.lw.Close(); err != nil {
		return err
	}
	compressedBytes := bw.compBuf.Bytes()

	// 3. Record Metadata
	// Convert [64]byte key to string, trimming nulls
	keyStr := string(bytes.TrimRight(bw.buffer[0].Key[:], "\x00"))

	// Check if block is distinct (all keys are identical)
	isDistinct := true
	if len(bw.buffer) > 1 {
		firstKey := bw.buffer[0].Key
		for i := 1; i < len(bw.buffer); i++ {
			if firstKey != bw.buffer[i].Key {
				isDistinct = false
				break
			}
		}
	}

	meta := BlockMeta{
		StartKey:    keyStr,
		Offset:      bw.offset,
		Length:      int64(len(compressedBytes)),
		RecordCount: int64(len(bw.buffer)), // Track record count for fast COUNT(*)
		IsDistinct:  isDistinct,
	}
	bw.sparseIndex.Blocks = append(bw.sparseIndex.Blocks, meta)

	// 4. Write to Disk
	n, err := bw.w.Write(compressedBytes)
	if err != nil {
		return err
	}
	bw.offset += int64(n)

	// 5. Reset
	bw.buffer = bw.buffer[:0]
	bw.currentSize = 0
	return nil
}

// Close finalizes the file by writing the remaining buffer and the footer
func (bw *BlockWriter) Close() error {
	// Flush remaining records
	if err := bw.FlushBlock(); err != nil {
		return err
	}

	// Serialize Footer (Sparse Index)
	footerBytes, err := json.Marshal(bw.sparseIndex)
	if err != nil {
		return err
	}

	// Write Footer
	n, err := bw.w.Write(footerBytes)
	if err != nil {
		return err
	}

	// Write Footer Length (8 bytes)
	if err := binary.Write(bw.w, binary.BigEndian, int64(n)); err != nil {
		return err
	}

	return nil
}

// BlockReader handles reading compressed blocks
type BlockReader struct {
	r       io.ReadSeeker
	Footer  SparseIndex
	compBuf []byte        // reusable buffer for compressed block data
	recBuf  []IndexRecord // reusable buffer for decompressed records
}

// NewBlockReader initializes a reader and loads the SparseIndex
func NewBlockReader(r io.ReadSeeker) (*BlockReader, error) {
	// 1. Seek to end - 8 to get footer length
	if _, err := r.Seek(-8, io.SeekEnd); err != nil {
		return nil, err
	}

	var footerLen int64
	if err := binary.Read(r, binary.BigEndian, &footerLen); err != nil {
		return nil, err
	}

	// 2. Seek to Footer Start
	if _, err := r.Seek(-(8 + footerLen), io.SeekEnd); err != nil {
		return nil, err
	}

	// 3. Read Footer
	footerBytes := make([]byte, footerLen)
	if _, err := io.ReadFull(r, footerBytes); err != nil {
		return nil, err
	}

	var footer SparseIndex
	if err := json.Unmarshal(footerBytes, &footer); err != nil {
		return nil, err
	}

	return &BlockReader{
		r:      r,
		Footer: footer,
	}, nil
}

// ReadBlock reads and decompresses a specific block
func (br *BlockReader) ReadBlock(meta BlockMeta) ([]IndexRecord, error) {
	// Seek to block start
	if _, err := br.r.Seek(meta.Offset, io.SeekStart); err != nil {
		return nil, err
	}

	// Read compressed data checking reusable buffer
	needed := int(meta.Length)
	if cap(br.compBuf) < needed {
		br.compBuf = make([]byte, needed)
	}
	br.compBuf = br.compBuf[:needed]

	if _, err := io.ReadFull(br.r, br.compBuf); err != nil {
		return nil, err
	}

	// Decompress with LZ4
	lr := lz4.NewReader(bytes.NewReader(br.compBuf))

	// Since we don't know exact uncompressed size easily without reading headers or
	// assuming block size, we read all.
	// However, we can use ReadBatchRecords if we knew the count.
	// But standard Reader is fine since we are reading from memory (lz4 stream).
	// Wait, we need to read into br.recBuf.

	br.recBuf = br.recBuf[:0]

	for {
		rec, err := ReadRecord(lr)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		br.recBuf = append(br.recBuf, rec)
	}

	return br.recBuf, nil
}
