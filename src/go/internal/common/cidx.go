package common

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"

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

// BlockReader handles reading compressed blocks.
// Supports two modes: seek-based (via io.ReadSeeker) and mmap-based (zero-copy).
type BlockReader struct {
	r         io.ReadSeeker // nil when using mmap mode
	mmapData  []byte        // non-nil when using mmap mode (zero-copy)
	Footer    SparseIndex
	compBuf   []byte        // reusable buffer for compressed block data
	decompBuf []byte        // reusable buffer for decompressed block data
	recBuf    []IndexRecord // reusable buffer for decompressed records
}

// NewBlockReader initializes a reader and loads the SparseIndex (seek-based mode).
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

// NewBlockReaderMmap creates a mmap-based block reader (zero-copy, minimal memory).
// The file is memory-mapped and the footer is parsed directly from mapped memory.
// Call Cleanup() when done to unmap.
func NewBlockReaderMmap(path string) (*BlockReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()

	data, err := MmapFile(f)
	if err != nil {
		return nil, err
	}

	if len(data) < 8 {
		_ = MunmapFile(data)
		return nil, fmt.Errorf("index file too small: %d bytes", len(data))
	}

	// Parse footer length from last 8 bytes (zero I/O — direct memory access)
	footerLen := int64(binary.BigEndian.Uint64(data[len(data)-8:]))
	footerStart := int64(len(data)) - 8 - footerLen
	if footerStart < 4 { // must be after CIDX magic
		_ = MunmapFile(data)
		return nil, fmt.Errorf("invalid footer: start=%d", footerStart)
	}

	// Parse footer from mapped memory (zero-copy)
	var footer SparseIndex
	if err := json.Unmarshal(data[footerStart:int64(len(data))-8], &footer); err != nil {
		_ = MunmapFile(data)
		return nil, err
	}

	return &BlockReader{
		mmapData: data,
		Footer:   footer,
	}, nil
}

// Cleanup releases mmap resources. Safe to call on non-mmap readers.
func (br *BlockReader) Cleanup() {
	if br.mmapData != nil {
		_ = MunmapFile(br.mmapData)
		br.mmapData = nil
	}
}

// ReadBlock reads and decompresses a specific block using batch parsing.
// Decompresses the full block into a flat buffer, then batch-parses all records at once.
// Uses mmap zero-copy when available, otherwise falls back to seek+read.
func (br *BlockReader) ReadBlock(meta BlockMeta) ([]IndexRecord, error) {
	var compData []byte

	if br.mmapData != nil {
		// Mmap mode: zero-copy slice directly into mapped memory (no syscalls)
		end := meta.Offset + meta.Length
		if end > int64(len(br.mmapData)) {
			return nil, fmt.Errorf("block extends past mmap boundary: %d > %d", end, len(br.mmapData))
		}
		compData = br.mmapData[meta.Offset:end]
	} else {
		// Seek mode: traditional file I/O
		if _, err := br.r.Seek(meta.Offset, io.SeekStart); err != nil {
			return nil, err
		}

		needed := int(meta.Length)
		if cap(br.compBuf) < needed {
			br.compBuf = make([]byte, needed)
		}
		br.compBuf = br.compBuf[:needed]

		if _, err := io.ReadFull(br.r, br.compBuf); err != nil {
			return nil, err
		}
		compData = br.compBuf
	}

	// Decompress entire block into a flat buffer
	lr := lz4.NewReader(bytes.NewReader(compData))

	// Use decompBuf for decompressed data (reusable)
	// Estimate: each block is ~64KB uncompressed
	if cap(br.decompBuf) < BlockTargetSize*2 {
		br.decompBuf = make([]byte, 0, BlockTargetSize*2)
	}
	br.decompBuf = br.decompBuf[:0]

	// Read all decompressed data using a temp buffer
	var tmpBuf [8192]byte
	for {
		n, err := lr.Read(tmpBuf[:])
		if n > 0 {
			br.decompBuf = append(br.decompBuf, tmpBuf[:n]...)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
	}

	// Batch parse all records at once (single pass, zero per-record overhead)
	count := len(br.decompBuf) / RecordSize
	if count == 0 {
		br.recBuf = br.recBuf[:0]
		return br.recBuf, nil
	}

	// Inline batch parse to reuse br.recBuf
	if cap(br.recBuf) < count {
		br.recBuf = make([]IndexRecord, count)
	}
	br.recBuf = br.recBuf[:count]

	for i := 0; i < count; i++ {
		offset := i * RecordSize
		br.recBuf[i] = IndexRecord{
			Key:    *(*[64]byte)(br.decompBuf[offset : offset+64]),
			Offset: int64(binary.BigEndian.Uint64(br.decompBuf[offset+64 : offset+72])),
			Line:   int64(binary.BigEndian.Uint64(br.decompBuf[offset+72 : offset+80])),
		}
	}

	return br.recBuf, nil
}
