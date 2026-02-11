package common

import (
	"sync"
)

// BlockCacheEntry holds cached decompressed records for a single block.
type BlockCacheEntry struct {
	Records []IndexRecord
	key     string
	prev    *BlockCacheEntry
	next    *BlockCacheEntry
}

// BlockCache is a memory-bounded LRU cache for decompressed index blocks.
// Thread-safe. Evicts least-recently-used entries when memory limit is exceeded.
type BlockCache struct {
	mu       sync.RWMutex
	items    map[string]*BlockCacheEntry
	head     *BlockCacheEntry // most recent
	tail     *BlockCacheEntry // least recent
	curBytes int64
	maxBytes int64
}

// NewBlockCache creates an LRU cache with a maximum memory budget (in bytes).
// Each cached record is ~80 bytes (RecordSize).
func NewBlockCache(maxBytes int64) *BlockCache {
	return &BlockCache{
		items:    make(map[string]*BlockCacheEntry),
		maxBytes: maxBytes,
	}
}

// Get retrieves cached records for a given cache key.
// Returns nil if not found. Promotes entry to head on hit.
func (bc *BlockCache) Get(key string) []IndexRecord {
	bc.mu.Lock()
	defer bc.mu.Unlock()

	entry, ok := bc.items[key]
	if !ok {
		return nil
	}

	// Promote to head (most recent)
	bc.moveToHead(entry)
	return entry.Records
}

// Put stores decompressed records in the cache.
// Evicts LRU entries if the cache exceeds its memory budget.
func (bc *BlockCache) Put(key string, records []IndexRecord) {
	bc.mu.Lock()
	defer bc.mu.Unlock()

	// Don't cache if already present
	if _, ok := bc.items[key]; ok {
		return
	}

	entryBytes := int64(len(records)) * int64(RecordSize)

	// Don't cache if single block exceeds budget
	if entryBytes > bc.maxBytes {
		return
	}

	// Evict until we have room
	for bc.curBytes+entryBytes > bc.maxBytes && bc.tail != nil {
		bc.evict()
	}

	entry := &BlockCacheEntry{
		Records: records,
		key:     key,
	}
	bc.items[key] = entry
	bc.curBytes += entryBytes
	bc.addToHead(entry)
}

// Stats returns current cache statistics.
func (bc *BlockCache) Stats() (entries int, bytesUsed int64, bytesCap int64) {
	bc.mu.RLock()
	defer bc.mu.RUnlock()
	return len(bc.items), bc.curBytes, bc.maxBytes
}

// --- internal linked list operations ---

func (bc *BlockCache) addToHead(entry *BlockCacheEntry) {
	entry.prev = nil
	entry.next = bc.head
	if bc.head != nil {
		bc.head.prev = entry
	}
	bc.head = entry
	if bc.tail == nil {
		bc.tail = entry
	}
}

func (bc *BlockCache) moveToHead(entry *BlockCacheEntry) {
	if entry == bc.head {
		return
	}
	bc.removeFromList(entry)
	bc.addToHead(entry)
}

func (bc *BlockCache) removeFromList(entry *BlockCacheEntry) {
	if entry.prev != nil {
		entry.prev.next = entry.next
	} else {
		bc.head = entry.next
	}
	if entry.next != nil {
		entry.next.prev = entry.prev
	} else {
		bc.tail = entry.prev
	}
	entry.prev = nil
	entry.next = nil
}

func (bc *BlockCache) evict() {
	if bc.tail == nil {
		return
	}
	victim := bc.tail
	bc.removeFromList(victim)
	bc.curBytes -= int64(len(victim.Records)) * int64(RecordSize)
	delete(bc.items, victim.key)
}
