package beck

// keydir is the in-memory index handler of the entire database. It maps keys to their respective headers

import (
	"sync"
	"time"
)

type keyDir struct {
	// map of key to header
	data map[string]*header
	mu   sync.RWMutex
}

type header struct {
	fileID     int
	recordSize int
	// position marking the start of the full record on disk
	recordPosition uint64
	timestamp      int64
}

type keyDirEntry struct {
	key    string
	header *header
}

func NewKeyDir() *keyDir {
	return &keyDir{
		data: make(map[string]*header),
	}
}

func (k *keyDir) get(key string) *header {
	k.mu.RLock()
	defer k.mu.RUnlock()
	h, ok := k.data[key]
	if !ok {
		return nil
	}

	return h
}

func (k *keyDir) put(key string, fileID int, recordSize int, recordPosition uint64) bool {
	k.mu.Lock()
	defer k.mu.Unlock()

	// override if it exists
	val := k.data[key]

	k.data[key] = &header{
		fileID:         fileID,
		recordSize:     recordSize,
		recordPosition: recordPosition,
		timestamp:      time.Now().Unix(),
	}
	return val != nil
}

// putBatch performs a batch insert of key-header pairs into keydir
func (k *keyDir) putBatch(entries []keyDirEntry) {
	k.mu.Lock()
	defer k.mu.Unlock()

	for _, entry := range entries {
		k.data[entry.key] = entry.header
	}
}

func (k *keyDir) delete(key string) bool {
	k.mu.Lock()
	defer k.mu.Unlock()
	if _, ok := k.data[key]; !ok {
		return false
	}

	delete(k.data, key)
	return true
}

func (k *keyDir) listKeys() []string {
	k.mu.RLock()
	defer k.mu.RUnlock()

	keys := make([]string, 0, len(k.data))
	for key := range k.data {
		keys = append(keys, key)
	}
	return keys
}
