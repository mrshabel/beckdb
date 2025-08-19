package beck

import "time"

type keyDir struct {
	// map of key to header
	data map[string]*header
}

type header struct {
	FileID        string
	ValueSize     int
	ValuePosition uint
	Timestamp     int64
}

func (k *keyDir) get(key string) *header {
	return k.data[key]
}

func (k *keyDir) put(key string, fileID string, value []byte, valuePosition uint) bool {
	// override if it exists
	val := k.get(key)
	k.data[key] = &header{FileID: fileID, ValueSize: len(value), ValuePosition: valuePosition, Timestamp: time.Now().Unix()}
	return val != nil
}

func (k *keyDir) delete(key string) bool {
	if val := k.get(key); val == nil {
		return false
	}

	delete(k.data, key)
	return true
}

func (k *keyDir) listKeys() []string {
	keys := make([]string, 0, len(k.data))
	for key := range k.data {
		keys = append(keys, key)
	}
	return keys
}
