package beck

import (
	"bytes"
	"encoding/binary"
	"os"
	"sync"
)

// hintfile contains a snapshot of the datafile for quick bootstrap when building the keydir from an existing datafile
// | keySize (4-byte) | record size (8-byte) | record offset (8-byte) | key |

// section lengths in bytes
const (
	hintRecordSizeLen   = 8
	hintRecordOffsetLen = 8
	// header size without actual key and data (20 bytes)
	hintHeaderLen = keySizeLen + hintRecordSizeLen + hintRecordOffsetLen
)

type hintFile struct {
	f *os.File

	readOnly bool
	mu       sync.RWMutex
}

// hintRecord is a single hint entry
type hintRecord struct {
	key            string
	recordSize     int
	recordPosition uint64
}

func NewHintFile(name string, readOnly bool) (*hintFile, error) {
	// open file in append only mode if mode is rw
	perm := os.O_RDONLY
	if !readOnly {
		perm = os.O_APPEND | os.O_RDWR | os.O_CREATE
	}

	f, err := os.OpenFile(name, perm, 0644)
	if err != nil {
		return nil, err
	}

	df := &hintFile{
		f:        f,
		readOnly: readOnly,
	}

	return df, nil
}

// append writes a hint record to the file
func (h *hintFile) append(key string, recordSize int, recordPosition uint64) (err error) {
	// skip if hint file is opened in read-only mode
	if h.readOnly {
		return ErrDatabaseReadOnly
	}
	h.mu.Lock()
	defer h.mu.Unlock()

	var buf bytes.Buffer
	keyBytes := []byte(key)

	binary.Write(&buf, enc, uint32(len(keyBytes)))
	binary.Write(&buf, enc, uint64(recordSize))
	binary.Write(&buf, enc, recordPosition)

	// write key
	buf.Write(keyBytes)

	_, err = h.f.Write(buf.Bytes())
	return err
}

// readNext reads the next record from the hint file without resetting the offset position
func (h *hintFile) readNext() (*hintRecord, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	// extract header
	header := make([]byte, hintHeaderLen)
	n, err := h.f.Read(header)
	if err != nil {
		return nil, err
	}
	if n < hintHeaderLen {
		return nil, ErrInvalidRecord
	}

	keySize := int(enc.Uint32(header[:keySizeLen]))
	recordSize := int(enc.Uint64(header[keySizeLen : keySizeLen+hintRecordSizeLen]))
	recordPosition := int(enc.Uint64(header[keySizeLen+hintRecordSizeLen : keySizeLen+hintRecordSizeLen+hintRecordOffsetLen]))

	// read key
	keyBytes := make([]byte, keySize)
	n, err = h.f.Read(keyBytes)
	if err != nil {
		return nil, err
	}
	if n < keySize {
		return nil, ErrInvalidRecord
	}

	return &hintRecord{
		key:            string(keyBytes),
		recordPosition: uint64(recordPosition),
		recordSize:     recordSize,
	}, nil
}

// persist flushes all buffered writes to disk instantly
func (h *hintFile) sync() error {
	if h.readOnly {
		return ErrDatabaseReadOnly
	}
	h.mu.Lock()
	defer h.mu.Unlock()

	return h.f.Sync()
}

// close closes the hint file
func (h *hintFile) close() error {
	h.mu.Lock()
	defer h.mu.Unlock()

	// sync only when file is opened for writing
	if !h.readOnly {
		if err := h.f.Sync(); err != nil {
			return err
		}
	}

	return h.f.Close()
}

// purge closes the current hint file and removes it from disk.
// this should be called after all references to the current hint file are cleared
func (h *hintFile) purge() error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if err := h.f.Close(); err != nil {
		return err
	}

	return os.Remove(h.f.Name())
}
