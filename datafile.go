package beck

import (
	"bytes"
	"encoding/binary"
	"os"
	"sync"
	"time"
)

// datafile is a smallest unit of beckdb. It holds sequence of records in an append-only format. The record format is shown below:
// | crc (4-byte) | timestamp (8-byte) | keySize (4-byte) | valSize (8-byte) | key | val |

// section lengths in bytes
const (
	crcLen       = 4
	timestampLen = 8
	keySizeLen   = 4
	valSizeLen   = 8
	// header size without actual key and data (24 bytes)
	headerLen = crcLen + timestampLen + keySizeLen + valSizeLen
)

// encoding format
var (
	enc = binary.LittleEndian
)

type datafile struct {
	f *os.File

	// whether to perform fsync on write or not
	syncOnWrite  bool
	syncInterval time.Duration

	readOnly bool

	// current file content size
	size int
	mu   sync.RWMutex
}

func NewDatafile(name string, readOnly bool, syncOnWrite bool, syncInterval time.Duration) (*datafile, error) {
	// open file in append only mode if mode is rw
	perm := os.O_RDONLY
	if !readOnly {
		perm = os.O_APPEND | os.O_RDWR | os.O_CREATE
	}

	f, err := os.OpenFile(name, perm, 0644)
	if err != nil {
		return nil, err
	}

	// get file size for existing file
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}

	df := &datafile{
		f:            f,
		size:         int(fi.Size()),
		readOnly:     readOnly,
		syncOnWrite:  syncOnWrite,
		syncInterval: syncInterval,
	}

	return df, nil
}

// append the key-value pair to the file and return the value size, and position
func (d *datafile) append(key string, val []byte) (size int, offset uint64, err error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	// skip if datafile is opened in read-only mode
	if d.readOnly {
		return 0, 0, ErrDatabaseReadOnly
	}

	// create encoded record and write to file handler
	r := newRecord(key, val)
	encoded, err := r.encode()
	if err != nil {
		return 0, 0, err
	}

	n, err := d.f.Write(encoded)
	if err != nil {
		return 0, 0, err
	}
	if n < len(encoded) {
		return 0, 0, ErrIncompleteWrite
	}

	// sync if durable
	if d.syncOnWrite {
		if err := d.f.Sync(); err != nil {
			return 0, 0, err
		}
	}

	// update file size. the previous size is the offset for the current record
	offset = uint64(d.size)
	size = len(encoded)
	d.size += size

	return size, offset, nil
}

// read retrieves the value of record at a given offset
func (d *datafile) read(offset uint64, size int) ([]byte, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	// read full record and extract header
	record := make([]byte, size)
	n, err := d.f.ReadAt(record, int64(offset))
	if err != nil {
		return nil, err
	}
	if n < size {
		return nil, ErrInvalidRecord
	}

	// decode header
	header := record[:headerLen]

	checksum := enc.Uint32(header[:crcLen])
	keySize := int(enc.Uint32(header[crcLen+timestampLen : crcLen+timestampLen+keySizeLen]))
	valSize := int(enc.Uint64(header[crcLen+timestampLen+keySizeLen:]))

	// extract value
	key := record[headerLen : headerLen+keySize]
	val := record[headerLen+keySize : headerLen+keySize+valSize]

	// verify checksum and retrieve data
	if getChecksum(string(key), val) != checksum {
		return nil, ErrInvalidRecord
	}
	return val, nil
}

// readRecord reads the full record from a given offset without knowing the record size.
// this is useful for background merging. the record and total size is returned
func (d *datafile) readRecord(offset uint64) (*record, int, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	// retrieve key and value size from header
	header := make([]byte, headerLen)
	n, err := d.f.ReadAt(header, int64(offset))
	if err != nil {
		return nil, 0, err
	}
	if n < headerLen {
		return nil, 0, ErrInvalidRecord
	}

	checksum := enc.Uint32(header[:crcLen])
	timestamp := int64(enc.Uint64(header[crcLen : crcLen+timestampLen]))
	keySize := int(enc.Uint32(header[crcLen+timestampLen : crcLen+timestampLen+keySizeLen]))
	valSize := int(enc.Uint64(header[crcLen+timestampLen+keySizeLen:]))

	// read full record
	recordSize := headerLen + keySize + valSize
	data := make([]byte, recordSize)
	n, err = d.f.ReadAt(data, int64(offset))
	if err != nil {
		return nil, 0, err
	}
	if n < recordSize {
		return nil, 0, ErrInvalidRecord
	}

	// extract value
	key := data[headerLen : headerLen+keySize]
	val := data[headerLen+keySize : headerLen+keySize+valSize]

	// verify checksum and retrieve data
	if getChecksum(string(key), val) != checksum {
		return nil, 0, ErrInvalidRecord
	}
	return &record{
		checksum:  checksum,
		timestamp: timestamp,
		keySize:   keySize,
		valSize:   valSize,
		key:       string(key),
		val:       val,
	}, recordSize, nil
}

// sync flushes all buffered writes to disk in the specified interval
func (d *datafile) sync() error {
	if d.syncInterval <= 0 {
		return nil
	}
	if d.readOnly {
		return ErrDatabaseReadOnly
	}

	ticker := time.NewTicker(d.syncInterval)
	defer ticker.Stop()

	for range ticker.C {
		d.mu.Lock()

		if err := d.f.Sync(); err != nil {
			d.mu.Unlock()
			return err
		}

		d.mu.Unlock()
	}
	return nil
}

// persist flushes all buffered writes to disk instantly
func (d *datafile) persist() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.readOnly {
		return ErrDatabaseReadOnly
	}

	if err := d.f.Sync(); err != nil {
		return err
	}
	return nil
}

// close flushes all pending writes and to disk and finally close the file
func (d *datafile) close() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	// sync only when file is opened for writing
	if !d.readOnly {
		if err := d.f.Sync(); err != nil {
			return err
		}
	}

	return d.f.Close()
}

// purge closes the current datafile and removes it from disk.
// this should be called after all references to the current datafile are cleared
func (d *datafile) purge() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if err := d.f.Close(); err != nil {
		return err
	}

	return os.Remove(d.f.Name())
}

// record is a disk representation of the key-value record with its metadata
type record struct {
	checksum  uint32
	timestamp int64
	keySize   int
	valSize   int
	key       string
	val       []byte
}

func newRecord(key string, val []byte) *record {
	checksum := getChecksum(key, val)

	return &record{
		checksum:  checksum,
		timestamp: time.Now().Unix(),
		keySize:   len(key),
		valSize:   len(val),
		key:       key,
		val:       val,
	}
}

// encode returns a little-endian encoded format of the record as specified in the documentation.
func (r *record) encode() ([]byte, error) {
	// write header: checksum, timestamp, key size, val size to buffer
	var buf bytes.Buffer

	binary.Write(&buf, enc, r.checksum)
	binary.Write(&buf, enc, r.timestamp)
	binary.Write(&buf, enc, uint32(r.keySize))
	binary.Write(&buf, enc, uint64(r.valSize))

	// write key and val
	buf.WriteString(r.key)
	buf.Write(r.val)

	return buf.Bytes(), nil
}

// decodeRecord attempts to decode the binary data into the record
func decodeRecord(data []byte) (*record, error) {
	if len(data) < headerLen {
		return nil, ErrInvalidRecord
	}

	// extract headers: checksum, timestamp, key size, val size
	checksum := enc.Uint32(data[:crcLen])
	timestamp := int64(enc.Uint64(data[crcLen : crcLen+timestampLen]))
	keySize := int(enc.Uint32(data[crcLen+timestampLen : crcLen+timestampLen+keySizeLen]))
	valSize := int(enc.Uint64(data[crcLen+timestampLen+keySizeLen : crcLen+timestampLen+keySizeLen+valSizeLen]))

	if len(data) < headerLen+keySize+valSize {
		return nil, ErrInvalidRecord
	}

	// extract key and value
	key := string(data[headerLen : headerLen+keySize])
	val := data[headerLen+keySize : headerLen+keySize+valSize]

	return &record{
		checksum:  checksum,
		timestamp: timestamp,
		keySize:   keySize,
		valSize:   valSize,
		key:       key,
		val:       val,
	}, nil
}
