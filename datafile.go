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
	// header size without actual key and data (20 bytes)
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
	// skip if datafile is opened in read-only mode
	if d.readOnly {
		return 0, 0, ErrDatabaseReadOnly
	}

	d.mu.Lock()
	defer d.mu.Unlock()

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
	d.size += len(encoded)

	return len(val), offset, nil
}

// read retrieves the value of record at a given offset
func (d *datafile) read(offset uint64) ([]byte, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	// retrieve key and value size from header
	header := make([]byte, headerLen)
	n, err := d.f.ReadAt(header, int64(offset))
	if err != nil {
		return nil, err
	}
	if n < headerLen {
		return nil, ErrInvalidRecord
	}

	// decode header
	checksum := enc.Uint32(header[:crcLen])
	keySize := int(enc.Uint32(header[crcLen+timestampLen : crcLen+timestampLen+keySizeLen]))
	valSize := int(enc.Uint64(header[crcLen+timestampLen+keySizeLen : crcLen+timestampLen+keySizeLen+valSizeLen]))

	// read full record
	recordSize := headerLen + keySize + valSize
	record := make([]byte, recordSize)
	n, err = d.f.ReadAt(record, int64(offset))
	if err != nil {
		return nil, err
	}
	if n < recordSize {
		return nil, ErrInvalidRecord
	}

	// extract value
	key := record[headerLen : headerLen+keySize]
	val := record[headerLen+keySize : headerLen+keySize+valSize]

	// verify checksum and retrieve data
	if getChecksum(string(key), val) != checksum {
		return nil, ErrInvalidRecord
	}
	return val, nil
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
		defer d.mu.Unlock()

		if err := d.f.Sync(); err != nil {
			return err
		}
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
