package beck

import (
	"fmt"
	"sync"
)

// DB represents a collection of buckets persisted to a file on disk.
// Data access needs to be performed on an opened database instance
// ErrDatabaseNotOpen is returned if db is not opened while attempting to access a file
type Beck struct {
	keyDir         *keyDir
	activeDatafile *datafile
	// map of file ids to datafiles
	oldDataFiles map[int]*datafile

	activeIndex int
	cfg         *Config
	mu          sync.RWMutex
}

// Open a new or existing beck datastore with additional options.
// Valid options include sync on put (if this writer would
// prefer to sync the write file after every write operation).
// The directory must be readable and writable by this process, and
// only one process may open a Bitcask with read write at a time.
func Open(dataDir string, cfg *Config) (*Beck, error) {
	db := &Beck{}
	if cfg == nil {
		cfg = DefaultConfig
	}
	db.cfg = cfg

	// get all existing datafiles
	recentFileID := 0
	datafiles, err := getDatafiles(dataDir)
	if err != nil {
		return nil, err
	}
	for idx, dfPath := range datafiles {
		fileID, err := getFileID(dfPath)
		if err != nil {
			continue
		}

		df, err := NewDatafile(dfPath, true, false, 0)
		if err != nil {
			return nil, fmt.Errorf("failed to open datafile: %w", err)
		}

		db.oldDataFiles[fileID] = df

		// update most recent datafile to last entry
		if idx == len(datafiles)-1 {
			recentFileID = fileID
		}
	}

	// setup active file
	db.activeIndex = recentFileID + 1
	db.activeDatafile, err = NewDatafile(fmt.Sprintf("%d.%s", db.activeIndex, datafileExt), false, cfg.SyncOnWrite, cfg.SyncInterval)
	if err != nil {
		return nil, fmt.Errorf("failed to setup active datafile: %w", err)
	}

	// setup keydir
	// TODO: replay hints file to keydir on setup
	db.keyDir = NewKeyDir()

	// TODO: setup a lockfile to allow only a single writer to update db if multiple processes open it in rw mode.
	// this will prevent database corruption

	return db, nil
}

// Get retrieves a value by key from a the datastore. An error is returned if the key is not found
func (db *Beck) Get(key string) ([]byte, error) {
	// retrieve header from keydir
	header := db.keyDir.get(key)
	if header == nil {
		return nil, ErrKeyNotFound
	}

	// retrieve value from datadir
	var df *datafile
	fileId := header.FileID
	if fileId == db.activeIndex {
		df = db.activeDatafile
	} else {
		df = db.oldDataFiles[fileId]
	}

	if df == nil {
		return nil, ErrInvalidKey
	}

	val, err := df.read(uint64(header.ValuePosition))
	if err != nil {
		return nil, err
	}
	return val, nil
}

// Put stores a key and value to the datastore. It replaces the value if it already exists
func (db *Beck) Put(key string, val []byte) ([]byte, error)

// Delete removes a record by key from a the datastore. An error is returned if the key is not found
func (db *Beck) Delete(key string) error

// ListKeys returns a list of all the keys in the datastore
func (db *Beck) ListKeys() ([]string, error)

// Sync flushes all buffered writes to disk. It performs an fsync
func (db *Beck) Sync() error

// Close shutdowns the application and mark the current active-file as old
func (db *Beck) Close() error

// merge compacts all non-active files
func (db *Beck) merge() error
