package beck

import (
	"fmt"
	"sync"
	"time"
)

// DB represents a collection of buckets persisted to a file on disk.
// Data access needs to be performed on an opened database instance
// ErrDatabaseNotOpen is returned if db is not opened while attempting to access a file
type BeckDB struct {
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
func Open(cfg *Config) (*BeckDB, error) {
	db := &BeckDB{oldDataFiles: make(map[int]*datafile)}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	db.cfg = cfg

	// setup keydir
	db.keyDir = NewKeyDir()

	// get all existing datafiles
	recentFileID := 0
	datafiles, err := getDatafiles(cfg.DataDir)
	if err != nil {
		return nil, err
	}
	for idx, dfPath := range datafiles {
		fileID, err := getFileID(dfPath)
		if err != nil {
			continue
		}

		// replay data from hint file/datafile into keydir. fallback is the datafile
		err = db.replayFromHintFile(getHintFilePath(cfg.DataDir, fileID), fileID)
		if err != nil {
			// fallback on err
			err = db.replayFromDataFile(dfPath, fileID)
		} else {
			err = db.replayFromDataFile(dfPath, fileID)
		}

		if err != nil {
			return nil, fmt.Errorf("failed to replay data into keydir from datafile %v: %w", dfPath, err)
		}

		// now load datafile
		df, err := NewDatafile(dfPath, true, false, 0)
		if err != nil {
			return nil, fmt.Errorf("failed to open datafile, path=(%s): %w", dfPath, err)
		}

		db.mu.Lock()
		db.oldDataFiles[fileID] = df
		db.mu.Unlock()

		// update most recent datafile to last entry
		if idx == len(datafiles)-1 {
			recentFileID = fileID
		}
	}

	// setup active file
	db.activeIndex = recentFileID + 1
	activeDfPath := getDatafilePath(cfg.DataDir, db.activeIndex)
	db.activeDatafile, err = NewDatafile(activeDfPath, false, cfg.SyncOnWrite, cfg.SyncInterval)
	if err != nil {
		return nil, fmt.Errorf("failed to setup active datafile, path=(%s): %w", activeDfPath, err)
	}

	// TODO: setup a lockfile to allow only a single writer to update db if multiple processes open it in rw mode.
	// this will prevent database corruption

	// periodically flush buffer if user background sync
	if !cfg.ReadOnly && !cfg.SyncOnWrite {
		go db.Sync()
	}

	// monitor active datafile and merge old datafiles
	go db.Merge()
	go db.trackActiveDatafile()

	return db, nil
}

// Get retrieves a value by key from a the datastore. An error is returned if the key is not found
func (db *BeckDB) Get(key string) ([]byte, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	// retrieve header from keydir
	header := db.keyDir.get(key)
	if header == nil {
		return nil, ErrKeyNotFound
	}

	// retrieve value from datadir
	var df *datafile

	if header.fileID == db.activeIndex {
		df = db.activeDatafile
	} else {
		df = db.oldDataFiles[header.fileID]
	}

	if df == nil {
		return nil, ErrInvalidKey
	}

	val, err := df.read(header.recordPosition, header.recordSize)
	if err != nil {
		return nil, err
	}
	return val, nil
}

// Put stores a key and value to the datastore. It replaces the value if it already exists
func (db *BeckDB) Put(key string, val []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if err := validateEntry(key, val); err != nil {
		return err
	}
	// append to datastore then write to keydir
	size, offset, err := db.activeDatafile.append(key, val)
	if err != nil {
		return err
	}

	db.keyDir.put(key, db.activeIndex, size, offset)
	return nil
}

// Delete removes a record by key from a the datastore. An error is returned if the key is not found
func (db *BeckDB) Delete(key string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// check if val exists
	if header := db.keyDir.get(key); header == nil {
		return ErrKeyNotFound
	}

	// append tombstone entry to datastore then remove from keydir
	if _, _, err := db.activeDatafile.append(key, tombstoneVal); err != nil {
		return err
	}

	db.keyDir.delete(key)
	return nil
}

// ListKeys returns a list of all the keys in the datastore
func (db *BeckDB) ListKeys() []string {
	// rw lock since keydir remains same throughout
	db.mu.RLock()
	defer db.mu.RUnlock()

	return db.keyDir.listKeys()
}

// Sync flushes all buffered writes to disk. It performs an fsync on the active datafile
func (db *BeckDB) Sync() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.activeDatafile.sync()
}

// Close shutdowns the application and mark the current active-file as old
func (db *BeckDB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// close active datafile and all old file
	if err := db.activeDatafile.close(); err != nil {
		return fmt.Errorf("failed to close active datafile: %w", err)
	}

	for _, df := range db.oldDataFiles {
		if err := df.close(); err != nil {
			return fmt.Errorf("failed to close old datafile: %w", err)
		}
	}
	return nil
}

// Merge runs a background worker that periodically merge old datafiles
func (db *BeckDB) Merge() {
	ticker := time.NewTicker(db.cfg.MergeInterval)
	defer ticker.Stop()

	for range ticker.C {
		if err := db.Compact(); err != nil {
			// silently swallow error
		}
	}
}

// trackActiveDatafile monitors the active datafile to ensure it has not crossed the file limit
func (db *BeckDB) trackActiveDatafile() {
	ticker := time.NewTicker(db.cfg.TrackActiveDatafileInterval)
	defer ticker.Stop()
	maxWaitInterval := 10 * time.Minute

	for range ticker.C {
		// increase wait time if file wasn't rotated
		if !db.RotateActiveDatafile() {
			ticker.Reset(min(2*db.cfg.TrackActiveDatafileInterval, maxWaitInterval))
		}
	}
}
