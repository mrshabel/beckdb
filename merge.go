package beck

import (
	"fmt"
	"io"
	"time"
)

type entry struct {
	key string
	val []byte
}

// compaction and background merging of old datafiles to produce a single datafile and hint file
func (db *BeckDB) Compact() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if len(db.oldDataFiles) < 2 {
		return nil
	}

	liveEntries := []entry{}
	staleFileIDs := make([]int, 0, len(db.oldDataFiles))

	// begin merge by processing each file and checking if record's key matches the exact file and offset
	for fileID, datafile := range db.oldDataFiles {
		// track offset for each entry and process until EOF or error is encountered
		var offset uint64
		for {
			record, size, err := datafile.readRecord(offset)
			if err == io.EOF {
				break
			}
			if err != nil {
				return fmt.Errorf("failed to read record from file %d: %w", fileID, err)
			}

			// write record only when its metadata matches what is in keydir
			header := db.keyDir.get(record.key)
			if header != nil && header.fileID == fileID && header.recordPosition == offset {
				liveEntries = append(liveEntries, entry{key: record.key, val: record.val})
			}

			// update size
			offset += uint64(size)
		}

		// mark file as stale
		staleFileIDs = append(staleFileIDs, fileID)
	}

	// cleanup conflicting merged file if it exists
	if existingMerged, exists := db.oldDataFiles[0]; exists {
		existingMerged.purge()
		delete(db.oldDataFiles, 0)
	}

	// write live entries to new merged file and update keydir accordingly
	mergedFileID := defaultMergedFileID
	mergedDF, err := NewDatafile(getDatafilePath(db.cfg.DataDir, mergedFileID), false, false, 0)
	if err != nil {
		return fmt.Errorf("failed to create merged datafile: %w", err)
	}
	hintf, err := NewHintFile(getHintFilePath(db.cfg.DataDir, mergedFileID), false)
	if err != nil {
		mergedDF.purge()
		return fmt.Errorf("failed to create hint file: %w", err)
	}
	defer hintf.close()

	mergedKeyDirEntries := make([]keyDirEntry, 0, len(liveEntries))
	now := time.Now().Unix()

	for _, entry := range liveEntries {
		// write to datafile and hintfile while removing both files on error
		size, offset, err := mergedDF.append(entry.key, entry.val)
		if err != nil {
			mergedDF.purge()
			hintf.purge()
			return fmt.Errorf("failed to append to merged datafile: %w", err)
		}
		if err := hintf.append(entry.key, size, offset); err != nil {
			mergedDF.purge()
			hintf.purge()
			return fmt.Errorf("failed to append to hint file: %w", err)
		}

		mergedKeyDirEntries = append(mergedKeyDirEntries,
			keyDirEntry{
				key: entry.key,
				header: &header{
					fileID:         mergedFileID,
					recordSize:     size,
					recordPosition: offset,
					timestamp:      now,
				},
			})
	}

	// sync all written entries
	if err := mergedDF.persist(); err != nil {
		mergedDF.purge()
		hintf.purge()
		return fmt.Errorf("failed to persist merged file: %w", err)
	}
	if err := hintf.sync(); err != nil {
		mergedDF.purge()
		hintf.purge()
		return fmt.Errorf("failed to persist hint file: %w", err)
	}

	// mark merged datafile as old datafile
	db.oldDataFiles[mergedFileID] = mergedDF

	// write all entries to key dir at once
	db.keyDir.putBatch(mergedKeyDirEntries)

	return db.cleanupStaleDatafiles(staleFileIDs)
}

// replay the keydir from a hint file
func (db *BeckDB) replayFromHintFile(path string, fileID int) error {
	hintf, err := NewHintFile(path, true)
	if err != nil {
		return err
	}

	defer func() {
		// reset offset to start of the file for next read.
		hintf.f.Seek(0, 0)
		// finally close file
		hintf.close()
	}()

	// read hint file sequentially until end of file or error
	for {
		hint, err := hintf.readNext()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		db.keyDir.put(hint.key, fileID, hint.recordSize, hint.recordPosition)
	}
	return nil
}

// replay keydir from a datafile
func (db *BeckDB) replayFromDataFile(dfPath string, fileID int) error {
	// open datafile in read-only mode
	df, err := NewDatafile(dfPath, true, false, 0)
	if err != nil {
		return err
	}
	defer df.close()

	// read until end of file or error
	var offset uint64
	for {
		record, size, err := df.readRecord(offset)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		// write to keydir
		db.keyDir.put(record.key, fileID, size, offset)
		offset += uint64(size)
	}
	return nil
}

// RotateActiveDatafile swaps the active bool into an old data if it's exceeded max datafile size
func (db *BeckDB) RotateActiveDatafile() bool {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.activeDatafile.size < int(db.cfg.MaxFileSize) {
		return false
	}

	// move active file to old datafile and create a new datafile
	activeFileID := db.activeIndex + 1
	newActiveDatafile, err := NewDatafile(getDatafilePath(db.cfg.DataDir, activeFileID), false, db.cfg.SyncOnWrite, db.cfg.SyncInterval)
	if err != nil {
		// fail silently
		return false
	}

	db.oldDataFiles[db.activeIndex] = db.activeDatafile
	db.activeDatafile = newActiveDatafile
	db.activeIndex = activeFileID

	return true
}

// remove all stale datafiles
func (db *BeckDB) cleanupStaleDatafiles(fileIDs []int) error {
	// track return only last known error
	var knownErr error

	for _, fileID := range fileIDs {
		datafile, exists := db.oldDataFiles[fileID]
		if !exists {
			continue
		}
		if err := datafile.purge(); err != nil {
			knownErr = err
		}

		delete(db.oldDataFiles, fileID)
	}

	return knownErr
}
