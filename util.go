package beck

import (
	"bytes"
	"fmt"
	"hash/crc32"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
)

// get datafiles retrieves all datafiles in the specified directory in their sorted order. oldest to latest
func getDatafiles(path string) ([]string, error) {
	// get all files matching the datafile extension
	dirs, err := filepath.Glob(filepath.Join(path, "*"+datafileExt))
	if err != nil {
		return nil, err
	}

	// sort files by file id
	slices.SortFunc(dirs, func(a, b string) int {
		idA, _ := getFileID(a)
		idB, _ := getFileID(b)
		return idA - idB
	})
	return dirs, nil
}

// getFileID retrieves the file id from a given datafile path
func getFileID(path string) (int, error) {
	filename := filepath.Base(path)
	id, err := strconv.ParseInt(strings.TrimSuffix(filename, datafileExt), 10, 64)
	if err != nil {
		return 0, err
	}
	return int(id), nil
}

// getChecksum computes the checksum of key and value combination as byte slice
func getChecksum(key string, val []byte) uint32 {
	// compute checksum of key and value combination as byte slice
	rawData := [][]byte{[]byte(key), val}
	data := bytes.Join(rawData, []byte(""))
	return crc32.ChecksumIEEE(data)
}

// getDatafilePath composes the filepath for the specified datadir based on the index
func getDatafilePath(dataDir string, index int) string {
	return filepath.Join(dataDir, fmt.Sprintf("%d%s", index, datafileExt))
}

// validateEntry runs the key-value pair against all constraints
func validateEntry(key string, val []byte) error {
	if key == "" {
		return ErrKeyRequired
	}
	if len(key) > maxKeySize {
		return ErrKeyTooLarge
	}
	if len(val) > maxValueSize {
		return ErrValTooLarge
	}

	return nil
}
