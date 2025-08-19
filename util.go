package beck

import (
	"os"
	"slices"
	"strconv"
	"strings"
)

// get datafiles retrieves all datafiles in the specified directory in their sorted order. oldest to latest
func getDatafiles(path string) ([]string, error) {
	dirs := []string{}
	f, err := os.Open(path)
	if err != nil {
		return dirs, err
	}

	dir, err := f.ReadDir(-1)
	if err != nil {
		return dirs, err
	}

	for _, d := range dir {
		// only process files with .data extension
		if d.IsDir() {
			continue
		}
		if strings.HasSuffix(d.Name(), ".data") {
			dirs = append(dirs, d.Name())
		}
	}

	// sort files
	slices.Sort(dirs)
	return dirs, nil
}

// getFileID retrieves the file id from a given datafile path
func getFileID(path string) (int, error) {
	id, err := strconv.ParseInt(strings.TrimSuffix(path, datafileExt), 10, 64)
	if err != nil {
		return 0, err
	}
	return int(id), nil
}
