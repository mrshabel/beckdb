package beck_test

import (
	"os"
	"testing"

	beck "github.com/mrshabel/beckdb"
	"github.com/stretchr/testify/require"
)

const (
	// 1mb
	maxFileSize = 1024 * 1024
)

func TestDB(t *testing.T) {
	// setup directory and db configs
	dataDir, err := os.MkdirTemp("./test", "beck")
	require.NoError(t, err)
	defer os.RemoveAll(dataDir)

	cfg := &beck.Config{DataDir: dataDir, MaxFileSize: maxFileSize, SyncOnWrite: true, ReadOnly: false}

	// bench test
	for _, tt := range []struct {
		name string
		fn   func(*testing.T, *beck.Config)
	}{
		{name: "test open", fn: testOpen},
		{name: "test put entry", fn: testPut},
		{name: "test retrieve entry", fn: testGet},
	} {
		t.Run(tt.name, func(t *testing.T) {
			tt.fn(t, cfg)
		})
	}
}

// test that database is accessible and can be opened in all modes. default is rw mode
func testOpen(t *testing.T, cfg *beck.Config) {
	db, err := beck.Open(cfg)
	require.NoError(t, err)
	// close db and open in read-only mode
	err = db.Close()
	require.NoError(t, err)

	cfg.ReadOnly = true
	db, err = beck.Open(cfg)
	require.NoError(t, err)

	err = db.Close()
	require.NoError(t, err)
}

// test that data can be written to the db
func testPut(t *testing.T, cfg *beck.Config) {
	db, err := beck.Open(cfg)
	require.NoError(t, err)
	defer db.Close()

	key := "name"
	val := []byte("mrshabel")

	err = db.Put(key, val)
	require.NoError(t, err)
}

// test that data can be retrieved from the db
func testGet(t *testing.T, cfg *beck.Config) {
	t.Helper()
	db, err := beck.Open(cfg)
	require.NoError(t, err)
	defer db.Close()

	key := "name"
	val := []byte("mrshabel")

	// attempt to retrieve missing key
	dbVal, err := db.Get(key)
	require.ErrorIs(t, err, beck.ErrKeyNotFound)
	require.Equal(t, []byte(nil), dbVal)

	// now we write val to db and attempt to retrieve it
	err = db.Put(key, val)
	require.NoError(t, err)

	dbVal, err = db.Get(key)
	require.NoError(t, err)
	require.Equal(t, val, dbVal)
}
