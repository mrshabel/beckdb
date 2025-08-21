package beck_test

import (
	"fmt"
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
	dataDir, err := os.MkdirTemp("", "beck")
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

// benchmarks
func BenchmarkDb(b *testing.B) {
	// setup directory and db configs
	dataDir, err := os.MkdirTemp("", "beck_bench")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dataDir)

	// start db
	db, err := beck.Open(&beck.Config{
		DataDir:     dataDir,
		MaxFileSize: maxFileSize,
		// disabled fsync
		SyncOnWrite: false,
		ReadOnly:    false,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	for _, tt := range []struct {
		name string
		fn   func(*testing.B, *beck.BeckDB)
	}{
		{name: "benchmark put entry", fn: benchmarkPut},
		{name: "benchmark get entry", fn: benchmarkGet},
		{name: "benchmark put and get entry", fn: benchmarkPutGet},
	} {
		b.Run(tt.name, func(b *testing.B) {
			tt.fn(b, db)
		})
	}
}

// this benchmark will be used for starting and shutting down the db
func BenchmarkOpenClose(b *testing.B) {
	// setup directory and db configs
	dataDir, err := os.MkdirTemp("", "beck_bench_oc")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dataDir)

	cfg := &beck.Config{
		DataDir:     dataDir,
		MaxFileSize: maxFileSize,
		// disabled fsync
		SyncOnWrite: false,
		ReadOnly:    false,
	}

	for range b.N {
		db, err := beck.Open(cfg)
		if err != nil {
			b.Fatal(err)
		}

		if err = db.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkPut(b *testing.B, db *beck.BeckDB) {
	key := "name"
	val := []byte("mrshabel")

	for idx := range b.N {
		if err := db.Put(fmt.Sprintf("%s-%d", key, idx), val); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkGet(b *testing.B, db *beck.BeckDB) {
	key := "name"
	val := []byte("mrshabel")

	// seed 1k data and reset benchmark for next iteration
	seedKeys := 1000
	for idx := range seedKeys {
		if err := db.Put(fmt.Sprintf("%s-%d", key, idx), val); err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()
	b.ReportAllocs()

	// only retrieve up to seeded keys
	for idx := range b.N {
		_, err := db.Get(fmt.Sprintf("%s-%d", key, idx%seedKeys))
		if err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkPutGet(b *testing.B, db *beck.BeckDB) {
	key := "name_test"
	val := []byte("mrshabel")

	// put data into db
	for idx := range b.N {
		if err := db.Put(fmt.Sprintf("%s-%d", key, idx), val); err != nil {
			b.Fatal(err)
		}
	}

	// get data from db
	for idx := range b.N {
		_, err := db.Get(fmt.Sprintf("%s-%d", key, idx))
		if err != nil {
			b.Fatal(err)
		}
	}
}
