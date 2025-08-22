package beck

import "time"

const (
	// 64 mb
	defaultMaxFileSize  = 64 << 20
	defaultSyncInterval = 1 * time.Second

	datafileExt = ".data"
	hintFileExt = ".hint"

	// maximum length of key in bytes
	maxKeySize = 32768
	// maximum length of value in bytes
	maxValueSize = 1 << 20
)

var (
	tombstoneVal = []byte{}
)

type Config struct {
	DataDir      string
	MaxFileSize  int64
	SyncOnWrite  bool
	SyncInterval time.Duration
	ReadOnly     bool
}

var DefaultConfig = &Config{
	MaxFileSize:  defaultMaxFileSize,
	SyncOnWrite:  true,
	SyncInterval: 0,
}
