package beck

import "time"

const (
	// 64 mb
	defaultMaxFileSize  = 64 << 20
	defaultSyncInterval = 1 * time.Second

	datafileExt = ".data"
	hintFileExt = ".hint"
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
