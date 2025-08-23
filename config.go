package beck

import "time"

const (
	// 64 mb
	defaultMaxFileSize   = 64 << 20
	defaultSyncInterval  = 1 * time.Second
	defaultMergeInterval = 5 * time.Minute
	// interval to check whether active file has exceeded max size or not
	defaultTrackActiveDatafileInterval = 5 * time.Minute

	datafileExt   = ".data"
	hintFileExt   = ".hint"
	mergedFileExt = ".merge"

	// file id for merged files
	defaultMergedFileID = 0

	// maximum length of key in bytes
	maxKeySize = 32768
	// maximum length of value in bytes
	maxValueSize = 1 << 20
)

var (
	tombstoneVal = []byte{}
)

type Config struct {
	DataDir                     string
	MaxFileSize                 int64
	SyncOnWrite                 bool
	SyncInterval                time.Duration
	MergeInterval               time.Duration
	TrackActiveDatafileInterval time.Duration
	ReadOnly                    bool
}

func (cfg *Config) validate() error {
	if cfg.DataDir == "" {
		return ErrDatabaseDirectoryRequired
	}
	if cfg.MaxFileSize <= 0 {
		cfg.MaxFileSize = defaultMaxFileSize
	}
	if cfg.SyncOnWrite && cfg.SyncInterval <= 0 {
		cfg.SyncInterval = defaultSyncInterval
	}
	if cfg.MergeInterval <= 0 {
		cfg.MergeInterval = defaultMergeInterval
	}
	if cfg.TrackActiveDatafileInterval <= 0 {
		cfg.TrackActiveDatafileInterval = defaultTrackActiveDatafileInterval
	}
	return nil
}

var DefaultConfig = &Config{
	MaxFileSize:                 defaultMaxFileSize,
	SyncOnWrite:                 true,
	SyncInterval:                0,
	MergeInterval:               defaultMergeInterval,
	TrackActiveDatafileInterval: defaultTrackActiveDatafileInterval,
}
