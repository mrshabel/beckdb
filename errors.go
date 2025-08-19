package beck

import "errors"

// general errors
var (
	ErrDatabaseNotOpen = errors.New("database not open")
	ErrInvalidRecord   = errors.New("invalid record format")
	ErrInvalidChecksum = errors.New("invalid value checksum. potential data corruption")
	ErrIncompleteWrite = errors.New("incomplete write")
)

// key-val errors
var (
	ErrKeyNotFound = errors.New("key not found")
	ErrInvalidKey  = errors.New("key is invalid")
	ErrKeyTooLarge = errors.New("key is too large")
	ErrValTooLarge = errors.New("value is too large")
)
