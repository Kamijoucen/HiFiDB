package errs

import "errors"

var (
	ErrKeyIsEmpty        = errors.New("the key is empty")
	ErrIndexUpdateFailed = errors.New("index update failed")
	ErrKeyNotFound       = errors.New("key not found")
	ErrDataFileNotFound  = errors.New("data file not found")
	ErrDataDirCorrupted  = errors.New("data dir corrupted")
	ErrInvalidCRC        = errors.New("invalid crc")
	ErrExceedMaxFileSize = errors.New("exceed max file size")
	ErrMergeIsProgress   = errors.New("merge is progress")
)
