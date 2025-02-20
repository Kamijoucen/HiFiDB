package kv

import "errors"

type Options struct {
	DirPath        string
	DataFileSize   int64
	EachSyncWrites bool
}

func checkOptions(options *Options) error {
	if options.DirPath == "" {
		return errors.New("database dir path is empty")
	}
	if options.DataFileSize <= 0 {
		return errors.New("database data file size is invalid")
	}

	return nil
}
