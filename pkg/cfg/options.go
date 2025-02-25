package cfg

import (
	"errors"
)

type Option func(*Options)

type Options struct {
	DirPath         string
	DataFileSize    int64
	EachSyncWrites  bool
	MemoryIndexType IndexType
}

func WithDirPath(dirPath string) Option {
	return func(o *Options) {
		o.DirPath = dirPath
	}
}

func WithDataFileSize(size int64) Option {
	return func(o *Options) {
		o.DataFileSize = size
	}
}

func WithEachSyncWrites(sync bool) Option {
	return func(o *Options) {
		o.EachSyncWrites = sync
	}
}

func WithMemoryIndexType(indexType IndexType) Option {
	return func(o *Options) {
		o.MemoryIndexType = indexType
	}
}

func NewOptions(opts ...Option) (*Options, error) {
	options := &Options{
		DirPath:         "",
		DataFileSize:    0,
		EachSyncWrites:  false,
		MemoryIndexType: BTree,
	}

	for _, opt := range opts {
		opt(options)
	}

	if err := CheckOptions(options); err != nil {
		return nil, err
	}

	return options, nil
}

func CheckOptions(options *Options) error {
	if options.DirPath == "" {
		return errors.New("database dir path is empty")
	}
	if options.DataFileSize <= 0 {
		return errors.New("database data file size is invalid")
	}
	return nil
}

func GetDefaultOptions() *Options {
	op, _ := NewOptions(
		WithDirPath("./data"),
		WithDataFileSize(1024*1024*1024), // 1GB
		WithEachSyncWrites(false),
		WithMemoryIndexType(BTree),
	)
	return op
}
