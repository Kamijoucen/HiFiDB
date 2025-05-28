package cfg

import (
	"errors"
)

type Option func(*Options)

type Options struct {

	// DirPath      数据库目录路径
	DirPath string

	// DataFileSize  数据文件大小，单位为字节
	DataFileSize int64

	// SyncWrites    是否每次写入都进行同步
	SyncWrites bool

	// BytesPerSync   累计写入多少字节后进行一次同步
	BytesPerSync uint32

	// MemoryIndexType 内存索引类型
	MemoryIndexType IndexType

	// MMapAtStartUp 是否在启动时将数据文件映射到内存
	MMapAtStartup bool
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
		o.SyncWrites = sync
	}
}

func WithMemoryIndexType(indexType IndexType) Option {
	return func(o *Options) {
		o.MemoryIndexType = indexType
	}
}

func WithBytesPerSync(bytes uint32) Option {
	return func(o *Options) {
		o.BytesPerSync = bytes
	}
}

func WithMMapAtStartup(mmap bool) Option {
	return func(o *Options) {
		o.MMapAtStartup = mmap
	}
}

func NewOptions(opts ...Option) (*Options, error) {
	options := &Options{
		DirPath:         "",
		DataFileSize:    0,
		SyncWrites:      false,
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

func GetDBDefaultOptions() *Options {
	op, _ := NewOptions(
		WithDirPath("./data"),
		WithDataFileSize(1024*1024*1024), // 1GB
		WithEachSyncWrites(false),
		WithMemoryIndexType(BTree),
		WithBytesPerSync(0), // 不开启
		WithMMapAtStartup(true),
	)
	return op
}
