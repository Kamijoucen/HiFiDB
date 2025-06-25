package cfg

import (
	"errors"
)

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
	return &Options{
		DirPath:         "./data",
		DataFileSize:    1024 * 1024 * 1024, // 1GB
		SyncWrites:      false,
		MemoryIndexType: BTree,
		BytesPerSync:    0, // 不开启
		MMapAtStartup:   true,
	}
}
