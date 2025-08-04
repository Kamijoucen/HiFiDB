package kv

import (
	"errors"
)

// 索引类型定义
type IndexType = uint8

const (
	// BTree index
	BTree IndexType = iota + 1

	// ART ART index
	ART
)

// IO类型定义
type IOType = uint8

const (
	IO_FILE IOType = iota + 1
	IO_MMAP
)

// Options 数据库配置选项
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

// CheckOptions 检查配置选项是否有效
func CheckOptions(options *Options) error {
	if options.DirPath == "" {
		return errors.New("database dir path is empty")
	}
	if options.DataFileSize <= 0 {
		return errors.New("database data file size is invalid")
	}
	return nil
}

// GetDBDefaultOptions 获取默认数据库配置
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

// IteratorOptions 迭代器选项
type IteratorOptions struct {
	Prefix  []byte // 遍历的key前缀
	Reverse bool   // 是否逆序遍历
}

// WriteBatchOptions 写批量操作选项
type WriteBatchOptions struct {
	MaxBatchSize   int  // 最大批量大小
	EachSyncWrites bool // 每次写操作是否同步
}

func GetDefaultWriteBatchOptions() *WriteBatchOptions {
	return &WriteBatchOptions{
		MaxBatchSize:   10000,
		EachSyncWrites: true,
	}
}

func GetDefaultIteratorOptions() *IteratorOptions {
	return &IteratorOptions{
		Prefix:  nil,
		Reverse: false,
	}
}
