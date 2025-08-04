package kv

import (
	"fmt"
)

const DataFilePerm = 0644

// IOManager 系统IO抽象, 用于切换文件IO, MMAP IO等
type IOManager interface {
	Read([]byte, int64) (int, error)

	Write([]byte) (int, error)

	Sync() error

	Close() error

	Size() (int64, error)
}

func NewIOManager(indexType IOType, fileName string) (IOManager, error) {
	switch indexType {
	case IO_FILE:
		return NewFileIOManager(fileName)
	case IO_MMAP:
		return NewMMapIOManager(fileName)
	}
	return nil, fmt.Errorf("unsupported IO type: %d", indexType)
}
