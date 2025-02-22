package fio

const DataFilePerm = 0644

// IOManager 系统IO抽象, 用于切换文件IO, MMAP IO等
type IOManager interface {
	Read([]byte, int64) (int, error)

	Write([]byte) (int, error)

	Sync() error

	Close() error

	Size() (int64, error)
}

func NewIOManager(fileName string) (IOManager, error) {
	// TODO 目前只支持文件IO
	return NewFileIOManager(fileName)
}
