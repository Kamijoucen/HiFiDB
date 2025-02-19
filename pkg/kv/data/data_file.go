package data

import (
	"github.com/kamijoucen/hifidb/pkg/kv/fio"
)

type DataFile struct {
	FileId      uint32
	WriteOffset int64
	IoManager   fio.IOManager
}

func OpenDataFile(dirPath string, fileId uint32) (*DataFile, error) {
	return nil, nil
}

func (d *DataFile) Sync() error {
	return nil
}

func (d *DataFile) Write(b []byte) error {
	return nil
}

func (d *DataFile) ReadAt(off int64) ([]byte, error) {
	return nil, nil
}
