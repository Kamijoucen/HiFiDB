package data

import "github.com/kamijoucen/hifidb/pkg/kv/fio"

type DataFile struct {
	FileId      uint32
	WriteOffset int64
	IoManager   fio.IOManager
}

func OpenDataFile(dirPath string, fileId uint32) *DataFile {
	return nil
}
