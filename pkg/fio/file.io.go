package fio

import "os"

type FileIO struct {
	fd *os.File
}

func NewFileIO(fileName string) (*FileIO, error) {
	fd, err := os.OpenFile(
		fileName,
		os.O_CREATE|os.O_RDWR|os.O_APPEND,
		DataFilePerm,
	)
	if err != nil {
		return nil, err
	}
	return &FileIO{
		fd: fd,
	}, nil
}

func (f *FileIO) Read(p []byte, off int64) (int, error) {
	return f.fd.ReadAt(p, off)
}

func (f *FileIO) Write(p []byte) (int, error) {
	return f.fd.Write(p)
}

func (f *FileIO) Sync() error {
	return f.fd.Sync()
}

func (f *FileIO) Close() error {
	return f.fd.Close()
}
