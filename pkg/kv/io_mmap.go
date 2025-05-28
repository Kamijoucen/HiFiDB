package kv

import (
	"golang.org/x/exp/mmap"
)

type MMapIO struct {
	mmap *mmap.ReaderAt
}

func NewMMapIOManager(fileName string) (*MMapIO, error) {
	mmapReader, err := mmap.Open(fileName)
	if err != nil {
		return nil, err
	}
	return &MMapIO{
		mmap: mmapReader,
	}, nil
}

func (f *MMapIO) Read(p []byte, off int64) (int, error) {
	return f.mmap.ReadAt(p, off)
}

func (f *MMapIO) Write(p []byte) (int, error) {
	panic("not implemented")
}

func (f *MMapIO) Sync() error {
	panic("not implemented")
}

func (f *MMapIO) Close() error {
	return f.mmap.Close()
}

func (f *MMapIO) Size() (int64, error) {
	return int64(f.mmap.Len()), nil
}
