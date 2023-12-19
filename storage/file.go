package storage

import (
	"os"
	"sync"
)

type safeFile struct {
	lock sync.RWMutex
	f    *os.File
}

func NewSafeFile(f *os.File) *safeFile {
	return &safeFile{
		f: f,
	}
}

func (sf *safeFile) ReadAt(b []byte, off int64) (n int, err error) {
	sf.lock.RLock()
	defer sf.lock.RUnlock()
	return sf.f.ReadAt(b, off)
}

func (sf *safeFile) WriteAt(b []byte, off int64) (n int, err error) {
	sf.lock.Lock()
	defer sf.lock.Unlock()
	return sf.f.WriteAt(b, off)
}

func (sf *safeFile) Write(b []byte) (n int, err error) {
	sf.lock.Lock()
	defer sf.lock.Unlock()
	return sf.f.Write(b)
}

func (sf *safeFile) Close() error {
	sf.lock.Lock()
	defer sf.lock.Unlock()
	return sf.f.Close()
}
