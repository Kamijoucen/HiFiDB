package common

import (
	"os"
	"sync"
)

// 文件状态
const (
	NONE = uint8(iota)
	OPEN
	CLOSE
)

type SafeFile struct {
	lock  sync.RWMutex
	path  string
	state uint8
	f     *os.File
}

func NewSafeFile(path string) *SafeFile {
	return &SafeFile{
		path:  path,
		state: NONE,
	}
}

func (sf *SafeFile) Open(flag int) error {
	sf.lock.Lock()
	defer sf.lock.Unlock()
	if sf.state == OPEN || sf.state == CLOSE {
		return nil
	}
	f, err := os.OpenFile(sf.path, flag, 0666)
	if err != nil {
		return err
	}
	sf.f = f
	sf.state = OPEN
	return nil
}

func (sf *SafeFile) ReadAt(b []byte, off int64) (n int, err error) {
	sf.lock.RLock()
	defer sf.lock.RUnlock()
	return sf.f.ReadAt(b, off)
}

func (sf *SafeFile) Read(b []byte) (n int, err error) {
	sf.lock.RLock()
	defer sf.lock.RUnlock()
	return sf.f.Read(b)
}

func (sf *SafeFile) WriteAt(b []byte, off int64) (n int, err error) {
	sf.lock.Lock()
	defer sf.lock.Unlock()
	return sf.f.WriteAt(b, off)
}

func (sf *SafeFile) Write(b []byte) (n int, err error) {
	sf.lock.Lock()
	defer sf.lock.Unlock()
	return sf.f.Write(b)
}

func (sf *SafeFile) Close() error {
	sf.lock.Lock()
	defer sf.lock.Unlock()
	if sf.state == CLOSE || sf.state == NONE {
		return nil
	}
	sf.state = CLOSE
	return sf.f.Close()
}

func (sf *SafeFile) IsExist() bool {
	sf.lock.RLock()
	defer sf.lock.RUnlock()
	if _, err := os.Stat(sf.path); err == nil {
		return true
	}
	return false
}
