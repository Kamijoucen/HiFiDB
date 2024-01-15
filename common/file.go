package common

import (
	"bufio"
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
	lock  *sync.RWMutex
	path  string
	state uint8
	f     *os.File
	buf   *bufio.Writer
}

func NewSafeFile(path string) *SafeFile {
	return &SafeFile{
		path:  path,
		state: NONE,
		lock:  &sync.RWMutex{},
	}
}

func NewSafeFileWithLock(path string, hasLock bool) *SafeFile {
	file := &SafeFile{
		path:  path,
		state: NONE,
	}
	if hasLock {
		file.lock = &sync.RWMutex{}
	} else {
		file.lock = nil
	}
	return file
}

// lock
func (sf *SafeFile) Lock() {
	sf.lock.Lock()
}

// unlock
func (sf *SafeFile) Unlock() {
	sf.lock.Unlock()
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
	sf.buf = bufio.NewWriter(f)
	sf.state = OPEN
	return nil
}

func (sf *SafeFile) Seek(offset int64, whence int) (int64, error) {
	return sf.f.Seek(offset, whence)
}

func (sf *SafeFile) ReadAt(b []byte, off int64) (n int, err error) {
	return sf.f.ReadAt(b, off)
}

func (sf *SafeFile) Read(b []byte) (n int, err error) {
	return sf.f.Read(b)
}

func (sf *SafeFile) Write(b []byte) (n int, err error) {
	return sf.buf.Write(b)
}

func (sf *SafeFile) Flush() error {
	return sf.buf.Flush()
}

func (sf *SafeFile) Close() error {
	if sf.state == CLOSE || sf.state == NONE {
		return nil
	}
	sf.state = CLOSE
	if err := sf.buf.Flush(); err != nil {
		return err
	}
	return sf.f.Close()
}

func (sf *SafeFile) IsExist() bool {
	if _, err := os.Stat(sf.path); err == nil {
		return true
	}
	return false
}

func WriteBytesAndFlush(f *SafeFile, b []byte) error {
	if _, err := f.Write(b); err != nil {
		return err
	}
	if err := f.Flush(); err != nil {
		return err
	}
	return nil
}
