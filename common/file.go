package common

import (
	"bufio"
	"errors"
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
	buf   *bufio.Writer
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
	sf.buf = bufio.NewWriter(f)
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

func (sf *SafeFile) Write(b []byte) (n int, err error) {
	sf.lock.Lock()
	defer sf.lock.Unlock()
	return sf.buf.Write(b)
}

func (sf *SafeFile) UnsafeReadAt(b []byte, off int64) (n int, err error) {
	return sf.f.ReadAt(b, off)
}

func (sf *SafeFile) UnsafeRead(b []byte) (n int, err error) {
	return sf.f.Read(b)
}

func (sf *SafeFile) UnsafeWrite(b []byte) (n int, err error) {
	return sf.buf.Write(b)
}

// flush
func (sf *SafeFile) Flush() error {
	sf.lock.Lock()
	defer sf.lock.Unlock()
	if sf.state != OPEN {
		return errors.New("file not open")
	}
	return sf.buf.Flush()
}

// unsafe flush
func (sf *SafeFile) UnsafeFlush() error {
	return sf.buf.Flush()
}

func (sf *SafeFile) Close() error {
	sf.lock.Lock()
	defer sf.lock.Unlock()
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
	sf.lock.RLock()
	defer sf.lock.RUnlock()
	if _, err := os.Stat(sf.path); err == nil {
		return true
	}
	return false
}

// write bytes
func WriteBytesAndFlush(f *SafeFile, b []byte) error {
	if _, err := f.UnsafeWrite(b); err != nil {
		return err
	}
	if err := f.UnsafeFlush(); err != nil {
		return err
	}
	return nil
}
