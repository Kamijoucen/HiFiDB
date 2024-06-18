package kv

import (
	"fmt"
	"os"
	"path/filepath"
)

const (
	globalSeq = uint(iota)
)

type Meta struct {
	f      *os.File
	curSeq uint64
}

// new meta
func NewMeta(db *DB, curMetaIndex uint64) (*Meta, error) {
	fname := filepath.Join(db.path, fmt.Sprint(curMetaIndex)+".meta")
	f, err := os.OpenFile(fname, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	return &Meta{
		f: f,
	}, nil
}

// 获取全局自增ID
func (m *Meta) GetGlobalSeq() (uint64, error) {
	return 0, nil
}

// 设置全局自增
func (m *Meta) GlobalIncrement(i uint32) error {
	return nil
}

// Close
func (m *Meta) Close() error {
	return nil
}
