package kv

import "os"

type Meta struct {
	f *os.File
}

// 获取全局自增ID
func (m *Meta) GetGlobalSeq() (uint64, error) {
	return 0, nil
}

// 设置全局自增
func (m *Meta) GlobalIncrement(i uint32) error {
	return nil
}
