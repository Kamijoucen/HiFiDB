package kv

import (
	"sync"

	"github.com/kamijoucen/hifidb/common"
	"github.com/kamijoucen/hifidb/config"
	"github.com/kamijoucen/hifidb/kv/entity"
)

type memTableManager struct {
	lock       sync.RWMutex
	walManager *walManager
	sstManager *SstService
	sortTable  common.SortTable[[]byte, *memValue]
	size       uint64
}

type memValue struct {
	ValueType uint8
	Value     []byte
}

func NewMemTable() *memTableManager {
	return &memTableManager{
		sortTable:  NewBSTTable(),
		walManager: NewWalManager(),
		sstManager: NewSstService(),
		size:       0,
	}
}

// TODO
// 按道理来说，add和update在lsm结构中都是insert，因为合并阶段会删除老数据
// 可以考虑做一个update语义操作，用来更新部分数据，并且在合并时填充其他数据
func (m *memTableManager) Add(key []byte, value []byte) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	val := &memValue{entity.NORMAL_VALUE, value}
	// item size
	m.size += memItemSize(key, val)

	if err := m.sortTable.Add(key, val); err != nil {
		return err
	}
	// check memTable size
	if m.size >= config.GlobalConfig.MEMTableSize {
		m.flush()
	}
	return nil
}

func (m *memTableManager) Update(key []byte, value []byte) error {
	return nil
}

// delete
func (m *memTableManager) Delete(key []byte) ([]byte, error) {
	return nil, nil
}

// get
func (m *memTableManager) Get() ([]byte, error) {
	return nil, nil
}

func memItemSize(key []byte, val *memValue) uint64 {
	// key + value + 1
	return uint64(len(key)) + 1 + uint64(len(val.Value))
}

// close
func (m *memTableManager) Close() {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.flush()
	m.sstManager.Close()
}

// flush
func (m *memTableManager) flush() {
	tempSt := m.sortTable
	m.sortTable = NewBSTTable()
	m.size = 0
	data := MemTableToSSTable(tempSt)
	for _, item := range data {
		m.sstManager.WriteData(item)
	}
}
