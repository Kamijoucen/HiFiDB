package kv

import (
	"sync"

	"github.com/kamijoucen/hifidb/common"
	"github.com/kamijoucen/hifidb/config"
)

type memTableManager struct {
	lock       sync.RWMutex
	flushing   chan struct{}
	walManager *walManager
	sstManager *sstManager
	sortTable  common.SortTable[[]byte, *memValue]
	size       uint64
}

type memValue struct {
	ValueType uint8
	Value     []byte
}

func NewMemTable() *memTableManager {
	return &memTableManager{
		flushing:   make(chan struct{}, 1),
		sortTable:  NewBSTTable(),
		walManager: NewWalManager(),
		sstManager: NewSstManager(),
		size:       0,
	}
}

// TODO
// 按道理来说，add和update在lsm结构中都是insert，因为合并阶段会删除老数据
// 可以考虑做一个update语义操作，用来更新部分数据，并且在合并时填充其他数据
func (m *memTableManager) Add(key []byte, value []byte) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	val := &memValue{NORMAL_VALUE, value}
	// item size
	m.size += size(key, val)
	if err := m.sortTable.Add(key, val); err != nil {
		return err
	}
	// check memTable size
	if m.size >= config.GlobalConfig.SSTableSize {
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

func size(key []byte, val *memValue) uint64 {
	// key + value + 1
	return uint64(len(key)) + 1 + uint64(len(val.Value))
}

// close
func (m *memTableManager) Close() {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.flush()

	select {
	case <-m.flushing:
	default:
	}
}

// flush
func (m *memTableManager) flush() {

	m.flushing <- struct{}{}
	defer func() { <-m.flushing }()

	tempSt := m.sortTable
	m.sortTable = NewBSTTable()
	m.size = 0

	// real flush
	// TODO 这里要处理sst写入失败的情况
	go func() {
		sst, err := MemTableToSSTable(tempSt)
		if err != nil {
			panic(err)
		}
		m.sstManager.WriteTable(sst)
	}()
}
