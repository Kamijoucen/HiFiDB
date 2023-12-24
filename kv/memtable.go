package kv

import "sync"

type memTableManager struct {
	lock       sync.RWMutex
	walManager *walManager
	sstManager *sstManager
	sortTable  SortTable[[]byte, *memValue]
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
		sstManager: NewSstManager(),
		size:       0,
	}
}

func size(key []byte, val *memValue) uint64 {
	// key + value + 1
	return uint64(len(key)) + 1 + uint64(len(val.Value))
}

// TODO
// 按道理来说，add和update在lsm结构中都是insert，因为合并阶段会删除老数据
// 可以考虑做一个update语义操作，用来更新部分数据，并且在合并时填充其他数据
func (m *memTableManager) Add(key []byte, value []byte) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	// item size
	m.size += size(key, &memValue{NORMAL_VALUE, value})
	if err := m.sortTable.Add(key, &memValue{NORMAL_VALUE, value}); err != nil {
		return err
	}
	// TODO check memTable size
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
