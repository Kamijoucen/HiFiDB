package kv

import "github.com/emirpasic/gods/v2/trees/redblacktree"

type memTable struct {
	sortTable *redblacktree.Tree[string, []byte]
}

func NewMemTable() *memTable {
	return &memTable{
		sortTable: redblacktree.New[string, []byte](),
	}
}

// Get
func (m *memTable) Get(key string) ([]byte, bool) {
	if val, ok := m.sortTable.Get(key); ok {
		return val, true
	}
	return nil, false
}

// Put
func (m *memTable) Put(key string, value []byte) {
	m.sortTable.Put(key, value)
}

// Delete
func (m *memTable) Delete(key string) {
	// TODO 不会真正删除，只是标记删除
}

// Iterator
func (m *memTable) Iterator() *redblacktree.Iterator[string, []byte] {
	return m.sortTable.Iterator()
}

// Size
func (m *memTable) Size() int {
	return m.sortTable.Size()
}
