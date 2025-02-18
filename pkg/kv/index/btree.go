package index

import (
	"sync"

	"github.com/google/btree"

	"github.com/kamijoucen/hifidb/pkg/kv/data"
)

type BTreeIndex struct {
	tree *btree.BTree
	lock *sync.RWMutex
}

func NewBTreeIndex() *BTreeIndex {
	return &BTreeIndex{
		tree: btree.New(32),
		lock: &sync.RWMutex{},
	}
}

func (b *BTreeIndex) Put(key []byte, value *data.LogRecordPos) bool {
	it := &Item{
		key: key,
		pos: value,
	}
	b.lock.Lock()
	b.tree.ReplaceOrInsert(it)
	b.lock.Unlock()
	return true
}

func (b *BTreeIndex) Get(key []byte) *data.LogRecordPos {
	it := &Item{
		key: key,
	}
	b.lock.RLock()
	bi := b.tree.Get(it)
	b.lock.RUnlock()
	if bi == nil {
		return nil
	}
	return bi.(*Item).pos
}

func (b *BTreeIndex) Delete(key []byte) bool {
	it := &Item{
		key: key,
	}
	b.lock.Lock()
	oldItem := b.tree.Delete(it)
	b.lock.Unlock()
	return oldItem != nil
}
