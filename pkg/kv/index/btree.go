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

// BTree 索引迭代器
type btreeIterator struct {
	curIndex int     // 当前遍历的下表位置
	reverse  bool    // 是否反向遍历
	values   []*Item // 遍历的数据
}

func newBTreeIterator(tree *btree.BTree, reverse bool) *btreeIterator {
	return &btreeIterator{
		curIndex: 0,
		reverse:  reverse,
		values:   make([]*Item, tree.Len()),
	}
}

// Rewind 回到起始位置
func (i *btreeIterator) Rewind() {

}

// Seek 移动第一个大于等于key的位置
func (i *btreeIterator) Seek(key []byte) {
	panic("not implemented") // TODO: Implement
}

// Next 移动到下一个key
func (i *btreeIterator) Next() {
	panic("not implemented") // TODO: Implement
}

// Valid 是否有效，即是否还有下一个key，用于退出循环
func (i *btreeIterator) Valid() (_ bool) {
	panic("not implemented") // TODO: Implement
}

// Key 返回当前位置key
func (i *btreeIterator) Key() (_ []byte) {
	panic("not implemented") // TODO: Implement
}

// Value 返回当前位置value
func (i *btreeIterator) Value() (_ *data.LogRecordPos) {
	panic("not implemented") // TODO: Implement
}

// Close 关闭迭代器
func (i *btreeIterator) Close() {
	panic("not implemented") // TODO: Implement
}
