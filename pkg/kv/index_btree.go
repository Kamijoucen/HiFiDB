package kv

import (
	"bytes"
	"sort"
	"sync"

	"github.com/google/btree"
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

func (b *BTreeIndex) Put(key []byte, value *LogRecordPos) bool {
	it := &Item{
		key: key,
		pos: value,
	}
	b.lock.Lock()
	b.tree.ReplaceOrInsert(it)
	b.lock.Unlock()
	return true
}

func (b *BTreeIndex) Get(key []byte) *LogRecordPos {
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

func (b *BTreeIndex) Size() int {
	return b.tree.Len()
}

func (b *BTreeIndex) IndexIterator(reverse bool) IndexIterator {
	if b.tree == nil {
		return nil
	}
	b.lock.RLock()
	defer b.lock.RUnlock()

	return newBTreeIterator(b.tree, reverse)
}

func (b *BTreeIndex) Close() error {
	if b.tree != nil {
		b.tree.Clear(false)
	}
	b.tree = nil
	b.lock = nil
	return nil
}

// BTree 索引迭代器
type btreeIterator struct {
	curIndex int     // 当前遍历的下表位置
	reverse  bool    // 是否反向遍历
	values   []*Item // 遍历的数据
}

func newBTreeIterator(tree *btree.BTree, reverse bool) *btreeIterator {
	// TODO 内存膨胀，优化
	var idx int
	values := make([]*Item, tree.Len())

	sf := func(item btree.Item) bool {
		values[idx] = item.(*Item)
		idx++
		return true
	}
	if reverse {
		tree.Descend(sf)
	} else {
		tree.Ascend(sf)
	}
	return &btreeIterator{
		curIndex: 0,
		reverse:  reverse,
		values:   values,
	}
}

// Rewind 回到起始位置
func (i *btreeIterator) Rewind() {
	i.curIndex = 0
}

// Seek 移动第一个大于等于key的位置
func (i *btreeIterator) Seek(key []byte) {
	if i.reverse {
		i.curIndex = sort.Search(len(i.values), func(idx int) bool {
			return bytes.Compare(i.values[idx].key, key) <= 0
		})
	} else {
		i.curIndex = sort.Search(len(i.values), func(idx int) bool {
			return bytes.Compare(i.values[idx].key, key) >= 0
		})
	}
}

// Next 移动到下一个key
func (i *btreeIterator) Next() {
	i.curIndex++
}

// Valid 是否有效，即是否还有下一个key，用于退出循环
func (i *btreeIterator) Valid() bool {
	return i.curIndex < len(i.values)
}

// Key 返回当前位置key
func (i *btreeIterator) Key() []byte {
	return i.values[i.curIndex].key
}

// Value 返回当前位置value
func (i *btreeIterator) Value() *LogRecordPos {
	return i.values[i.curIndex].pos
}

// Close 关闭迭代器
func (i *btreeIterator) Close() {
	i.values = nil
}
