package kv

import (
	"bytes"
	"sort"
	"sync"

	art "github.com/plar/go-adaptive-radix-tree/v2"
)

// ArTree 自适应前缀树
type ArTree struct {
	tree art.Tree
	lock *sync.RWMutex
}

func NewArTree() *ArTree {
	return &ArTree{
		tree: art.New(),
		lock: &sync.RWMutex{},
	}
}

// Put 添加key-value，返回是否添加成功
func (a *ArTree) Put(key []byte, value *LogRecordPos) *LogRecordPos {
	a.lock.Lock()
	oldValue, _ := a.tree.Insert(key, value)
	a.lock.Unlock()
	if oldValue == nil {
		return nil
	}
	return oldValue.(*LogRecordPos)
}

// Get 获取key对应的value
func (a *ArTree) Get(key []byte) *LogRecordPos {

	a.lock.RLock()
	v, found := a.tree.Search(key)
	a.lock.RUnlock()

	if !found {
		return nil
	}
	return v.(*LogRecordPos)
}

// Delete 删除key，返回是否删除成功
func (a *ArTree) Delete(key []byte) (*LogRecordPos, bool) {
	a.lock.Lock()
	oldValue, deleted := a.tree.Delete(key)
	a.lock.Unlock()

	if oldValue == nil {
		return nil, deleted
	}
	return oldValue.(*LogRecordPos), deleted
}

// Size 获取索引大小
func (a *ArTree) Size() int {

	a.lock.RLock()
	defer a.lock.RUnlock()

	return a.tree.Size()
}

// IndexIterator 获取迭代器
func (a *ArTree) IndexIterator(reverse bool) IndexIterator {
	a.lock.RLock()
	defer a.lock.RUnlock()
	return newArTreeIterator(a.tree, reverse)
}

func (a *ArTree) Close() error {
	a.tree = nil
	a.lock = nil
	return nil
}

// BTree 索引迭代器
type arTreeIterator struct {
	curIndex int     // 当前遍历的下表位置
	reverse  bool    // 是否反向遍历
	values   []*Item // 遍历的数据
}

func newArTreeIterator(tree art.Tree, reverse bool) *arTreeIterator {
	// TODO 内存膨胀，优化
	var idx int
	if reverse {
		idx = tree.Size() - 1
	}
	values := make([]*Item, tree.Size())
	saveValues := func(node art.Node) bool {
		item := &Item{
			key: node.Key(),
			pos: node.Value().(*LogRecordPos),
		}
		values[idx] = item
		if reverse {
			idx--
		} else {
			idx++
		}
		return true
	}
	tree.ForEach(saveValues)

	return &arTreeIterator{
		curIndex: 0,
		reverse:  reverse,
		values:   values,
	}
}

// Rewind 回到起始位置
func (i *arTreeIterator) Rewind() {
	i.curIndex = 0
}

// Seek 移动第一个大于等于key的位置
func (i *arTreeIterator) Seek(key []byte) {
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
func (i *arTreeIterator) Next() {
	i.curIndex++
}

// Valid 是否有效，即是否还有下一个key，用于退出循环
func (i *arTreeIterator) Valid() bool {
	return i.curIndex < len(i.values)
}

// Key 返回当前位置key
func (i *arTreeIterator) Key() []byte {
	return i.values[i.curIndex].key
}

// Value 返回当前位置value
func (i *arTreeIterator) Value() *LogRecordPos {
	return i.values[i.curIndex].pos
}

// Close 关闭迭代器
func (i *arTreeIterator) Close() {
	i.values = nil
}
