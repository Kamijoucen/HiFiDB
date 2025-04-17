package kv

import (
	"bytes"

	"github.com/google/btree"

	"github.com/kamijoucen/hifidb/pkg/cfg"
)

func NewIndex(indexType cfg.IndexType, dirPath string, syncWrites bool) Indexer {
	switch indexType {
	case cfg.BTree:
		return NewBTreeIndex()
	case cfg.ART:
		return NewArTree()
	case cfg.BPTree:
		return NewBPlusTree(dirPath, syncWrites)
	default:
		panic("unknown index type")
	}
}

type Indexer interface {

	// Put 添加key-value，返回是否添加成功
	Put(key []byte, value *LogRecordPos) bool

	// Get 获取key对应的value
	Get(key []byte) *LogRecordPos

	// Delete 删除key，返回是否删除成功
	Delete(key []byte) bool

	// Size 获取索引大小
	Size() int

	// IndexIterator 获取迭代器
	IndexIterator(reverse bool) IndexIterator

	Close() error
}

type Item struct {
	key []byte
	pos *LogRecordPos
}

func (i *Item) Less(bi btree.Item) bool {
	return bytes.Compare(i.key, bi.(*Item).key) < 0
}

// IndexIterator 索引迭代器
type IndexIterator interface {

	// Rewind 回到起始位置
	Rewind()

	// Seek 移动第一个大于等于key的位置
	Seek(key []byte)

	// Next 移动到下一个key
	Next()

	// Valid 是否有效，即是否还有下一个key，用于退出循环
	Valid() bool

	// Key 返回当前位置key
	Key() []byte

	// Value 返回当前位置value
	Value() *LogRecordPos

	// Close 关闭迭代器
	Close()
}
