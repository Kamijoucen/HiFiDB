package index

import (
	"bytes"

	"github.com/google/btree"

	"github.com/kamijoucen/hifidb/pkg/kv/data"
)

type IndexType = uint8

const (
	// BTree
	BTree IndexType = iota + 1

	// ART
	ART
)

func NewIndex(indexType IndexType) Indexer {
	switch indexType {
	case BTree:
		return NewBTreeIndex()
	case ART:
		return nil
	default:
		panic("unknown index type")
	}
}

type Indexer interface {
	Put(key []byte, value *data.LogRecordPos) bool
	Get(key []byte) *data.LogRecordPos
	Delete(key []byte) bool
}

type Item struct {
	key []byte
	pos *data.LogRecordPos
}

func (i *Item) Less(bi btree.Item) bool {
	return bytes.Compare(i.key, bi.(*Item).key) < 0
}
