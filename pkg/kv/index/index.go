package index

import (
	"bytes"

	"github.com/google/btree"

	"github.com/kamijoucen/hifidb/pkg/kv/data"
)

type Type = uint8

const (
	// BTree BTree index
	BTree Type = iota + 1

	// ART ART index
	ART
)

func NewIndex(indexType Type) Indexer {
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
