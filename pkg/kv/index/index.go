package index

import (
	"bytes"

	"github.com/google/btree"

	"github.com/kamijoucen/hifidb/pkg/cfg"
	"github.com/kamijoucen/hifidb/pkg/kv/data"
)

func NewIndex(indexType cfg.IndexType) Indexer {
	switch indexType {
	case cfg.BTree:
		return NewBTreeIndex()
	case cfg.ART:
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
