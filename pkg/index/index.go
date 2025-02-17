package index

import (
	"bytes"

	"github.com/google/btree"
	"github.com/kamijoucen/hifidb/pkg/data"
)

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
