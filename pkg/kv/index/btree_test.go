package index

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/kamijoucen/hifidb/pkg/kv/data"
)

func TestBTree_Put(t *testing.T) {
	bt := NewBTreeIndex()

	res := bt.Put([]byte("key"), &data.LogRecordPos{Fid: 1, Offset: 2})
	assert.True(t, res)

	res = bt.Put([]byte("key1"), &data.LogRecordPos{Fid: 2, Offset: 3})
	assert.True(t, res)
}

func TestBTree_Get(t *testing.T) {
	bt := NewBTreeIndex()

	bt.Put([]byte("key"), &data.LogRecordPos{Fid: 1, Offset: 2})
	bt.Put([]byte("key1"), &data.LogRecordPos{Fid: 2, Offset: 3})

	pos := bt.Get([]byte("key"))
	assert.Equal(t, &data.LogRecordPos{Fid: 1, Offset: 2}, pos)

	pos = bt.Get([]byte("key1"))
	assert.Equal(t, &data.LogRecordPos{Fid: 2, Offset: 3}, pos)

	pos = bt.Get([]byte("key2"))
	assert.Nil(t, pos)
}

func TestBTree_Delete(t *testing.T) {
	bt := NewBTreeIndex()

	bt.Put([]byte("key"), &data.LogRecordPos{Fid: 1, Offset: 2})
	bt.Put([]byte("key1"), &data.LogRecordPos{Fid: 2, Offset: 3})

	res := bt.Delete([]byte("key"))
	assert.True(t, res)

	pos := bt.Get([]byte("key"))
	assert.Nil(t, pos)

	pos = bt.Get([]byte("key1"))
	assert.Equal(t, &data.LogRecordPos{Fid: 2, Offset: 3}, pos)

	res = bt.Delete(nil)
	assert.False(t, res)
}
