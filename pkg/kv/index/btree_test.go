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

func TestBTree_Iterator(t *testing.T) {
	bt1 := NewBTreeIndex()
	// 1.BTree 为空的情况
	iter1 := bt1.Iterator(false)
	assert.Equal(t, false, iter1.Valid())

	//	2.BTree 有数据的情况
	bt1.Put([]byte("ccde"), &data.LogRecordPos{Fid: 1, Offset: 10})
	iter2 := bt1.Iterator(false)
	assert.Equal(t, true, iter2.Valid())
	assert.NotNil(t, iter2.Key())
	assert.NotNil(t, iter2.Value())
	iter2.Next()
	assert.Equal(t, false, iter2.Valid())

	// 3.有多条数据
	bt1.Put([]byte("acee"), &data.LogRecordPos{Fid: 1, Offset: 10})
	bt1.Put([]byte("eede"), &data.LogRecordPos{Fid: 1, Offset: 10})
	bt1.Put([]byte("bbcd"), &data.LogRecordPos{Fid: 1, Offset: 10})
	iter3 := bt1.Iterator(false)
	for iter3.Rewind(); iter3.Valid(); iter3.Next() {
		assert.NotNil(t, iter3.Key())
	}

	iter4 := bt1.Iterator(true)
	for iter4.Rewind(); iter4.Valid(); iter4.Next() {
		assert.NotNil(t, iter4.Key())
	}

	// 4.测试 seek
	iter5 := bt1.Iterator(false)
	for iter5.Seek([]byte("cc")); iter5.Valid(); iter5.Next() {
		assert.NotNil(t, iter5.Key())
	}

	// 5.反向遍历的 seek
	iter6 := bt1.Iterator(true)
	for iter6.Seek([]byte("zz")); iter6.Valid(); iter6.Next() {
		assert.NotNil(t, iter6.Key())
	}
}
