package kv

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBTree_Put(t *testing.T) {
	bt := NewBTreeIndex()

	// 首次插入应该返回 nil（没有旧值）
	res := bt.Put([]byte("key"), &LogRecordPos{Fid: 1, Offset: 2})
	assert.Nil(t, res)

	res = bt.Put([]byte("key1"), &LogRecordPos{Fid: 2, Offset: 3})
	assert.Nil(t, res)

	// 更新已存在的 key 应该返回旧值
	oldValue := bt.Put([]byte("key"), &LogRecordPos{Fid: 10, Offset: 20})
	assert.Equal(t, &LogRecordPos{Fid: 1, Offset: 2}, oldValue)
}

func TestBTree_Put_RepeatKey(t *testing.T) {
	bt := NewBTreeIndex()

	// 首次插入
	res1 := bt.Put([]byte("key"), &LogRecordPos{Fid: 1, Offset: 100})
	assert.Nil(t, res1)

	// 第二次插入同一个 key，应该返回第一次的值
	res2 := bt.Put([]byte("key"), &LogRecordPos{Fid: 2, Offset: 200})
	assert.Equal(t, &LogRecordPos{Fid: 1, Offset: 100}, res2)

	// 第三次插入同一个 key，应该返回第二次的值
	res3 := bt.Put([]byte("key"), &LogRecordPos{Fid: 3, Offset: 300})
	assert.Equal(t, &LogRecordPos{Fid: 2, Offset: 200}, res3)

	// 验证最终的值是最后一次插入的值
	finalValue := bt.Get([]byte("key"))
	assert.Equal(t, &LogRecordPos{Fid: 3, Offset: 300}, finalValue)
}

func TestBTree_Get(t *testing.T) {
	bt := NewBTreeIndex()

	bt.Put([]byte("key"), &LogRecordPos{Fid: 1, Offset: 2})
	bt.Put([]byte("key1"), &LogRecordPos{Fid: 2, Offset: 3})

	pos := bt.Get([]byte("key"))
	assert.Equal(t, &LogRecordPos{Fid: 1, Offset: 2}, pos)

	pos = bt.Get([]byte("key1"))
	assert.Equal(t, &LogRecordPos{Fid: 2, Offset: 3}, pos)

	pos = bt.Get([]byte("key2"))
	assert.Nil(t, pos)
}

func TestBTree_Delete(t *testing.T) {
	bt := NewBTreeIndex()

	bt.Put([]byte("key"), &LogRecordPos{Fid: 1, Offset: 2})
	bt.Put([]byte("key1"), &LogRecordPos{Fid: 2, Offset: 3})

	// 删除存在的 key 应该返回被删除的值和 true
	deletedValue, success := bt.Delete([]byte("key"))
	assert.True(t, success)
	assert.Equal(t, &LogRecordPos{Fid: 1, Offset: 2}, deletedValue)

	pos := bt.Get([]byte("key"))
	assert.Nil(t, pos)

	pos = bt.Get([]byte("key1"))
	assert.Equal(t, &LogRecordPos{Fid: 2, Offset: 3}, pos)

	// 删除不存在的 key（nil 或其他不存在的 key）应该返回 nil 和 false
	deletedValue2, success2 := bt.Delete(nil)
	assert.False(t, success2)
	assert.Nil(t, deletedValue2)

	// 测试删除不存在的普通 key
	deletedValue3, success3 := bt.Delete([]byte("nonexistent"))
	assert.False(t, success3)
	assert.Nil(t, deletedValue3)
}

func TestBTree_Iterator(t *testing.T) {
	bt1 := NewBTreeIndex()
	// 1.BTree 为空的情况
	iter1 := bt1.IndexIterator(false)
	assert.Equal(t, false, iter1.Valid())

	//	2.BTree 有数据的情况
	bt1.Put([]byte("ccde"), &LogRecordPos{Fid: 1, Offset: 10})
	iter2 := bt1.IndexIterator(false)
	assert.Equal(t, true, iter2.Valid())
	assert.NotNil(t, iter2.Key())
	assert.NotNil(t, iter2.Value())
	iter2.Next()
	assert.Equal(t, false, iter2.Valid())

	// 3.有多条数据
	bt1.Put([]byte("acee"), &LogRecordPos{Fid: 1, Offset: 10})
	bt1.Put([]byte("eede"), &LogRecordPos{Fid: 1, Offset: 10})
	bt1.Put([]byte("bbcd"), &LogRecordPos{Fid: 1, Offset: 10})
	iter3 := bt1.IndexIterator(false)
	for iter3.Rewind(); iter3.Valid(); iter3.Next() {
		assert.NotNil(t, iter3.Key())
	}

	iter4 := bt1.IndexIterator(true)
	for iter4.Rewind(); iter4.Valid(); iter4.Next() {
		assert.NotNil(t, iter4.Key())
	}

	// 4.测试 seek
	iter5 := bt1.IndexIterator(false)
	for iter5.Seek([]byte("cc")); iter5.Valid(); iter5.Next() {
		assert.NotNil(t, iter5.Key())
	}

	// 5.反向遍历的 seek
	iter6 := bt1.IndexIterator(true)
	for iter6.Seek([]byte("zz")); iter6.Valid(); iter6.Next() {
		assert.NotNil(t, iter6.Key())
	}
}
