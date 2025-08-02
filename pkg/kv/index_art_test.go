package kv

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestArTree_Put(t *testing.T) {
	art := NewArTree()

	// 首次插入应该返回 nil（没有旧值）
	res1 := art.Put([]byte("a"), &LogRecordPos{Fid: 1, Offset: 2})
	assert.Nil(t, res1)

	res2 := art.Put([]byte("b"), &LogRecordPos{Fid: 2, Offset: 3})
	assert.Nil(t, res2)

	res3 := art.Put([]byte("c"), &LogRecordPos{Fid: 3, Offset: 4})
	assert.Nil(t, res3)

	// 更新已存在的 key 应该返回旧值
	oldValue := art.Put([]byte("a"), &LogRecordPos{Fid: 10, Offset: 20})
	assert.Equal(t, &LogRecordPos{Fid: 1, Offset: 2}, oldValue)
}

func TestArTree_Put_RepeatKey(t *testing.T) {
	art := NewArTree()

	// 首次插入
	res1 := art.Put([]byte("key"), &LogRecordPos{Fid: 1, Offset: 100})
	assert.Nil(t, res1)

	// 第二次插入同一个 key，应该返回第一次的值
	res2 := art.Put([]byte("key"), &LogRecordPos{Fid: 2, Offset: 200})
	assert.Equal(t, &LogRecordPos{Fid: 1, Offset: 100}, res2)

	// 第三次插入同一个 key，应该返回第二次的值
	res3 := art.Put([]byte("key"), &LogRecordPos{Fid: 3, Offset: 300})
	assert.Equal(t, &LogRecordPos{Fid: 2, Offset: 200}, res3)

	// 验证最终的值是最后一次插入的值
	finalValue := art.Get([]byte("key"))
	assert.Equal(t, &LogRecordPos{Fid: 3, Offset: 300}, finalValue)
}

func TestArTree_Get(t *testing.T) {
	art := NewArTree()

	art.Put([]byte("a"), &LogRecordPos{Fid: 1, Offset: 2})
	art.Put([]byte("b"), &LogRecordPos{Fid: 2, Offset: 3})
	art.Put([]byte("c"), &LogRecordPos{Fid: 3, Offset: 4})

	pos1 := art.Get([]byte("a"))
	assert.Equal(t, &LogRecordPos{Fid: 1, Offset: 2}, pos1)

	pos2 := art.Get([]byte("b"))
	assert.Equal(t, &LogRecordPos{Fid: 2, Offset: 3}, pos2)

	pos3 := art.Get([]byte("c"))
	assert.Equal(t, &LogRecordPos{Fid: 3, Offset: 4}, pos3)

	pos4 := art.Get([]byte("d"))
	assert.Nil(t, pos4)
}

func TestArTree_Delete(t *testing.T) {
	art := NewArTree()

	art.Put([]byte("a"), &LogRecordPos{Fid: 1, Offset: 2})
	art.Put([]byte("b"), &LogRecordPos{Fid: 2, Offset: 3})

	// 删除存在的 key 应该返回被删除的值和 true
	deletedValue, success := art.Delete([]byte("a"))
	assert.True(t, success)
	assert.Equal(t, &LogRecordPos{Fid: 1, Offset: 2}, deletedValue)

	pos1 := art.Get([]byte("a"))
	assert.Nil(t, pos1)

	// 删除不存在的 key 应该返回 nil 和 false
	deletedValue2, success2 := art.Delete([]byte("c"))
	assert.False(t, success2)
	assert.Nil(t, deletedValue2)

	// 测试删除不存在的普通 key
	deletedValue3, success3 := art.Delete([]byte("nonexistent"))
	assert.False(t, success3)
	assert.Nil(t, deletedValue3)
}

func TestArTree_Size(t *testing.T) {
	art := NewArTree()

	assert.Equal(t, 0, art.Size())

	art.Put([]byte("a"), &LogRecordPos{Fid: 1, Offset: 2})
	assert.Equal(t, 1, art.Size())

	art.Put([]byte("b"), &LogRecordPos{Fid: 2, Offset: 3})
	assert.Equal(t, 2, art.Size())

	art.Delete([]byte("a"))
	assert.Equal(t, 1, art.Size())
}

func TestArTree_Iterator(t *testing.T) {
	art := NewArTree()

	art.Put([]byte("a"), &LogRecordPos{Fid: 1, Offset: 2})
	art.Put([]byte("b"), &LogRecordPos{Fid: 2, Offset: 3})
	art.Put([]byte("c"), &LogRecordPos{Fid: 3, Offset: 4})

	// Forward iteration
	iter := art.IndexIterator(false)
	iter.Rewind()
	assert.True(t, iter.Valid())
	assert.Equal(t, []byte("a"), iter.Key())
	iter.Next()
	assert.True(t, iter.Valid())
	assert.Equal(t, []byte("b"), iter.Key())
	iter.Next()
	assert.True(t, iter.Valid())
	assert.Equal(t, []byte("c"), iter.Key())
	iter.Next()
	assert.False(t, iter.Valid())
	iter.Close()

	// Reverse iteration
	revIter := art.IndexIterator(true)
	revIter.Rewind()
	assert.True(t, revIter.Valid())
	assert.Equal(t, []byte("c"), revIter.Key())
	revIter.Next()
	assert.True(t, revIter.Valid())
	assert.Equal(t, []byte("b"), revIter.Key())
	revIter.Next()
	assert.True(t, revIter.Valid())
	assert.Equal(t, []byte("a"), revIter.Key())
	revIter.Next()
	assert.False(t, revIter.Valid())
	revIter.Close()
}
