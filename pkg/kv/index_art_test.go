package kv

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestArTree_Put(t *testing.T) {
	art := NewArTree()

	res1 := art.Put([]byte("a"), &LogRecordPos{Fid: 1, Offset: 2})
	assert.True(t, res1)

	res2 := art.Put([]byte("b"), &LogRecordPos{Fid: 2, Offset: 3})
	assert.True(t, res2)

	res3 := art.Put([]byte("c"), &LogRecordPos{Fid: 3, Offset: 4})
	assert.True(t, res3)
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

	res1 := art.Delete([]byte("a"))
	assert.True(t, res1)

	pos1 := art.Get([]byte("a"))
	assert.Nil(t, pos1)

	res2 := art.Delete([]byte("c"))
	assert.False(t, res2)
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
