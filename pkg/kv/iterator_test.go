package kv

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/kamijoucen/hifidb/pkg/cfg"
)

func TestDB_NewIterator(t *testing.T) {
	opts := cfg.GetDBDefaultOptions()
	dir, _ := os.MkdirTemp("", "bitcask-go-iterator-1")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	iterator := db.NewIterator(GetDefaultIteratorOptions())
	assert.NotNil(t, iterator)
	assert.Equal(t, false, iterator.Valid())
}

func TestDB_Iterator_One_Value(t *testing.T) {
	opts := cfg.GetDBDefaultOptions()
	dir, _ := os.MkdirTemp("", "bitcask-go-iterator-2")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put(GetTestKey(10), GetTestKey(10))
	assert.Nil(t, err)

	iterator := db.NewIterator(GetDefaultIteratorOptions())
	defer iterator.Close()
	assert.NotNil(t, iterator)
	assert.Equal(t, true, iterator.Valid())
	assert.Equal(t, GetTestKey(10), iterator.Key())
	val, err := iterator.Value()
	assert.Nil(t, err)
	assert.Equal(t, GetTestKey(10), val)
}

func TestDB_Iterator_Multi_Values(t *testing.T) {
	opts := cfg.GetDBDefaultOptions()
	dir, _ := os.MkdirTemp("", "bitcask-go-iterator-3")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put([]byte("annde"), RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("cnedc"), RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("aeeue"), RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("esnue"), RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("bnede"), RandomValue(10))
	assert.Nil(t, err)

	// 正向迭代
	iter1 := db.NewIterator(GetDefaultIteratorOptions())
	for iter1.Rewind(); iter1.Valid(); iter1.Next() {
		assert.NotNil(t, iter1.Key())
	}
	iter1.Rewind()
	for iter1.Seek([]byte("c")); iter1.Valid(); iter1.Next() {
		assert.NotNil(t, iter1.Key())
	}
	iter1.Close()

	// 反向迭代
	iter2 := db.NewIterator(&IteratorOptions{
		Prefix:  nil,
		Reverse: true,
	})
	for iter2.Rewind(); iter2.Valid(); iter2.Next() {
		assert.NotNil(t, iter2.Key())
	}
	iter2.Rewind()
	for iter2.Seek([]byte("c")); iter2.Valid(); iter2.Next() {
		assert.NotNil(t, iter2.Key())
	}
	iter2.Close()

	// 指定了 prefix
	iter3 := db.NewIterator(&IteratorOptions{
		Prefix:  []byte("aee"),
		Reverse: false,
	})
	for iter3.Rewind(); iter3.Valid(); iter3.Next() {
		assert.NotNil(t, iter3.Key())
	}
	iter3.Close()
}
