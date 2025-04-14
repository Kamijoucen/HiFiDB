package kv

import (
	"path/filepath"

	"go.etcd.io/bbolt"
)

const bpTreeIndexFileName = "bptree-index"

var indexBucketName = []byte("bp-index")

type BPlusTree struct {
	tree *bbolt.DB
}

func NewBPlusTree(dirPath string) *BPlusTree {
	bptree, err := bbolt.Open(filepath.Join(dirPath, bpTreeIndexFileName), 0644, nil)
	if err != nil {
		panic(err)
	}
	err = bptree.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(indexBucketName)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		panic(err)
	}
	return &BPlusTree{
		tree: bptree,
	}
}

// Put 添加key-value，返回是否添加成功
func (i *BPlusTree) Put(key []byte, value *LogRecordPos) bool {
	err := i.tree.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		if bucket == nil {
			panic("bucket not found")
		}
		return bucket.Put(key, EncodeLogRecordPos(value))
	})
	return err == nil
}

// Get 获取key对应的value
func (i *BPlusTree) Get(key []byte) *LogRecordPos {
	var value *LogRecordPos
	err := i.tree.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		if bucket == nil {
			panic("bucket not found")
		}
		data := bucket.Get(key)
		if len(data) > 0 {
			value = DecodeLogRecordPos(data)
		}
		return nil
	})
	if err != nil {
		panic(err)
	}
	return value
}

// Delete 删除key，返回是否删除成功
func (i *BPlusTree) Delete(key []byte) bool {
	var ok bool = false
	err := i.tree.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		if value := bucket.Get(key); len(value) > 0 {
			ok = true
			return bucket.Delete(key)
		}
		return nil
	})
	if err != nil {
		panic(err)
	}
	return ok
}

// Size 获取索引大小
func (i *BPlusTree) Size() int {
	size := 0
	err := i.tree.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		if bucket == nil {
			panic("bucket not found")
		}
		size = bucket.Stats().KeyN
		return nil
	})
	if err != nil {
		panic(err)
	}
	return size
}

// IndexIterator 获取迭代器
func (i *BPlusTree) IndexIterator(reverse bool) IndexIterator {
	return nil
}
