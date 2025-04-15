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

// Close 关闭索引
func (i *BPlusTree) Close() error {
	return i.tree.Close()
}

// IndexIterator 获取迭代器
func (i *BPlusTree) IndexIterator(reverse bool) IndexIterator {
	return newBpTreeIterator(i.tree, reverse)
}

// bpTreeIterator B+树迭代器
type bpTreeIterator struct {
	currentKey   []byte
	currentValue []byte
	tx           *bbolt.Tx
	cursor       *bbolt.Cursor
	reverse      bool
}

// newBpTreeIterator 创建B+树迭代器
func newBpTreeIterator(tree *bbolt.DB, reverse bool) *bpTreeIterator {
	tx, err := tree.Begin(false)
	if err != nil {
		panic(err)
	}
	bi := &bpTreeIterator{
		tx:      tx,
		cursor:  tx.Bucket(indexBucketName).Cursor(),
		reverse: reverse,
	}
	bi.Rewind()
	return bi
}

// Rewind 回到起始位置
func (bi *bpTreeIterator) Rewind() {
	if bi.reverse {
		bi.currentKey, bi.currentValue = bi.cursor.Last()
	} else {
		bi.currentKey, bi.currentValue = bi.cursor.First()
	}
}

// Seek 移动第一个大于等于key的位置
func (bi *bpTreeIterator) Seek(key []byte) {
	bi.currentKey, bi.currentValue = bi.cursor.Seek(key)
}

// Next 移动到下一个key
func (bi *bpTreeIterator) Next() {
	if bi.reverse {
		bi.currentKey, bi.currentValue = bi.cursor.Prev()
	} else {
		bi.currentKey, bi.currentValue = bi.cursor.Next()
	}
}

// Valid 是否有效，即是否还有下一个key，用于退出循环
func (bi *bpTreeIterator) Valid() bool {
	return bi.currentKey != nil
}

// Key 返回当前位置key
func (bi *bpTreeIterator) Key() []byte {
	return bi.currentKey
}

// Value 返回当前位置value
func (bi *bpTreeIterator) Value() *LogRecordPos {
	return DecodeLogRecordPos(bi.currentValue)
}

// Close 关闭迭代器
func (bi *bpTreeIterator) Close() {
	_ = bi.tx.Rollback()
}
