package kv

import (
	"bytes"

	"github.com/kamijoucen/hifidb/pkg/errs"
)

type Iterator struct {
	indexIter IndexIterator
	db        *DB
	options   *IteratorOptions
}

// NewIterator 创建迭代器
func (db *DB) NewIterator(opts *IteratorOptions) *Iterator {
	indexIter := db.index.IndexIterator(opts.Reverse)
	return &Iterator{
		indexIter: indexIter,
		db:        db,
		options:   opts,
	}
}

// Rewind 回到起始位置
func (it *Iterator) Rewind() {
	it.indexIter.Rewind()
	it.skipToNext()
}

// Seek 移动第一个大于等于key的位置
func (it *Iterator) Seek(key []byte) {
	it.indexIter.Seek(key)
	it.skipToNext()
}

// Next 移动到下一个key
func (it *Iterator) Next() {
	it.indexIter.Next()
	it.skipToNext()
}

// Valid 是否有效，即是否还有下一个key，用于退出循环
func (it *Iterator) Valid() bool {
	return it.indexIter.Valid()
}

// Key 获取当前位置的key
func (it *Iterator) Key() []byte {
	return it.indexIter.Key()
}

// Value 获取当前key对应的value
func (it *Iterator) Value() ([]byte, error) {

	logRecordPos := it.indexIter.Value()
	if logRecordPos == nil {
		return nil, errs.ErrKeyNotFound
	}

	it.db.lock.RLock()
	defer it.db.lock.RUnlock()

	return it.db.getValueByPosition(logRecordPos)
}

// Close 关闭迭代器
func (it *Iterator) Close() {
	it.indexIter.Close()
}

// skipToNext 跳过不符合条件的key
func (it *Iterator) skipToNext() {
	prefixLen := len(it.options.Prefix)
	if prefixLen == 0 {
		return
	}
	for ; it.indexIter.Valid(); it.indexIter.Next() {
		key := it.indexIter.Key()
		if bytes.HasPrefix(key, it.options.Prefix) {
			break
		}
	}
}
