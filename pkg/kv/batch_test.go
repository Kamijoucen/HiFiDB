package kv

import (
	"os"
	"testing"

	"github.com/kamijoucen/hifidb/pkg/cfg"
	"github.com/kamijoucen/hifidb/pkg/errs"
	"github.com/stretchr/testify/assert"
)

func TestDB_WriteBatch1(t *testing.T) {
	opts := cfg.GetDBDefaultOptions()
	dir, _ := os.MkdirTemp("", "bitcask-go-batch-1")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 写数据之后并不提交
	wb := db.NewWriteBatch(GetDefaultWriteBatchOptions())
	err = wb.Put(GetTestKey(1), RandomValue(10))
	assert.Nil(t, err)
	err = wb.Delete(GetTestKey(2))
	assert.Nil(t, err)

	_, err = db.Get(GetTestKey(1))
	assert.Equal(t, errs.ErrKeyNotFound, err)

	// 正常提交数据
	err = wb.Commit()
	assert.Nil(t, err)

	val1, err := db.Get(GetTestKey(1))
	assert.NotNil(t, val1)
	assert.Nil(t, err)

	// 删除有效的数据
	wb2 := db.NewWriteBatch(GetDefaultWriteBatchOptions())
	err = wb2.Delete(GetTestKey(1))
	assert.Nil(t, err)
	err = wb2.Commit()
	assert.Nil(t, err)

	_, err = db.Get(GetTestKey(1))
	assert.Equal(t, errs.ErrKeyNotFound, err)
}

func TestDB_WriteBatch2(t *testing.T) {
	opts := cfg.GetDBDefaultOptions()
	dir, _ := os.MkdirTemp("", "bitcask-go-batch-2")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put(GetTestKey(1), RandomValue(10))
	assert.Nil(t, err)

	wb := db.NewWriteBatch(GetDefaultWriteBatchOptions())
	err = wb.Put(GetTestKey(2), RandomValue(10))
	assert.Nil(t, err)
	err = wb.Delete(GetTestKey(1))
	assert.Nil(t, err)

	err = wb.Commit()
	assert.Nil(t, err)

	err = wb.Put(GetTestKey(11), RandomValue(10))
	assert.Nil(t, err)
	err = wb.Commit()
	assert.Nil(t, err)

	// 重启
	err = db.Close()
	assert.Nil(t, err)

	db2, err := Open(opts)
	assert.Nil(t, err)

	_, err = db2.Get(GetTestKey(1))
	assert.Equal(t, errs.ErrKeyNotFound, err)

	// 校验序列号
	assert.Equal(t, uint64(2), db2.seqNo)

	err = db2.Close()
	assert.Nil(t, err)
}

//func TestDB_WriteBatch3(t *testing.T) {
//	opts := DefaultOptions
//	//dir, _ := os.MkdirTemp("", "bitcask-go-batch-3")
//	dir := "/tmp/bitcask-go-batch-3"
//	opts.DirPath = dir
//	db, err := Open(opts)
//	//defer destroyDB(db)
//	assert.Nil(t, err)
//	assert.NotNil(t, db)
//
//	keys := db.ListKeys()
//	t.Log(len(keys))
//	//
//	//wbOpts := DefaultWriteBatchOptions
//	//wbOpts.MaxBatchNum = 10000000
//	//wb := db.NewWriteBatch(wbOpts)
//	//for i := 0; i < 500000; i++ {
//	//	err := wb.Put(GetTestKey(i), RandomValue(1024))
//	//	assert.Nil(t, err)
//	//}
//	//err = wb.Commit()
//	//assert.Nil(t, err)
//}
