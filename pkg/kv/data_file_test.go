package kv

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOpenDataFile(t *testing.T) {
	df1, err := OpenDataFile(IO_FILE, os.TempDir(), 1)
	assert.Nil(t, err)
	assert.NotNil(t, df1)

	df2, err := OpenDataFile(IO_FILE, os.TempDir(), 2)
	assert.Nil(t, err)
	assert.NotNil(t, df2)

	// 重复打开
	df3, err := OpenDataFile(IO_FILE, os.TempDir(), 2)
	assert.Nil(t, err)
	assert.NotNil(t, df3)
}

func TestDataFile_Write(t *testing.T) {
	df, err := OpenDataFile(IO_FILE, os.TempDir(), 1)
	assert.Nil(t, err)
	assert.NotNil(t, df)

	err = df.Write([]byte("hello"))
	assert.Nil(t, err)

	err = df.Write([]byte("world"))
	assert.Nil(t, err)
}

func TestDataFile_Sync(t *testing.T) {
	df, err := OpenDataFile(IO_FILE, os.TempDir(), 1)
	assert.Nil(t, err)
	assert.NotNil(t, df)

	err = df.Write([]byte("hello"))
	assert.Nil(t, err)

	err = df.Sync()
	assert.Nil(t, err)
}

func TestDataFile_Close(t *testing.T) {
	df, err := OpenDataFile(IO_FILE, os.TempDir(), 1)
	assert.Nil(t, err)
	assert.NotNil(t, df)

	err = df.Write([]byte("hello"))
	assert.Nil(t, err)

	err = df.Close()
	assert.Nil(t, err)
}

func TestDataFile_ReadLogRecord(t *testing.T) {
	dataFile, err := OpenDataFile(IO_FILE, os.TempDir(), 6666)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	// 只有一条 LogRecord
	rec1 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("bitcask kv go"),
	}
	res1, size1 := EncodeLogRecord(rec1)
	err = dataFile.Write(res1)
	assert.Nil(t, err)

	readRec1, readSize1, err := dataFile.ReadLogRecord(0)
	assert.Nil(t, err)
	assert.Equal(t, rec1, readRec1)
	assert.Equal(t, size1, readSize1)

	// 多条 LogRecord，从不同的位置读取
	rec2 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("a new value"),
	}
	res2, size2 := EncodeLogRecord(rec2)
	err = dataFile.Write(res2)
	assert.Nil(t, err)

	readRec2, readSize2, err := dataFile.ReadLogRecord(size1)
	assert.Nil(t, err)
	assert.Equal(t, rec2, readRec2)
	assert.Equal(t, size2, readSize2)

	// 被删除的数据在数据文件的末尾
	rec3 := &LogRecord{
		Key:   []byte("1"),
		Value: []byte(""),
		Type:  LogRecordDeleted,
	}
	res3, size3 := EncodeLogRecord(rec3)
	err = dataFile.Write(res3)
	assert.Nil(t, err)

	readRec3, readSize3, err := dataFile.ReadLogRecord(size1 + size2)
	assert.Nil(t, err)
	assert.Equal(t, rec3, readRec3)
	assert.Equal(t, size3, readSize3)
}
