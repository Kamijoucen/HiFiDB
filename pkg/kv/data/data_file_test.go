package data

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOpenDataFile(t *testing.T) {
	df1, err := OpenDataFile(os.TempDir(), 1)
	assert.Nil(t, err)
	assert.NotNil(t, df1)

	df2, err := OpenDataFile(os.TempDir(), 2)
	assert.Nil(t, err)
	assert.NotNil(t, df2)

	// 重复打开
	df3, err := OpenDataFile(os.TempDir(), 2)
	assert.Nil(t, err)
	assert.NotNil(t, df3)
}

func TestDataFile_Write(t *testing.T) {
	df, err := OpenDataFile(os.TempDir(), 1)
	assert.Nil(t, err)
	assert.NotNil(t, df)

	err = df.Write([]byte("hello"))
	assert.Nil(t, err)

	err = df.Write([]byte("world"))
	assert.Nil(t, err)
}

func TestDataFile_Sync(t *testing.T) {
	df, err := OpenDataFile(os.TempDir(), 1)
	assert.Nil(t, err)
	assert.NotNil(t, df)

	err = df.Write([]byte("hello"))
	assert.Nil(t, err)

	err = df.Sync()
	assert.Nil(t, err)
}

func TestDataFile_Close(t *testing.T) {
	df, err := OpenDataFile(os.TempDir(), 1)
	assert.Nil(t, err)
	assert.NotNil(t, df)

	err = df.Write([]byte("hello"))
	assert.Nil(t, err)

	err = df.Close()
	assert.Nil(t, err)
}
