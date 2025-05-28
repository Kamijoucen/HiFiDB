package kv

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIOMmap_Read(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "testfile")
	assert.NoError(t, err)
	tmpName := tmpFile.Name()
	_ = tmpFile.Close()
	defer func() {
		_ = os.Remove(tmpName)
	}()

	data := []byte("Hello HiFiDB - Read")
	// 使用普通的文件IO写入数据
	file, err := os.OpenFile(tmpName, os.O_WRONLY|os.O_CREATE, 0644)
	assert.NoError(t, err)
	_, err = file.Write(data)
	assert.NoError(t, err)
	_ = file.Close()

	fioInstance, err := NewMMapIOManager(tmpName)
	assert.NoError(t, err)
	readBuf := make([]byte, len(data))
	_, err = fioInstance.Read(readBuf, 0)
	assert.NoError(t, err)
	assert.Equal(t, data, readBuf)

	err = fioInstance.Close()
	assert.NoError(t, err)
}

func TestIOMmap_Close(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "testfile")
	assert.NoError(t, err)
	tmpName := tmpFile.Name()
	_ = tmpFile.Close()
	defer func() {
		_ = os.Remove(tmpName)
	}()

	fioInstance, err := NewMMapIOManager(tmpName)
	assert.NoError(t, err)

	err = fioInstance.Close()
	assert.NoError(t, err)
}
