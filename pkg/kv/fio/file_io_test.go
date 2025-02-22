package fio

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFileIO_Write(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "testfile")
	assert.NoError(t, err)
	tmpName := tmpFile.Name()
	tmpFile.Close()
	defer os.Remove(tmpName)

	fioInstance, err := NewFileIOManager(tmpName)
	assert.NoError(t, err)

	data := []byte("Hello HiFiDB - Write")
	n, err := fioInstance.Write(data)
	assert.NoError(t, err)
	assert.Equal(t, len(data), n)

	err = fioInstance.Close()
	assert.NoError(t, err)
}

func TestFileIO_Sync(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "testfile")
	assert.NoError(t, err)
	tmpName := tmpFile.Name()
	tmpFile.Close()
	defer os.Remove(tmpName)

	fioInstance, err := NewFileIOManager(tmpName)
	assert.NoError(t, err)

	err = fioInstance.Sync()
	assert.NoError(t, err)

	err = fioInstance.Close()
	assert.NoError(t, err)
}

func TestFileIO_Read(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "testfile")
	assert.NoError(t, err)
	tmpName := tmpFile.Name()
	tmpFile.Close()
	defer os.Remove(tmpName)

	fioInstance, err := NewFileIOManager(tmpName)
	assert.NoError(t, err)

	data := []byte("Hello HiFiDB - Read")
	n, err := fioInstance.Write(data)
	assert.NoError(t, err)
	assert.Equal(t, len(data), n)

	readBuf := make([]byte, len(data))
	_, err = fioInstance.Read(readBuf, 0)
	assert.NoError(t, err)
	assert.Equal(t, data, readBuf)

	err = fioInstance.Close()
	assert.NoError(t, err)
}

func TestFileIO_Close(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "testfile")
	assert.NoError(t, err)
	tmpName := tmpFile.Name()
	tmpFile.Close()
	defer os.Remove(tmpName)

	fioInstance, err := NewFileIOManager(tmpName)
	assert.NoError(t, err)

	err = fioInstance.Close()
	assert.NoError(t, err)
}
