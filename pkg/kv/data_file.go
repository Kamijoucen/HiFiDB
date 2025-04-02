package kv

import (
	"fmt"
	"hash/crc32"
	"io"
	"path/filepath"

	"github.com/kamijoucen/hifidb/pkg/errs"
)

const FileSuffix = ".data"

type DataFile struct {
	FileId      uint32
	WriteOffset int64
	IoManager   IOManager
}

func OpenDataFile(dirPath string, fileId uint32) (*DataFile, error) {

	fileName := filepath.Join(dirPath, fmt.Sprintf("%010d%s", fileId, FileSuffix))
	// 打开文件
	ioManager, err := NewIOManager(fileName)
	if err != nil {
		return nil, err
	}
	return &DataFile{
		FileId:      fileId,
		WriteOffset: 0,
		IoManager:   ioManager,
	}, nil
}

func (d *DataFile) Sync() error {
	return d.IoManager.Sync()
}

func (d *DataFile) Write(b []byte) error {
	_, err := d.IoManager.Write(b)
	if err != nil {
		return err
	}
	d.WriteOffset += int64(len(b))
	return nil
}

func (d *DataFile) Close() error {
	return d.IoManager.Close()
}

// ReadLogRecord 读取日志记录
func (d *DataFile) ReadLogRecord(off int64) (*LogRecord, int64, error) {

	var fileSize, err = d.IoManager.Size()
	if err != nil {
		return nil, 0, err
	}

	// 读取头部
	var headerSize = maxLogRecordHeaderSize
	if off+headerSize > fileSize {
		headerSize = fileSize - off
	}

	// 读取头部
	headerBytes, err := d.readNBytes(off, headerSize)
	if err != nil {
		return nil, 0, err
	}

	// 解析头部
	header, headerSize := decodeLogRecordHeader(headerBytes)
	if header == nil {
		return nil, 0, io.EOF
	}

	logRecord := &LogRecord{
		Type: header.recordType,
	}

	keySize, valueSize := int64(header.keySize), int64(header.valueSize)
	if keySize > 0 || valueSize > 0 {
		// TODO 复用
		// 读取key和value
		kvBuf, err := d.readNBytes(off+headerSize, keySize+valueSize)
		if err != nil {
			return nil, 0, err
		}
		logRecord.Key = kvBuf[:keySize]
		logRecord.Value = kvBuf[keySize:]
	}
	// 校验crc
	if getLogRecordCRC(logRecord, headerBytes[crc32.Size:headerSize]) != header.crc {
		return nil, 0, errs.ErrInvalidCRC
	}
	return logRecord, headerSize + keySize + valueSize, nil
}

// readNBytes
func (d *DataFile) readNBytes(off int64, n int64) ([]byte, error) {
	// TODO 复用 buf
	bf := make([]byte, n)
	_, err := d.IoManager.Read(bf, off)
	if err != nil {
		return nil, err
	}
	return bf, nil
}
