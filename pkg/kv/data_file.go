package kv

import (
	"fmt"
	"hash/crc32"
	"io"
	"path/filepath"

	"github.com/kamijoucen/hifidb/pkg/errs"
)

const (
	DataFileSuffix        = ".data"
	HintFileName          = "hint-index"
	MergeFinishedFileName = "merge-finished"
)

type DataFile struct {
	FileId      uint32
	WriteOffset int64
	IoManager   IOManager
}

// OpenDataFile 打开数据文件
func OpenDataFile(dirPath string, fileId uint32) (*DataFile, error) {
	fileName := GetDataFileName(dirPath, fileId)
	return newDataFile(fileName, fileId)
}

// OpenHintFile 打开hint文件
func OpenHintFile(dirPath string) (*DataFile, error) {
	fileName := filepath.Join(dirPath, HintFileName)
	return newDataFile(fileName, 0)
}

// OpenMergeFinishedFile 打开合并完成的标识文件
func OpenMergeFinishedFile(dirPath string) (*DataFile, error) {
	fileName := filepath.Join(dirPath, MergeFinishedFileName)
	return newDataFile(fileName, 0)
}

// GetDataFileName 获取数据文件名
func GetDataFileName(dirPath string, fileId uint32) string {
	return filepath.Join(dirPath, fmt.Sprintf("%010d%s", fileId, DataFileSuffix))
}

// newDataFile 创建数据文件
func newDataFile(fileName string, fileId uint32) (*DataFile, error) {
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

// Sync 同步数据
func (d *DataFile) Sync() error {
	return d.IoManager.Sync()
}

// Write 写入数据
func (d *DataFile) Write(b []byte) error {
	_, err := d.IoManager.Write(b)
	if err != nil {
		return err
	}
	d.WriteOffset += int64(len(b))
	return nil
}

// WriteHintRecord 写入hint记录
func (d *DataFile) WriteHintRecord(key []byte, pos *LogRecordPos) error {
	record := &LogRecord{
		Key:   key,
		Value: EncodeLogRecordPos(pos),
	}
	encRecord, _ := EncodeLogRecord(record)
	return d.Write(encRecord)

}

// WriteAt 写入数据
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

// readNBytes 从文件中读取n个字节
// 读取数据时，可能会发生io.EOF错误
func (d *DataFile) readNBytes(off int64, n int64) ([]byte, error) {
	// TODO 复用 buf
	bf := make([]byte, n)
	_, err := d.IoManager.Read(bf, off)
	if err != nil {
		return nil, err
	}
	return bf, nil
}
