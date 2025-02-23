package data

import (
	"encoding/binary"
	"hash/crc32"
)

type LogRecordType byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
)

const (
	// crc + type + keySize + valueSize
	// 4 + 1 + n + n
	maxLogRecordHeaderSize int64 = 4 + 1 + binary.MaxVarintLen32*2
)

type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType
}

type LogRecordPos struct {
	Fid    uint32
	Offset int64
}

type logRecordHeader struct {
	crc        uint32
	recordType LogRecordType
	keySize    uint32
	valueSize  uint32
}

func EncodeLogRecord(r *LogRecord) ([]byte, int64) {
	// header buf
	headerBuf := make([]byte, maxLogRecordHeaderSize)

	// 第五个第字节存储 type
	headerBuf[4] = byte(r.Type)

	var index = 5

	// 第五个直接后存储变长的keySize与valueSize
	index += binary.PutVarint(headerBuf[index:], int64(len(r.Key)))
	index += binary.PutVarint(headerBuf[index:], int64(len(r.Value)))

	// 实际长度
	var recordSize = index + len(r.Key) + len(r.Value)
	// TODO 复用
	encBytes := make([]byte, recordSize)

	// copy header, 此时header中已经包含crc的4个字节, 只是还没有计算crc
	copy(encBytes[:index], headerBuf[:index])

	// 将key和value拷贝到encBytes中
	copy(encBytes[index:], r.Key)
	copy(encBytes[index+len(r.Key):], r.Value)

	// 计算crc
	crc := crc32.ChecksumIEEE(encBytes[4:])
	// 在预留的4个字节中写入crc
	binary.LittleEndian.PutUint32(encBytes[:4], crc)

	return encBytes, int64(recordSize)
}

func DecodeLogRecord(data []byte) *LogRecord {
	return nil
}

func decodeLogRecordHeader(data []byte) (*logRecordHeader, int64) {
	if len(data) < 4 {
		return nil, 0
	}

	var crc = binary.LittleEndian.Uint32(data[:4])
	var recordType = LogRecordType(data[4])

	var index = 5

	// 取出变长的keySize与valueSize
	keySize, n := binary.Varint(data[index:])
	if n <= 0 {
		return nil, 0
	}
	index += n

	valueSize, n := binary.Varint(data[index:])
	if n <= 0 {
		return nil, 0
	}
	index += n

	logHeader := &logRecordHeader{
		crc:        crc,
		recordType: recordType,
		keySize:    uint32(keySize),
		valueSize:  uint32(valueSize),
	}
	return logHeader, int64(index)
}

func getLogRecordCRC(logRecord *LogRecord, logRecordHeaderBytes []byte) uint32 {
	if logRecord == nil {
		return 0
	}
	// TODO 复用
	checkBytes := make([]byte, len(logRecordHeaderBytes)+len(logRecord.Key)+len(logRecord.Value))
	copy(checkBytes, logRecordHeaderBytes)
	copy(checkBytes[len(logRecordHeaderBytes):], logRecord.Key)
	copy(checkBytes[len(logRecordHeaderBytes)+len(logRecord.Key):], logRecord.Value)

	return crc32.ChecksumIEEE(checkBytes)
}
