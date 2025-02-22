package data

import "encoding/binary"

type LogRecordType byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDelete
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
	return nil, 0
}

func DecodeLogRecord(data []byte) *LogRecord {
	return nil
}

func decodeLogRecordHeader(data []byte) (*logRecordHeader, int64) {
	return nil, 0
}

func getLogRecordCRC(logRecord *LogRecord, logRecordHeaderBytes []byte) uint32 {
	return 0
}
