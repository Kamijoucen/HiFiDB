package data

type LogRecordType byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDelete
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
