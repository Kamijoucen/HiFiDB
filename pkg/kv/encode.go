package kv

import (
	"encoding/binary"

	"github.com/kamijoucen/hifidb/pkg/kv/common"
	"github.com/kamijoucen/hifidb/pkg/kv/entity"
)

func EnCodeNextId(flag uint8, nextId uint64) []byte {
	b := make([]byte, 9)
	b[0] = flag
	binary.BigEndian.PutUint64(b[1:], nextId)
	return b
}

func Uint8ToBytes(n uint8) []byte {
	return []byte{n}
}

func Uint16ToBytes(n uint16) []byte {
	b := make([]byte, 2)
	binary.BigEndian.PutUint16(b, n)
	return b
}

func Uint32ToBytes(n uint32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, n)
	return b
}

func Uint64ToBytes(n uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, n)
	return b
}

func BytesToUint64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

func BytesToUint32(b []byte) uint32 {
	return binary.BigEndian.Uint32(b)
}

func BytesToUint8(b []byte) uint8 {
	return uint8(b[0])
}

func StrToBytes(s string, len int) []byte {
	b := make([]byte, len)
	copy(b, []byte(s))
	return b
}

func MemTableToSSTable(memTable common.SortTable[[]byte, *MemValue]) []*DataItem {
	items := make([]*DataItem, 0)
	iter := memTable.Iter()
	for iter.HasNext() {
		value := iter.Next()
		// TODO value type 没有写入
		items = append(items, &DataItem{Key: value.First, Value: value.Second.Value})
	}
	return items
}

// sst meta to bytes
func SSTMetaToBytes(meta *entity.SSTMeta) []byte {
	// flag + sst file id + level + min key size + min key + max key size + max key
	byteLen := 1 + 8 + 8 + 4 + 4 + len(meta.Range.MinKey) + len(meta.Range.MaxKey)
	b := make([]byte, byteLen)
	b[0] = entity.SST_META_NODE
	binary.BigEndian.PutUint64(b[1:], meta.FileId)
	binary.BigEndian.PutUint64(b[9:], meta.Level)
	binary.BigEndian.PutUint32(b[17:], uint32(len(meta.Range.MinKey)))
	copy(b[21:], meta.Range.MinKey)
	binary.BigEndian.PutUint32(b[21+len(meta.Range.MinKey):], uint32(len(meta.Range.MaxKey)))
	copy(b[25+len(meta.Range.MinKey):], meta.Range.MaxKey)
	return b
}

// encode key value to bytes
func DataItemToBytes(item *entity.DataItem) []byte {
	// key size + key + value size + value
	byteLen := 4 + len(item.Key) + 4 + len(item.Value)
	b := make([]byte, byteLen)
	binary.BigEndian.PutUint32(b, uint32(len(item.Key)))
	copy(b[4:], item.Key)
	binary.BigEndian.PutUint32(b[4+len(item.Key):], uint32(len(item.Value)))
	copy(b[8+len(item.Key):], item.Value)
	return b
}
