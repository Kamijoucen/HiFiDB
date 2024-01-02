package kv

import (
	"encoding/binary"

	"github.com/kamijoucen/hifidb/common"
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

func MemTableToSSTable(memTable common.SortTable[[]byte, *memValue]) []*DataItem {
	items := make([]*DataItem, 0)
	iter := memTable.Iter()
	for iter.HasNext() {
		value := iter.Next()
		// TODO value type 没有写入
		items = append(items, &DataItem{Key: value.First, Value: value.Second.Value})
	}
	return items
}
