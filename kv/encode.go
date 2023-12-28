package kv

import (
	"encoding/binary"

	"github.com/kamijoucen/hifidb/common"
	"github.com/kamijoucen/hifidb/config"
	"github.com/kamijoucen/hifidb/kv/entity"
)

type indexPart struct {
	keyLen  uint32
	key     []byte
	offset  uint64
	itemLen uint32
}

// TODO 考虑改为增量写入
func EnCodeSSTable(sst *entity.SsTable) ([]byte, error) {

	allBytes := make([]byte, 0, config.GlobalConfig.SSTableSize)

	indexList := make([]*indexPart, 0, len(sst.DataItems))
	for _, block := range sst.DataItems {
		keyLen := uint32(len(block.Key))
		// data block
		allBytes = append(allBytes, Uint32ToBytes(keyLen)...)
		allBytes = append(allBytes, block.Key...)
		allBytes = append(allBytes, Uint32ToBytes(uint32(len(block.Value)))...)
		allBytes = append(allBytes, block.Value...)
		// index
		dataItemLen := 4 + keyLen + 4 + uint32(len(block.Value))
		indexList = append(indexList, &indexPart{keyLen, block.Key, uint64(len(allBytes)), dataItemLen})
	}

	indexOffset := uint64(len(allBytes))
	// index block
	for _, index := range indexList {
		allBytes = append(allBytes, Uint32ToBytes(index.keyLen)...)
		allBytes = append(allBytes, index.key...)
		allBytes = append(allBytes, Uint64ToBytes(index.offset)...)
		allBytes = append(allBytes, Uint32ToBytes(index.itemLen)...)
	}
	indexLen := uint64(len(allBytes)) - indexOffset
	// footer
	allBytes = append(allBytes, Uint64ToBytes(indexOffset)...)
	allBytes = append(allBytes, Uint64ToBytes(indexLen)...)
	allBytes = append(allBytes, Uint32ToBytes(entity.MAGIC_NUMBER)...)
	// footer allBytes: indexOffset + indexLen + magic: 8 + 8 + 4 = 20
	return allBytes, nil
}

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

func MemTableToSSTable(memTable common.SortTable[[]byte, *memValue]) *entity.SsTable {
	items := make([]*entity.DataItem, 0)
	iter := memTable.Iter()
	for iter.Next() {
		items = append(items, &entity.DataItem{Key: iter.Key(), Value: iter.Value().Value})
	}
	sst := &entity.SsTable{}
	sst.DataItems = items
	return sst
}
