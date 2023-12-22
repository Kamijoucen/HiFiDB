package kv

import (
	"encoding/binary"

	"github.com/kamijoucen/hifidb/config"
)

type indexPart struct {
	keyLen uint32
	key    []byte
	offset uint64
}

// TODO 考虑改为增量写入
func EnCodeSSTable(sst *SSTable) ([]byte, error) {

	allBytes := make([]byte, 0, config.GlobalConfig.SSTableSize)

	indexSlice := make([]*indexPart, 0, len(sst.DataBlocks))
	for _, block := range sst.DataBlocks {
		keyLen := uint32(len(block.Key))
		// index
		indexSlice = append(indexSlice, &indexPart{keyLen, block.Key, uint64(len(allBytes))})
		// data block
		allBytes = append(allBytes, Uint32ToBytes(keyLen)...)
		allBytes = append(allBytes, block.Key...)
		allBytes = append(allBytes, Uint32ToBytes(uint32(len(block.Value)))...)
		allBytes = append(allBytes, block.Value...)
	}

	indexOffset := uint64(len(allBytes))
	// index block
	for _, index := range indexSlice {
		allBytes = append(allBytes, Uint32ToBytes(index.keyLen)...)
		allBytes = append(allBytes, index.key...)
		allBytes = append(allBytes, Uint64ToBytes(index.offset)...)
	}
	indexLen := uint64(len(allBytes)) - indexOffset
	// footer
	allBytes = append(allBytes, Uint64ToBytes(indexOffset)...)
	allBytes = append(allBytes, Uint64ToBytes(indexLen)...)
	allBytes = append(allBytes, Uint32ToBytes(MAGIC_NUMBER)...)
	// footer allBytes: indexOffset + indexLen + magic: 8 + 8 + 4 = 20
	return allBytes, nil
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

func StrToBytes(s string, len int) []byte {
	b := make([]byte, len)
	copy(b, []byte(s))
	return b
}
