package kv

import (
	"encoding/binary"
	"errors"
	"sync"
)

type result struct {
	index int
	bytes []byte
}

func EnCodeSSTable(sst *SSTable) ([]byte, error) {

	if len(sst.Nodes) == 0 {
		return nil, errors.New("SSTable.Nodes is empty")
	}
	// meta info
	allBytes := EnCodeSSTableMeta(&sst.MetaData)
	// index info
	allBytes = append(allBytes, EnCodeSSTableIndex(&sst.IndexData)...)

	resultChan := make(chan result, len(sst.Nodes))
	var wg sync.WaitGroup
	// close resultChan
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// TODO 预分配node的空间
	for i, node := range sst.Nodes {
		wg.Add(1)
		go func(i int, node *SSTableNode) {
			defer wg.Done()
			resultChan <- result{i, EnCodeSSTableNode(node)}
		}(i, node)
	}
	results := make([][]byte, len(sst.Nodes))
	for range sst.Nodes {
		r := <-resultChan
		results[r.index] = r.bytes
	}
	for _, bytes := range results {
		allBytes = append(allBytes, bytes...)
	}
	return allBytes, nil
}

// convert SSTable meta info to bytes
func EnCodeSSTableMeta(meta *SSTableMeta) []byte {

	bytes := make([]byte, 0, SS_TABLE_META_SIZE)

	bytes = append(bytes, Uint64ToBytes(meta.Size)...)
	// block start and end id
	bytes = append(bytes, StrToBytes(meta.BeginId, DATA_BLOCK_ID_SIZE)...)
	bytes = append(bytes, StrToBytes(meta.EndId, DATA_BLOCK_ID_SIZE)...)
	bytes = append(bytes, Uint32ToBytes(meta.BlockSize)...)
	bytes = append(bytes, Uint64ToBytes(uint64(meta.Timestamp))...)
	return bytes
}

// convert sstable index to bytes
func EnCodeSSTableIndex(index *SSTableIndex) []byte {

	bytes := make([]byte, 0, 8+len(index.Index)*SS_TABLE_INDEX_PAIR)
	bytes = append(bytes, Uint64ToBytes(index.Size)...)

	for k, v := range index.Index {
		bytes = append(bytes, StrToBytes(k, DATA_BLOCK_ID_SIZE)...)
		bytes = append(bytes, Uint64ToBytes(v)...)
	}
	return bytes
}

// convert SSTable node to bytes
func EnCodeSSTableNode(node *SSTableNode) []byte {
	bytes := EnCodeSSTableNodeMeta(&node.MetaData)
	bytes = append(bytes, StrToBytes(node.Value, len(node.Value))...)
	return bytes
}

// convert SSTable node meta info to bytes
func EnCodeSSTableNodeMeta(meta *SSNodeMeta) []byte {

	bytes := make([]byte, 0, SS_NODE_META_SIZE)

	bytes = append(bytes, StrToBytes(meta.Id, DATA_BLOCK_ID_SIZE)...)
	bytes = append(bytes, Uint8ToBytes(meta.DataType)...)
	bytes = append(bytes, Uint8ToBytes(meta.CompType)...)
	bytes = append(bytes, Uint64ToBytes(meta.Size)...)

	return bytes
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

func StrToBytes(s string, len int) []byte {
	b := make([]byte, len)
	copy(b, []byte(s))
	return b
}
