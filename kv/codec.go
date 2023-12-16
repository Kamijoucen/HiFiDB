package kv

import (
	"encoding/binary"
	"errors"
	"sync"

	"github.com/kamijoucen/hifidb/config"
)

type result struct {
	index int
	bytes []byte
}

func EnCodeSSTable(sst SSTable) ([]byte, error) {

	if len(sst.Nodes) == 0 {
		return nil, errors.New("")
	}

	// data block header
	allBytes := make([]byte, 0, 1+config.DATA_BLOCK_LENGTH_SIZE+config.DATA_BLOCK_ID_SIZE*2)

	// delete flag
	allBytes = append(allBytes, 0)

	// all size
	b := make([]byte, config.DATA_BLOCK_LENGTH_SIZE)
	binary.BigEndian.PutUint64(b, sst.Size)
	allBytes = append(allBytes, b...)

	allBytes = append(allBytes, StrToBytes(sst.BeginId, config.DATA_BLOCK_ID_SIZE)...)
	allBytes = append(allBytes, StrToBytes(sst.EndId, config.DATA_BLOCK_ID_SIZE)...)

	resultChan := make(chan result, len(sst.Nodes))
	var wg sync.WaitGroup

	// TODO 预分配node的空间
	for i, node := range sst.Nodes {
		wg.Add(1)
		go func(i int, node SSTableNode) {
			defer wg.Done()
			resultChan <- result{i, EnCodeSSTableNode(node)}
		}(i, node)
	}

	go func() {
		wg.Wait()
		close(resultChan)
	}()

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

func DeCodeSSTable(data []byte) *SSTable {
	// TODO
	return &SSTable{}
}

func EnCodeSSTableNode(node SSTableNode) []byte {
	var allBytes []byte

	idBytes := make([]byte, 128)
	copy(idBytes, node.Id)
	allBytes = append(allBytes, idBytes...)
	// TODO

	return allBytes
}

func StrToBytes(s string, len int) []byte {
	b := make([]byte, len)
	copy(b, []byte(s))
	return b
}
