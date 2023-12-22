package kv

import (
	"testing"
)

// write sstable test
func TestWriteSSTable(t *testing.T) {

	sm := NewSstManager()

	sst := &SSTable{
		DataBlocks: []*DataItem{
			{
				Key:   []byte("key1"),
				Value: []byte("value1"),
			},
			{
				Key:   []byte("key2"),
				Value: []byte("value2"),
			},
		},
	}

	for i := 0; i < 100; i++ {
		err := sm.WriteTable(sst)
		if err != nil {
			t.Error(err)
		}
	}
}
