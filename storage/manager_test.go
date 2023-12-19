package storage

import (
	"testing"
	"time"

	"github.com/kamijoucen/hifidb/kv"
)

func TestEnCodeSSTable(t *testing.T) {
	sst := kv.SSTable{
		MetaData: &kv.SSTableMeta{
			Size:      1,
			BeginId:   "beginid",
			EndId:     "endid",
			BlockSize: 1,
			Timestamp: time.Now().UnixNano(),
		},
		IndexData: &kv.SSTableIndex{
			Size: 1,
			Index: map[string]uint64{
				"index":  100,
				"index2": 999,
			},
		},
		Nodes: []*kv.SSTableNode{
			{
				MetaData: kv.SSNodeMeta{
					Id:       "node_meta_id",
					DataType: 1,
					CompType: 1,
					Size:     1,
				},
				Value: "node_meta_value",
			},
		},
	}

	manager := NewStorageManager()
	for i := 0; i < 1; i++ {
		err := manager.WriteSSTable(&sst)
		if err != nil {
			t.Error(err)
		}
	}
}
