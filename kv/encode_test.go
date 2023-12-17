package kv

import (
	"testing"
)

func TestEnCodeSSTable(t *testing.T) {
	sst := SSTable{
		MetaData: SSTableMeta{
			Size:      1,
			BeginId:   "begin",
			EndId:     "end",
			BlockSize: 1,
			Timestamp: 1,
		},
		IndexData: SSTableIndex{
			Size: 1,
			Index: map[string]uint64{
				"index": 1,
			},
		},
		Nodes: []*SSTableNode{
			{
				MetaData: SSNodeMeta{
					Id:       "id",
					DataType: 1,
					CompType: 1,
					Size:     1,
				},
				Value: "value",
			},
		},
	}

	bytes, err := EnCodeSSTable(&sst)
	if err != nil {
		t.Error(err)
	}
	t.Log(bytes)
}
