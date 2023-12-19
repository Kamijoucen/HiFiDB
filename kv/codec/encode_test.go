package codec

import (
	"testing"

	"github.com/kamijoucen/hifidb/kv"
)

func TestEnCodeSSTable(t *testing.T) {
	sst := kv.SSTable{
		MetaData: &kv.SSTableMeta{
			Size:      100,
			BeginId:   "begin",
			EndId:     "end",
			BlockSize: 10,
			Timestamp: 11111,
		},
		IndexData: &kv.SSTableIndex{
			Size: 1,
			Index: map[string]uint64{
				"index": 1,
			},
		},
		Nodes: []*kv.SSTableNode{
			{
				MetaData: kv.SSNodeMeta{
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
