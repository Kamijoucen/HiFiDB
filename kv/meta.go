package kv

const (
	SST_FILE_NEXT_ID  = "sst_file_next_id"
	META_FILE_NEXT_ID = "meta_file_next_id"
)

// node flag
const (
	NEXT_SST_FILE_ID_NODE = uint8(iota)
	NEXT_META_FILE_ID_NODE
	DELETE_SST_FILE
)

type metaTable struct {
}

func NewMetaTable() *metaTable {
	return &metaTable{}
}

type NextId struct {
	Flag  uint8  // 1 byte
	Size  uint64 // 8 bytes
	Type  uint8  // 1 byte
	Value uint64 // 8
}
