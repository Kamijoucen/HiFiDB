package entity

const (
	SST_FILE_NEXT_ID  = "sst_file_next_id"
	META_FILE_NEXT_ID = "meta_file_next_id"
)

// node flag
const (
	NEXT_SST_FILE_ID_NODE = uint8(iota)
	NEXT_META_FILE_ID_NODE
	SST_META_NODE
	DELETE_SST_FILE
)
