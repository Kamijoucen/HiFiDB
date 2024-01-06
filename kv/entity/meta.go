package entity

const (
	SST_FILE_NEXT_ID  = "sst_file_next_id"
	META_FILE_NEXT_ID = "meta_file_next_id"
)

// node flag
const (
	NEXT_SST_FILE_ID_NODE  = uint8(iota) // 0 下一个SST文件的ID
	NEXT_META_FILE_ID_NODE               // 1 下一个meta文件的ID
	SST_META_NODE                        // 2 SST文件的元数据
	DELETE_SST_FILE_NODE                 // 3 删除SST文件
	SST_LEVEL_NODE                       // 4 SST文件的level
)

// SST文件的元数据
type SSTMeta struct {
	FileId uint64    // SST文件的ID
	Level  uint64    // SST文件的level
	Range  RangePair // SST文件的范围
}

// SST文件的level
type RangePair struct {
	MinKey []byte // SST文件的最小key
	MaxKey []byte // SST文件的最大key
}
