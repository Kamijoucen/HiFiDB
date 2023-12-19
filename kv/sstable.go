package kv

// 压缩类型
const (
	// 不压缩
	COMP_TYPE_NONE = uint8(iota)
	// 压缩
	COMP_TYPE_ZLIB
)

// 数据块类型
const (
	// 普通数据
	DATA_BLOCK_TYPE_DATA = uint8(iota)
	// 删除数据
	DATA_BLOCK_TYPE_DELETE
)

const (
	DATA_BLOCK_ID_SIZE     = 128
	DATA_BLOCK_LENGTH_SIZE = 8
	SS_TABLE_META_SIZE     = 276
	SS_NODE_META_SIZE      = 138
	SS_TABLE_INDEX_PAIR    = 136
)

// sstable
type SSTable struct {
	MetaData  *SSTableMeta
	IndexData *SSTableIndex
	Nodes     []*SSTableNode
}

type SSTableMeta struct {
	Size      uint64 // 8 bytes
	BeginId   string // 128 bytes
	EndId     string // 128 bytes
	BlockSize uint32 // 4 bytes sstable中数据块的数量
	Timestamp int64  // 8 bytes all 276 bytes
}

// 有序的键值对
type SSTableIndex struct {
	Size  uint64            // 8 bytes
	Index map[string]uint64 // key 128 bytes value 8 bytes, one index all 136 bytes
}

// data block
type SSTableNode struct {
	MetaData SSNodeMeta
	Value    string
}

type SSNodeMeta struct {
	Id       string // 128 bytes
	DataType uint8  // 1 byte
	CompType uint8  // 1 byte
	Size     uint64 // 8 bytes all 138 bytes
}

// TODO 通过new新建table
