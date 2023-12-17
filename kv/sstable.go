package kv

const (
	// 压缩类型
	COMP_TYPE_NONE = uint8(iota)
	COMP_TYPE_ZLIB
)

const (
	// 数据块类型
	DATA_BLOCK_TYPE_DATA = uint8(iota)
	DATA_BLOCK_TYPE_DELETE
)

// sstable
type SSTable struct {
	MetaData  SSTableMeta
	IndexData SSTableIndex
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
