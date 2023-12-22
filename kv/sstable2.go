package kv

// |-------------------------------------|
// | key size | key | value size | value |
// |-------------------------------------|

const (
	MAGIC_NUMBER = 12138
)

type DataItem struct {
	Key   []byte
	Value []byte
}

type IndexItem struct {
	Key    []byte // key
	Offset uint64 // 数据块的偏移量
}

type FooterItem struct {
	IndexOffset uint64 // 索引块的偏移量
	IndexLen    uint64 // 索引块的长度
	Magic       uint32 // 魔数
}

type SSTable2 struct {
	DataBlocks  []*DataItem
	IndexBlocks []*IndexItem
}
