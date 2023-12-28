package entity

const (
	MAGIC_NUMBER = uint32(121213138)
)

const (
	// 值标识
	NORMAL_VALUE = uint8(iota)
	DELETE_VALUE
	UPDATE_VALUE
)

const (
	// 数据块压缩标识
	NO_COMPRESS = uint8(iota)
	SNAPPY_COMPRESS
)

type DataBlock struct {
	Items      []*DataItem // 数据项
	ItemOffset []uint64    // 数据偏移量
	CompFlag   uint8       // 压缩标识
	Checksum   uint32      // 校验和
}

type DataItem struct {
	Key   []byte
	Value []byte
}

// TODO
// index block设计可以优化，如果一个key的value很大，那么这个key的索引就会很大
// 这里可以参考leveldb中的共享前缀算法
// 并且index item不在指向每一个key的起始位置，而是指向一个小data block的尾部
type IndexItem struct {
	Key      []byte // key
	Offset   uint64 // 数据块的偏移量
	Length   uint32 // 数据块的长度
}

type FooterItem struct {
	IndexOffset uint64 // 索引块的偏移量
	IndexLen    uint64 // 索引块的长度
	Magic       uint32 // 魔数
}

// TODO
// 目前设计有点问题，sstable中一个大DataBlock中应该分为多个小block
// 考虑给每个小datablock添加校验和
// 而 index block 的索引指向小datablock的尾数据item的起始处
type SsTable struct {
	DataItems   []*DataItem
	IndexBlocks []*IndexItem
	FooterItem  *FooterItem
}

// item size
func ItemSize(item *DataItem) uint64 {
	return uint64(len(item.Key)) + uint64(len(item.Value))
}

// append data item
func AppendDataItem(sst *SsTable, item *DataItem) {
	sst.DataItems = append(sst.DataItems, item)
}
