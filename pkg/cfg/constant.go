package cfg

type IndexType = uint8

const (
	// BTree index
	BTree IndexType = iota + 1

	// ART ART index
	ART

	// BPTree B+Tree index
	BPTree
)

type IOType = uint8

const (
	IO_FILE IOType = iota + 1
	IO_MMAP
)
