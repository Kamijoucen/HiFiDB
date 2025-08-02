package cfg

type IndexType = uint8

const (
	// BTree index
	BTree IndexType = iota + 1

	// ART ART index
	ART

)

type IOType = uint8

const (
	IO_FILE IOType = iota + 1
	IO_MMAP
)
