package cfg

type IndexType = uint8

const (
	// BTree BTree index
	BTree IndexType = iota + 1

	// ART ART index
	ART

	// BPTree B+Tree index
	BPTree
)
