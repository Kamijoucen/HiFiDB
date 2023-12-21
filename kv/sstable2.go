package kv

// |----------------------------|
// |          key size			|
// |----------------------------|
// |          key data          |
// |----------------------------|
// |          value size        |
// |----------------------------|
// |          value data        |
// |----------------------------|

type ssItem struct {
	Key   []byte
	Value []byte
}

type SSTable2 struct {
	data []*ssItem
}
