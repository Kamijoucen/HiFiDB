package kv

type SSTable struct {
	Size    uint64
	BeginId string
	EndId   string
	Nodes   []SSTableNode
}

type SSTableNode struct {
	Id    string // max length 128
	Value string
}
