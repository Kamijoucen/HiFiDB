package kv

type MemTableNode interface {
	GetSize() int
	GetValue() []byte
	GetId() string
	GetLeft() MemTableNode
	GetRight() MemTableNode
}

type MemTable interface {
	Insert(m MemTableNode)
	Find(id string) MemTableNode
	Remove(id string)
}
