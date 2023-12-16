package kv

type bstTable struct {
	Root *bstNode
}

type bstNode struct {
	Key   string
	Value []byte
	Left  *bstNode
	Right *bstNode
}

func NewBstTable() MemTable {
	return &bstTable{
		Root: nil,
	}
}

func NewBstNode(id string) MemTableNode {
	return &bstNode{
		Key: id,
	}
}

// bst node ---------------------------------------------------

func (p *bstNode) GetId() string {
	return p.Key
}

func (p *bstNode) GetLeft() MemTableNode {
	return p.Left
}

func (p *bstNode) GetRight() MemTableNode {
	return p.Right
}

func (p *bstNode) GetSize() int {
	return len(p.Value) + len(p.Key)
}

func (p *bstNode) GetValue() []byte {
	return p.Value
}

// bst table ---------------------------------------------------

func (*bstTable) Find(id string) MemTableNode {
	panic("unimplemented")
}

func (*bstTable) Insert(m MemTableNode) {
	panic("unimplemented")
}

func (*bstTable) Remove(id string) {
	panic("unimplemented")
}
