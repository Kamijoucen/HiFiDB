package kv

import (
	"bytes"
	"errors"

	"github.com/kamijoucen/hifidb/common"
	"github.com/kamijoucen/hifidb/kv/entity"
)

// bstTable 二分查找树的实现。
type bstTable struct {
	root *bstNode
	len  uint32
}

// bstNode 二分查找树的节点。
type bstNode struct {
	key   []byte
	value *memValue
	left  *bstNode
	right *bstNode
	next  *bstNode
	prev  *bstNode
}

// NewBSTTable 创建一个新的二分查找树。
func NewBSTTable() *bstTable {
	return &bstTable{}
}

// Add 将一个新的键值对添加到二分查找树中。
func (bst *bstTable) Add(key []byte, value *memValue) error {
	newNode := &bstNode{key, value, nil, nil, nil, nil}
	if bst.root == nil {
		bst.root = newNode
	} else {
		node := bst.root
		for {
			r := bytes.Compare(key, node.key)
			if r < 0 {
				if node.left == nil {
					node.left = newNode
					if node.prev != nil {
						node.prev.next = newNode
						newNode.prev = node.prev
					}
					newNode.next = node
					node.prev = newNode
					break
				}
				node = node.left
			} else if r > 0 {
				if node.right == nil {
					node.right = newNode
					if node.next != nil {
						node.next.prev = newNode
						newNode.next = node.next
					}
					newNode.prev = node
					node.next = newNode
					break
				}
				node = node.right
			} else {
				node.value = value
				break
			}
		}
	}
	bst.len++
	return nil
}

// Update TODO 是否要在这里写入delete flag
func (bst *bstTable) Update(key []byte, value *memValue) error {
	if bst.root == nil {
		return errors.New("bst is empty")
	}
	node := bst.root
	for {
		r := bytes.Compare(key, node.key)
		if r < 0 {
			if node.left == nil {
				return errors.New("key not found")
			}
			node = node.left
		} else if r > 0 {
			if node.right == nil {
				return errors.New("key not found")
			}
			node = node.right
		} else {
			node.value = value
			break
		}
	}
	return nil
}

// Remove
func (bst *bstTable) Remove(key []byte) error {
	return bst.Add(key, &memValue{entity.DELETE_VALUE, nil})
}

// Get
func (bst *bstTable) Get(key []byte) (*memValue, error) {
	if bst.root == nil {
		return nil, errors.New("bst is empty")
	}
	node := bst.root
	for {
		r := bytes.Compare(key, node.key)
		if r < 0 {
			if node.left == nil {
				return nil, errors.New("key not found")
			}
			node = node.left
		} else if r > 0 {
			if node.right == nil {
				return nil, errors.New("key not found")
			}
			node = node.right
		} else {
			return node.value, nil
		}
	}
}

// Len
func (bst *bstTable) Len() uint32 {
	return bst.len
}

type bstIterator struct {
	current   *bstNode
	sortTable *bstTable
}

// Iter
func (bst *bstTable) Iter() common.Iterator[*common.Tuple[[]byte, *memValue]] {
	firstNode := bst.root
	if firstNode == nil {
		return &bstIterator{nil, bst}
	}
	for firstNode.left != nil {
		firstNode = firstNode.left
	}
	begin := &bstNode{}
	begin.next = firstNode
	return &bstIterator{begin, bst}
}

func (iter *bstIterator) Next() *common.Tuple[[]byte, *memValue] {
	if iter.current == nil {
		return nil
	}
	if iter.current.next != nil {
		iter.current = iter.current.next
		return iter.Value()
	} else {
		iter.current = nil
		return nil
	}
}

// has next
func (iter *bstIterator) HasNext() bool {
	if iter.current == nil {
		return false
	}
	return iter.current.next != nil
}

func (iter *bstIterator) Value() *common.Tuple[[]byte, *memValue] {
	if iter.current == nil {
		return nil
	}
	return &common.Tuple[[]byte, *memValue]{First: iter.current.key, Second: iter.current.value}
}
