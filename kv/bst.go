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
}

type bstIterator struct {
	current   *bstNode
	sortTable *bstTable
	stack     []*bstNode
}

// NewBSTTable 创建一个新的二分查找树。
func NewBSTTable() *bstTable {
	return &bstTable{}
}

// Add 将一个新的键值对添加到二分查找树中。
func (bst *bstTable) Add(key []byte, value *memValue) error {
	newNode := &bstNode{key, value, nil, nil}
	if bst.root == nil {
		bst.root = newNode
	} else {
		node := bst.root
		for {
			r := bytes.Compare(key, node.key)
			if r < 0 {
				if node.left == nil {
					node.left = newNode
					break
				}
				node = node.left
			} else if r > 0 {
				if node.right == nil {
					node.right = newNode
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

// Iter
func (bst *bstTable) Iter() common.Iterator[[]byte, *memValue] {
	iter := &bstIterator{nil, bst, make([]*bstNode, 0)}
	node := bst.root
	for node != nil {
		iter.stack = append(iter.stack, node)
		node = node.left
	}
	return iter
}

func (iter *bstIterator) Next() bool {
	if len(iter.stack) == 0 {
		return false
	}
	iter.current = iter.stack[len(iter.stack)-1]
	iter.stack = iter.stack[:len(iter.stack)-1]

	node := iter.current.right
	for node != nil {
		iter.stack = append(iter.stack, node)
		node = node.left
	}
	return true
}

func (iter *bstIterator) Key() []byte {
	return iter.current.key
}

func (iter *bstIterator) Value() *memValue {
	return iter.current.value
}
