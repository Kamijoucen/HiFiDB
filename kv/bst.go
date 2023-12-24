package kv

import (
	"bytes"
	"errors"
)

// 二分查找树
type bstTable struct {
	root *bstNode
}

type bstNode struct {
	key   []byte
	value *memValue
	left  *bstNode
	right *bstNode
}

func NewBSTTable() *bstTable {
	return &bstTable{}
}

// add
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
	return nil
}

// TODO 是否要在这里写入delete flag
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

// remove
func (bst *bstTable) Remove(key []byte) error {
	return bst.Add(key, &memValue{DELETE_VALUE, nil})
}

// get
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
