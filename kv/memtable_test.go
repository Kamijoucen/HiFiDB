package kv

import (
	"fmt"
	"testing"
	"time"

	"github.com/kamijoucen/hifidb/kv/entity"
)

// test
func TestMemTable(t *testing.T) {
	m := NewMemTable()
	for i := 0; i < 1000000; i++ {
		m.Add([]byte("key1"), []byte("value1"))
	}
	// wait 10s
	time.Sleep(10 * time.Second)
}

// bst test
func TestBSTTable(t *testing.T) {
	m := NewBSTTable()
	for i := 0; i < 100; i++ {
		m.Add(Uint32ToBytes(uint32(i)), &memValue{entity.NORMAL_VALUE, []byte("value1")})
	}
	// iter
	iter := m.Iter()
	for iter.Next() {
		// fmt print value convert string
		fmt.Println(BytesToUint32(iter.Key()), string(iter.Value().Value))
	}
}

// mamManager test
func TestMemTableManager(t *testing.T) {
	m := NewMemTable()
	for i := 0; i < 100000; i++ {
		m.Add(Uint32ToBytes(uint32(i)), []byte("测试测试abc"))
	}
	m.Close()
}
