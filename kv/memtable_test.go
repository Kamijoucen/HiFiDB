package kv

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/kamijoucen/hifidb/kv/entity"
)

// bst test
func TestBSTTable(t *testing.T) {
	m := NewBSTTable()
	for i := 0; i < 1000; i++ {
		m.Add(Uint32ToBytes(uint32(i)), &memValue{entity.NORMAL_VALUE, []byte("value1")})
	}
	// iter
	iter := m.Iter()
	for iter.HasNext() {
		value := iter.Next()
		// fmt print value convert string
		fmt.Println(BytesToUint32(value.First), string(value.Second.Value))
	}
}

func TestBSTTable2(t *testing.T) {
	m := NewBSTTable()
	m.Add(Uint32ToBytes(1), &memValue{entity.NORMAL_VALUE, []byte("value1")})
	m.Add(Uint32ToBytes(3), &memValue{entity.NORMAL_VALUE, []byte("value1")})
	m.Add(Uint32ToBytes(2), &memValue{entity.NORMAL_VALUE, []byte("value1")})
	m.Add(Uint32ToBytes(9), &memValue{entity.NORMAL_VALUE, []byte("value1")})
	m.Add(Uint32ToBytes(6), &memValue{entity.NORMAL_VALUE, []byte("value1")})
	m.Add(Uint32ToBytes(4), &memValue{entity.NORMAL_VALUE, []byte("value1")})
	m.Add(Uint32ToBytes(7), &memValue{entity.NORMAL_VALUE, []byte("value1")})
	// iter
	iter := m.Iter()
	for iter.HasNext() {
		value := iter.Next()
		// fmt print value convert string
		fmt.Println(BytesToUint32(value.First), string(value.Second.Value))
	}
}

func BenchmarkAddTest(b *testing.B) {
	m := NewBSTTable()
	for i := 0; i < b.N; i++ {
		m.Add(Uint32ToBytes(uint32(i)), &memValue{entity.NORMAL_VALUE, []byte("value1")})
	}
}

func BenchmarkMemTableManager(b *testing.B) {
	m := NewMemTable()
	for i := 0; i < b.N; i++ {
		s := strconv.Itoa(i)
		m.Add([]byte(s), []byte("路上看见法拉是否"))
	}
	m.Close()
}

// mamManager test
func TestMemTableManager(t *testing.T) {
	m := NewMemTable()
	for i := 0; i < 100; i++ {
		// int to string
		s := strconv.Itoa(i)
		m.Add([]byte(s), []byte("路上看见法拉是否"))
	}
	m.Close()
}
