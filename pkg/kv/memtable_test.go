package kv

import "testing"

// 测试相同的key，不同的value
func TestMemTable_Put(t *testing.T) {
	mem := NewMemTable()
	mem.Put("key", []byte("value1"))
	mem.Put("key", []byte("value2"))
	if size := mem.Size(); size != 1 {
		t.Errorf("size = %d, want 1", size)
	}
	if val, ok := mem.Get("key"); !ok || string(val) != "value2" {
		t.Errorf("Get(key) = %q, %t, want %q, true", val, ok, "value2")
	}
}
