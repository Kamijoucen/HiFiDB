package kv

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestNewBPlusTree_Put 测试 BPlusTree 的 Put 方法，验证是否可以正确插入键值对。
// 包括多个键值对的插入。
func TestNewBPlusTree_Put(t *testing.T) {
	path := filepath.Join("tmp")
	// Create a temporary directory for testing
	if err := os.MkdirAll(path, 0755); err != nil {
		t.Fatalf("failed to create test directory: %v", err)
	}
	defer func() {
		if err := os.RemoveAll(path); err != nil {
			t.Errorf("failed to remove test directory: %v", err)
		}
	}()
	tree := NewBPlusTree(path, true)
	defer func() {
		if err := tree.Close(); err != nil {
			t.Errorf("failed to close BPlusTree: %v", err)
		}
	}()

	assert.NotNil(t, tree, "BPlusTree should not be nil")

	tree.Put([]byte("key1"), &LogRecordPos{1, 11})
	tree.Put([]byte("key2"), &LogRecordPos{2, 22})
	tree.Put([]byte("key3"), &LogRecordPos{3, 33})
}

// TestBPlusTree_Get 测试 BPlusTree 的 Get 方法，验证是否可以正确获取已插入的键值对。
// 包括获取存在的键值对和不存在的键值对。
func TestBPlusTree_Get(t *testing.T) {
	path := filepath.Join("tmp")
	if err := os.MkdirAll(path, 0755); err != nil {
		t.Fatalf("failed to create test directory: %v", err)
	}
	defer func() {
		if err := os.RemoveAll(path); err != nil {
			t.Errorf("failed to remove test directory: %v", err)
		}
	}()
	tree := NewBPlusTree(path, true)
	defer func() {
		if err := tree.Close(); err != nil {
			t.Errorf("failed to close BPlusTree: %v", err)
		}
	}()

	tree.Put([]byte("key1"), &LogRecordPos{1, 11})
	tree.Put([]byte("key2"), &LogRecordPos{2, 22})

	pos := tree.Get([]byte("key1"))
	assert.NotNil(t, pos)
	assert.Equal(t, &LogRecordPos{1, 11}, pos)

	pos = tree.Get([]byte("key2"))
	assert.NotNil(t, pos)
	assert.Equal(t, &LogRecordPos{2, 22}, pos)

	pos = tree.Get([]byte("key3"))
	assert.Nil(t, pos)
}

// TestBPlusTree_Delete 测试 BPlusTree 的 Delete 方法，验证是否可以正确删除键值对。
// 包括删除存在的键值对和尝试删除不存在的键值对。
func TestBPlusTree_Delete(t *testing.T) {
	path := filepath.Join("tmp")
	if err := os.MkdirAll(path, 0755); err != nil {
		t.Fatalf("failed to create test directory: %v", err)
	}
	defer func() {
		if err := os.RemoveAll(path); err != nil {
			t.Errorf("failed to remove test directory: %v", err)
		}
	}()
	tree := NewBPlusTree(path, true)
	defer func() {
		if err := tree.Close(); err != nil {
			t.Errorf("failed to close BPlusTree: %v", err)
		}
	}()

	tree.Put([]byte("key1"), &LogRecordPos{1, 11})
	tree.Put([]byte("key2"), &LogRecordPos{2, 22})

	deleted := tree.Delete([]byte("key1"))
	assert.True(t, deleted)
	assert.Nil(t, tree.Get([]byte("key1")))

	deleted = tree.Delete([]byte("key3"))
	assert.False(t, deleted)
}

// TestBPlusTree_Size 测试 BPlusTree 的 Size 方法，验证是否可以正确返回当前键值对的数量。
// 包括插入、删除操作后对大小的影响。
func TestBPlusTree_Size(t *testing.T) {
	path := filepath.Join("tmp")
	if err := os.MkdirAll(path, 0755); err != nil {
		t.Fatalf("failed to create test directory: %v", err)
	}
	defer func() {
		if err := os.RemoveAll(path); err != nil {
			t.Errorf("failed to remove test directory: %v", err)
		}
	}()
	tree := NewBPlusTree(path, true)
	defer func() {
		if err := tree.Close(); err != nil {
			t.Errorf("failed to close BPlusTree: %v", err)
		}
	}()

	assert.Equal(t, 0, tree.Size())

	tree.Put([]byte("key1"), &LogRecordPos{1, 11})
	assert.Equal(t, 1, tree.Size())

	tree.Put([]byte("key2"), &LogRecordPos{2, 22})
	assert.Equal(t, 2, tree.Size())

	tree.Delete([]byte("key1"))
	assert.Equal(t, 1, tree.Size())
}

// TestBPlusTree_PutOverwrite 测试 BPlusTree 的 Put 方法覆盖场景，验证是否可以正确覆盖同一个键的值。
func TestBPlusTree_PutOverwrite(t *testing.T) {
	path := filepath.Join("tmp")
	if err := os.MkdirAll(path, 0755); err != nil {
		t.Fatalf("failed to create test directory: %v", err)
	}
	defer func() {
		if err := os.RemoveAll(path); err != nil {
			t.Errorf("failed to remove test directory: %v", err)
		}
	}()
	tree := NewBPlusTree(path, true)
	defer func() {
		if err := tree.Close(); err != nil {
			t.Errorf("failed to close BPlusTree: %v", err)
		}
	}()

	// Initial put
	tree.Put([]byte("key1"), &LogRecordPos{1, 11})
	pos := tree.Get([]byte("key1"))
	assert.NotNil(t, pos)
	assert.Equal(t, &LogRecordPos{1, 11}, pos)

	// Overwrite the same key
	tree.Put([]byte("key1"), &LogRecordPos{2, 22})
	pos = tree.Get([]byte("key1"))
	assert.NotNil(t, pos)
	assert.Equal(t, &LogRecordPos{2, 22}, pos)
}

// TestBPlusTree_DeleteAndReAdd 测试删除后重新添加相同键的场景，验证是否可以正确处理删除后重新插入的键值对。
func TestBPlusTree_DeleteAndReAdd(t *testing.T) {
	path := filepath.Join("tmp")
	if err := os.MkdirAll(path, 0755); err != nil {
		t.Fatalf("failed to create test directory: %v", err)
	}
	defer func() {
		if err := os.RemoveAll(path); err != nil {
			t.Errorf("failed to remove test directory: %v", err)
		}
	}()
	tree := NewBPlusTree(path, true)
	defer func() {
		if err := tree.Close(); err != nil {
			t.Errorf("failed to close BPlusTree: %v", err)
		}
	}()

	tree.Put([]byte("key1"), &LogRecordPos{1, 11})
	deleted := tree.Delete([]byte("key1"))
	assert.True(t, deleted)
	assert.Nil(t, tree.Get([]byte("key1")))

	tree.Put([]byte("key1"), &LogRecordPos{2, 22})
	pos := tree.Get([]byte("key1"))
	assert.NotNil(t, pos)
	assert.Equal(t, &LogRecordPos{2, 22}, pos)
}

// TestBPlusTree_RepeatedDelete 测试重复删除同一个键的场景，验证是否可以正确处理重复删除操作。
func TestBPlusTree_RepeatedDelete(t *testing.T) {
	path := filepath.Join("tmp")
	if err := os.MkdirAll(path, 0755); err != nil {
		t.Fatalf("failed to create test directory: %v", err)
	}
	defer func() {
		if err := os.RemoveAll(path); err != nil {
			t.Errorf("failed to remove test directory: %v", err)
		}
	}()
	tree := NewBPlusTree(path, true)
	defer func() {
		if err := tree.Close(); err != nil {
			t.Errorf("failed to close BPlusTree: %v", err)
		}
	}()

	tree.Put([]byte("key1"), &LogRecordPos{1, 11})
	deleted := tree.Delete([]byte("key1"))
	assert.True(t, deleted)
	assert.Nil(t, tree.Get([]byte("key1")))

	deleted = tree.Delete([]byte("key1"))
	assert.False(t, deleted)
}

// TestBPlusTree_Persistence 测试 BPlusTree 的持久化功能，验证关闭后重新打开是否可以正确加载数据。
func TestBPlusTree_Persistence(t *testing.T) {
	path := filepath.Join("tmp")
	if err := os.MkdirAll(path, 0755); err != nil {
		t.Fatalf("failed to create test directory: %v", err)
	}
	defer func() {
		if err := os.RemoveAll(path); err != nil {
			t.Errorf("failed to remove test directory: %v", err)
		}
	}()

	tree := NewBPlusTree(path, true)
	tree.Put([]byte("key1"), &LogRecordPos{1, 11})
	tree.Put([]byte("key2"), &LogRecordPos{2, 22})
	if err := tree.Close(); err != nil {
		t.Fatalf("failed to close BPlusTree: %v", err)
	}

	tree = NewBPlusTree(path, true)
	defer func() {
		if err := tree.Close(); err != nil {
			t.Errorf("failed to close BPlusTree: %v", err)
		}
	}()

	pos := tree.Get([]byte("key1"))
	assert.NotNil(t, pos)
	assert.Equal(t, &LogRecordPos{1, 11}, pos)

	pos = tree.Get([]byte("key2"))
	assert.NotNil(t, pos)
	assert.Equal(t, &LogRecordPos{2, 22}, pos)
}

// TestBPlusTree_IteratorForward 测试迭代器的正向遍历，包括正常情况和边界情况。
func TestBPlusTree_IteratorForward(t *testing.T) {
	path := filepath.Join("tmp")
	if err := os.MkdirAll(path, 0755); err != nil {
		t.Fatalf("failed to create test directory: %v", err)
	}
	defer func() {
		if err := os.RemoveAll(path); err != nil {
			t.Errorf("failed to remove test directory: %v", err)
		}
	}()
	tree := NewBPlusTree(path, true)
	defer func() {
		if err := tree.Close(); err != nil {
			t.Errorf("failed to close BPlusTree: %v", err)
		}
	}()

	// 插入多个键值对
	tree.Put([]byte("key1"), &LogRecordPos{1, 11})
	tree.Put([]byte("key2"), &LogRecordPos{2, 22})
	tree.Put([]byte("key3"), &LogRecordPos{3, 33})

	iter := tree.IndexIterator(false)
	defer iter.Close()

	// 正向遍历
	assert.True(t, iter.Valid())
	assert.Equal(t, []byte("key1"), iter.Key())
	iter.Next()
	assert.True(t, iter.Valid())
	assert.Equal(t, []byte("key2"), iter.Key())
	iter.Next()
	assert.True(t, iter.Valid())
	assert.Equal(t, []byte("key3"), iter.Key())
	iter.Next()
	assert.False(t, iter.Valid())

}

// TestBPlusTree_IteratorReverse 测试迭代器的反向遍历，包括正常情况和边界情况。
func TestBPlusTree_IteratorReverse(t *testing.T) {
	path := filepath.Join("tmp")
	if err := os.MkdirAll(path, 0755); err != nil {
		t.Fatalf("failed to create test directory: %v", err)
	}
	defer func() {
		if err := os.RemoveAll(path); err != nil {
			t.Errorf("failed to remove test directory: %v", err)
		}
	}()
	tree := NewBPlusTree(path, true)
	defer func() {
		if err := tree.Close(); err != nil {
			t.Errorf("failed to close BPlusTree: %v", err)
		}
	}()

	// 插入多个键值对
	tree.Put([]byte("key1"), &LogRecordPos{1, 11})
	tree.Put([]byte("key2"), &LogRecordPos{2, 22})
	tree.Put([]byte("key3"), &LogRecordPos{3, 33})

	iter := tree.IndexIterator(true)
	iter.Rewind()

	// 反向遍历
	assert.True(t, iter.Valid())
	assert.Equal(t, []byte("key3"), iter.Key())
	iter.Next()
	assert.True(t, iter.Valid())
	assert.Equal(t, []byte("key2"), iter.Key())
	iter.Next()
	assert.True(t, iter.Valid())
	assert.Equal(t, []byte("key1"), iter.Key())
	iter.Next()
	assert.False(t, iter.Valid())

	iter.Close()
}

// TestBPlusTree_IteratorEmpty 测试迭代器在空树上的行为。
func TestBPlusTree_IteratorEmpty(t *testing.T) {
	path := filepath.Join("tmp")
	if err := os.MkdirAll(path, 0755); err != nil {
		t.Fatalf("failed to create test directory: %v", err)
	}
	defer func() {
		if err := os.RemoveAll(path); err != nil {
			t.Errorf("failed to remove test directory: %v", err)
		}
	}()
	tree := NewBPlusTree(path, true)
	defer func() {
		if err := tree.Close(); err != nil {
			t.Errorf("failed to close BPlusTree: %v", err)
		}
	}()

	iter := tree.IndexIterator(false)
	iter.Rewind()
	assert.False(t, iter.Valid())
	iter.Close()

	revIter := tree.IndexIterator(true)
	revIter.Rewind()
	assert.False(t, revIter.Valid())
	revIter.Close()
}

// TestBPlusTree_IteratorAfterModification 测试：修改树结构后新建迭代器，验证新状态。
func TestBPlusTree_IteratorAfterModification(t *testing.T) {
	path := filepath.Join("tmp")
	os.MkdirAll(path, 0755)
	defer os.RemoveAll(path)
	tree := NewBPlusTree(path, true)
	defer tree.Close()

	tree.Put([]byte("key1"), &LogRecordPos{1, 11})
	tree.Put([]byte("key2"), &LogRecordPos{2, 22})

	// 先遍历并关闭迭代器
	iter := tree.IndexIterator(false)
	iter.Rewind()
	assert.True(t, iter.Valid())
	assert.Equal(t, []byte("key1"), iter.Key())
	iter.Next()
	assert.True(t, iter.Valid())
	assert.Equal(t, []byte("key2"), iter.Key())
	iter.Close()

	// 关闭迭代器后再修改树结构
	tree.Put([]byte("key3"), &LogRecordPos{3, 33})
	tree.Delete([]byte("key1"))

	// 新建迭代器，验证新状态
	iter2 := tree.IndexIterator(false)
	iter2.Rewind()
	assert.True(t, iter2.Valid())
	assert.Equal(t, []byte("key2"), iter2.Key())
	iter2.Next()
	assert.True(t, iter2.Valid())
	assert.Equal(t, []byte("key3"), iter2.Key())
	iter2.Next()
	assert.False(t, iter2.Valid())
	iter2.Close()
}
