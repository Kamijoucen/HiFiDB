package kv

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

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
	tree := NewBPlusTree(path)
	assert.NotNil(t, tree, "BPlusTree should not be nil")

	tree.Put([]byte("key1"), &LogRecordPos{1, 11})
	tree.Put([]byte("key2"), &LogRecordPos{2, 22})
	tree.Put([]byte("key3"), &LogRecordPos{3, 33})
}
