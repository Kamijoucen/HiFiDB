package kv

import (
	"testing"
)

func TestEnCodeSSTable(t *testing.T) {

	sst := SSTable{
		Size:    100,
		BeginId: "123",
		EndId:   "456",
		Nodes:   nil,
	}

	result := EnCodeSSTable(sst)

	t.Log(result)
}
