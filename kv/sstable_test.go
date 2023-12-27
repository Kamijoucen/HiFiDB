package kv

import (
	"sync"
	"testing"

	"github.com/kamijoucen/hifidb/kv/entity"
)

// write sstable test
func TestWriteSSTable(t *testing.T) {

	sm := NewSstService()

	sst := &entity.SsTable{
		DataItems: []*entity.DataItem{
			{
				Key:   []byte("11111111111111111111111111111111111111111111111111"),
				Value: []byte("value122222222222222222222222222222222222222222"),
			},
			{
				Key:   []byte("key23333333333333333333333333333333333333"),
				Value: []byte("value2444444444444444444444444444444444444444444"),
			},
		},
	}
	wg := sync.WaitGroup{}
	wg.Add(10)
	for i := 0; i < 10; i++ {
		go func() {
			err := sm.WriteTable(sst)
			if err != nil {
				t.Error(err)
			}
			wg.Done()
		}()
	}
	wg.Wait()
}
