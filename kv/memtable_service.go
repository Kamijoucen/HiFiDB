package kv

import (
	"sync"

	"github.com/kamijoucen/hifidb/common"
	"github.com/kamijoucen/hifidb/config"
	"github.com/kamijoucen/hifidb/kv/entity"
)

type MemTableService struct {
	lock       sync.RWMutex
	walService *WalService
	sstService *SstService
	sortTable  common.SortTable[[]byte, *MemValue]
	size       uint64
	memChannel chan common.SortTable[[]byte, *MemValue]
	done       chan bool
}
type MemValue struct {
	ValueType uint8
	Value     []byte
}

func NewMemTable(ws *WalService, ss *SstService) *MemTableService {
	mem := &MemTableService{
		walService: ws,
		sstService: ss,
		sortTable:  NewBSTTable(),
		size:       0,
		memChannel: make(chan common.SortTable[[]byte, *MemValue], 1),
		done:       make(chan bool, 1),
	}
	go mem.receiverMem()
	return mem
}

func (s *MemTableService) restSortTable() {
	temp := s.sortTable
	s.size = 0
	s.sortTable = NewBSTTable()
	s.memChannel <- temp
}

func (s *MemTableService) receiverMem() {
	for table := range s.memChannel {
		data := MemTableToSSTable(table)
		for _, item := range data {
			s.sstService.WriteData(item)
		}
	}
	s.done <- true
}

// TODO
// 按道理来说，add和update在lsm结构中都是insert，因为合并阶段会删除老数据
// 可以考虑做一个update语义操作，用来更新部分数据，并且在合并时填充其他数据
func (m *MemTableService) Add(key, value []byte) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	val := &MemValue{entity.NORMAL_VALUE, value}
	// item size
	m.size += memItemSize(key, val)

	if err := m.sortTable.Add(key, val); err != nil {
		return err
	}
	// check memTable size
	if m.size >= config.GlobalConfig.MEMTableSize {
		m.restSortTable()
	}
	return nil
}

func (m *MemTableService) Update(key, value []byte) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	return m.sortTable.Update(key, &MemValue{entity.NORMAL_VALUE, value})
}

// delete
func (m *MemTableService) Delete(key []byte) ([]byte, error) {
	m.lock.Lock()
	defer m.lock.Unlock()
	return nil, m.sortTable.Remove(key)
}

// get
func (m *MemTableService) Get(key []byte) (*MemValue, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	mv, err := m.sortTable.Get(key)
	if err != nil {
		return nil, err
	}
	return mv, nil
}

func memItemSize(key []byte, val *MemValue) uint64 {
	// key + value + 1
	return uint64(len(key)) + 1 + uint64(len(val.Value))
}

// close
func (m *MemTableService) Close() {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.restSortTable()
	close(m.memChannel)
	<-m.done
	m.sstService.Close()
}
