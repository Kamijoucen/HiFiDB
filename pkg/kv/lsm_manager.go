package kv

import "github.com/kamijoucen/hifidb/pkg/kv/entity"

type LsmManager struct {
	memTable    *MemTableService
	sstService  *SstService
	metaService *MetaService
	walService  *WalService
}

func NewLsmManager() *LsmManager {
	ws := NewWalService()
	ms := NewMetaService()
	ss := NewSstService(ms, ws)
	mts := NewMemTable(ws, ss)
	return &LsmManager{
		memTable:    mts,
		sstService:  ss,
		metaService: ms,
		walService:  ws,
	}
}

func (lsm *LsmManager) Get(key []byte) ([]byte, error) {

	mv, err := lsm.memTable.Get(key)
	if err != nil {
		return nil, err
	}
	if mv != nil {
		switch mv.ValueType {
		case entity.NORMAL_VALUE:
			fallthrough
		case entity.UPDATE_VALUE:
			return mv.Value, nil
		case entity.DELETE_VALUE:
			return nil, nil
		}
	}
	return nil, nil
}

func (lsm *LsmManager) Add(key []byte, value []byte) error {
	panic("not implemented") // TODO: Implement
}

func (lsm *LsmManager) Update(key []byte, value []byte) error {
	panic("not implemented") // TODO: Implement
}

func (lsm *LsmManager) Delete(key []byte) error {
	panic("not implemented") // TODO: Implement
}
