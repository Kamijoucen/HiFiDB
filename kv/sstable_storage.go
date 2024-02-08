package kv

import (
	"os"
	"path/filepath"
	"strconv"

	"github.com/kamijoucen/hifidb/common"
	"github.com/kamijoucen/hifidb/kv/entity"
)

type SsTableState struct {
	Id   uint64
	File *common.SafeFile
}

func OpenNewSsTable(nextSstId uint64, db *DB) (*SsTableState, error) {
	sstPath := filepath.Join(db.Config.DBPath, strconv.FormatUint(nextSstId, 10)+".sst")
	file := common.NewSafeFileWithLock(sstPath, false)
	err := file.Open(os.O_RDWR | os.O_CREATE | os.O_APPEND)
	if err != nil {
		return nil, err
	}
	return &SsTableState{
		Id:   nextSstId,
		File: file,
	}, nil
}

func (sst *SsTableState) WriteItem(item *entity.DataItem) {
}

func (sst *SsTableState) Close() {
	_ = sst.File.Close()
}
