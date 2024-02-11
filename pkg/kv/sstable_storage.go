package kv

import (
	"os"
	"path/filepath"
	"strconv"

	"github.com/kamijoucen/hifidb/pkg/kv/common"
	"github.com/kamijoucen/hifidb/pkg/kv/entity"
)

type SsTableState struct {
	Id              uint64
	File            *common.SafeFile
	sstBytesSize    uint64
	dataBlockBuffer []byte
	blockItemOffset []uint64
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

	// 将item转换为bytes
	// TODO 这里可以复用之前的bytes
	bytes := DataItemToBytes(item)
	sst.dataBlockBuffer = append(sst.dataBlockBuffer, bytes...)
	// 更新sst文件的大小
	sst.sstBytesSize += uint64(len(bytes))
	// 记录dataBlock的偏移量
	sst.blockItemOffset = append(sst.blockItemOffset, sst.sstBytesSize)

}

func (sst *SsTableState) Close() error {
	return sst.File.Close()
}
