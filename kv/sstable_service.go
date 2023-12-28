package kv

import (
	"os"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/kamijoucen/hifidb/common"
	"github.com/kamijoucen/hifidb/config"
	"github.com/kamijoucen/hifidb/kv/entity"
)

// data cache
// meta cache
// level manager
type sstService struct {
	lock        sync.RWMutex
	fileCache   map[string]*common.SafeFile
	metaManager *metaService
	walManager  *walManager
	sstReceiver chan *entity.SsTable
}

func NewSstService() *sstService {
	sst := &sstService{
		fileCache:   make(map[string]*common.SafeFile),
		metaManager: NewMetaService(),
		walManager:  NewWalManager(),
		sstReceiver: make(chan *entity.SsTable, 100),
	}
	go sst.receiveSstWrite()
	return sst
}

// @Deprecated 不应该直接写入一个sst，sst是否写入应该在manager中控制
// TODO sst 文件初始化和文件写入需要分离，sst写入仅针对对应文件加锁
func (sm *sstService) WriteTable(sst *entity.SsTable) error {
	// check nil
	if sst == nil || len(sst.DataItems) == 0 {
		return nil
	}
	nId, err := sm.metaManager.NextSstId()
	if err != nil {
		return err
	}
	sstPath := filepath.Join(config.GlobalConfig.DBPath, strconv.FormatUint(nId, 10)+".sst")
	file := common.NewSafeFile(sstPath)
	if err := file.Open(os.O_RDWR | os.O_CREATE | os.O_APPEND); err != nil {
		return err
	}

	sm.lock.Lock()
	defer sm.lock.Unlock()

	sm.fileCache[sstPath] = file
	if err := writeAll(file, sst); err != nil {
		return err
	}
	return nil
}

// write all memTable to sst
func writeAll(file *common.SafeFile, sst *entity.SsTable) error {
	bytes, err := EnCodeSSTable(sst)
	if err != nil {
		return err
	}
	if _, err := file.UnsafeWrite(bytes); err != nil {
		return err
	}
	return nil
}

// write memTable to sst
func write(file *common.SafeFile, sst *entity.SsTable) error {

	// data block default 4kb
	blockBytes := make([]byte, 0, config.GlobalConfig.DBBlockSize)

	var lastItemOffset uint32 = 0
	// write data block
	for _, block := range sst.DataItems {
		lastItemOffset = uint32(len(block.Key))
		// data block
		blockBytes = append(blockBytes, Uint32ToBytes(lastItemOffset)...)
		blockBytes = append(blockBytes, block.Key...)
		blockBytes = append(blockBytes, Uint32ToBytes(uint32(len(block.Value)))...)
		blockBytes = append(blockBytes, block.Value...)

		// if data block size > 4kb, write to file
		if len(blockBytes) >= int(config.GlobalConfig.DBBlockSize) {
			if _, err := file.UnsafeWrite(blockBytes); err != nil {
				panic(err)
			}
			blockBytes = make([]byte, config.GlobalConfig.DBBlockSize)
		}
	}
	return nil
}

// 接收一个sst文件写入请求
func (sm *sstService) SendSstWrite(sst *entity.SsTable) {
	sm.sstReceiver <- sst
}

// receive sst write request
func (sm *sstService) receiveSstWrite() {
	for sst := range sm.sstReceiver {
		if err := sm.WriteTable(sst); err != nil {
			panic(err)
		}
	}
}

// Close
func (sm *sstService) Close() error {
	sm.lock.Lock()
	defer sm.lock.Unlock()
	close(sm.sstReceiver)
	for _, file := range sm.fileCache {
		if err := file.Close(); err != nil {
			return err
		}
	}
	return nil
}
