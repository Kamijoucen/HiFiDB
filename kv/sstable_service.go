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

type tuple common.Tuple[[]byte, uint64]

// data cache
// meta cache
// level manager
type sstService struct {
	lock        sync.RWMutex
	fileCache   map[string]*common.SafeFile
	metaManager *metaService
	walManager  *walManager
	sstReceiver chan *entity.SsTable
	done        chan bool
}

func NewSstService() *sstService {
	sst := &sstService{
		fileCache:   make(map[string]*common.SafeFile),
		metaManager: NewMetaService(),
		walManager:  NewWalManager(),
		sstReceiver: make(chan *entity.SsTable, 100),
		done:        make(chan bool, 1),
	}
	go sst.receiveSstWrite()
	return sst
}

// @Deprecated 不应该直接写入一个sst，sst是否写入应该在manager中控制
// TODO sst 文件初始化和文件写入需要分离，sst写入仅针对对应文件加锁
// TODO SST的写入可以改为并发写，可以使用滑动窗口协议，保证整体的写入顺序
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
	return file.Flush()
}

// write memTable to sst
func write(file *common.SafeFile, sst *entity.SsTable) error {

	sstBytesSize := uint64(0)
	// 每个data block的最后一个key
	sstBlockLastKey := make([]*tuple, 0)

	// data block default 4kb
	blockBytes := make([]byte, 0, config.GlobalConfig.DBBlockSize)
	// data block key offset
	blockKeyOffset := make([]uint64, 0)

	// write data block
	for _, block := range sst.DataItems {

		blockKeyOffset = append(blockKeyOffset, sstBytesSize)

		keyByteSize := len(block.Key)
		valueByteSize := len(block.Value)
		// data block
		sstBytesSize += 4
		blockBytes = append(blockBytes, Uint32ToBytes(uint32(keyByteSize))...)
		sstBytesSize += uint64(keyByteSize)
		blockBytes = append(blockBytes, block.Key...)
		sstBytesSize += 4
		blockBytes = append(blockBytes, Uint32ToBytes(uint32(valueByteSize))...)
		sstBytesSize += uint64(valueByteSize)
		blockBytes = append(blockBytes, block.Value...)

		// if data block size > 4kb, write to file
		if len(blockBytes) >= int(config.GlobalConfig.DBBlockSize) {
			// generate index block and

			if _, err := file.UnsafeWrite(blockBytes); err != nil {
				panic(err)
			}
			// Record the last key of each data block, and the offset of the key in the sst
			sstBlockLastKey = append(sstBlockLastKey, &tuple{First: block.Key, Second: sstBytesSize})

			// clear block bytes
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
	sm.done <- true
}

// Close
func (sm *sstService) Close() error {
	sm.lock.Lock()
	defer sm.lock.Unlock()
	close(sm.sstReceiver)
	// wait all sst write done
	<-sm.done
	close(sm.done)
	for _, file := range sm.fileCache {
		if err := file.Close(); err != nil {
			return err
		}
	}
	return nil
}
