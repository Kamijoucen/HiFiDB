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

type DataItems []*entity.DataItem

type DataItem entity.DataItem

// data cache
// meta cache
// level manager
type SstService struct {
	lock            sync.RWMutex
	fileCache       *common.LRUCache[string, common.SafeFile]
	metaManager     *metaService
	walManager      *walManager
	sstReceiver     chan DataItems
	done            chan bool
	currentSstState *currentSstState
}

type currentSstState struct {
	sstBytesSize     uint64
	currentBlockSize uint32
	sstFile          *common.SafeFile
	sstBlockLastKey  []*tuple  // sst中每个data block的最后一个key
	blockItemOffset  []uint64  // 数据块中每个key的offset
	blockBytes       []byte    // 数据块
	blockLastItem    *DataItem // 数据块中最后一个item
}

func NewSstService() *SstService {
	closeFileFunc := func(s string, sf *common.SafeFile) {
		sf.Close()
	}
	sst := &SstService{
		fileCache:       common.NewLRUCacheWithRemoveCallBack[string, common.SafeFile](100, closeFileFunc),
		metaManager:     NewMetaService(),
		walManager:      NewWalManager(),
		sstReceiver:     make(chan DataItems, 100),
		done:            make(chan bool, 1),
		currentSstState: &currentSstState{},
	}
	go sst.receiveSstWrite()
	return sst
}

// @Deprecated 不应该直接写入一个sst，sst是否写入应该在manager中控制
// TODO sst 文件初始化和文件写入需要分离，sst写入仅针对对应文件加锁
// TODO SST的写入可以改为并发写，可以使用滑动窗口协议，保证整体的写入顺序
func (sm *SstService) WriteTable(dataItems DataItems) error {
	if len(dataItems) == 0 {
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
	sm.fileCache.Put(sstPath, file)
	// if _, err := sm.write(file, dataItems); err != nil {
	// 	return err
	// }
	return nil
}

// flush data block
func (sm *SstService) flushDataBlock() error {
	// TODO 如果当前数据块的大于配置的数据块大小，或者是最后一个数据项，需要将数据块写入文件
	// 数据块中索引块的起始offset
	blockIndexOffset := sm.currentSstState.sstBytesSize
	// 索引块的长度，每个索引都是unit8
	blockIndexLen := uint32(len(sm.currentSstState.blockItemOffset)) * 8
	// 向sst文件大小计数添加索引块的长度
	sm.currentSstState.sstBytesSize += uint64(blockIndexLen)
	// 将数据项的索引添加到数据块的尾部
	for _, offset := range sm.currentSstState.blockItemOffset {
		sm.currentSstState.blockBytes = append(sm.currentSstState.blockBytes, Uint64ToBytes(offset)...)
	}
	// block footer: index block offset + index block length + comp flag + checksum
	// 8 + 4 + 1 + 4 = 17 bytes
	sm.currentSstState.sstBytesSize += 17
	// 写入索引块位置 8bytes
	sm.currentSstState.blockBytes = append(sm.currentSstState.blockBytes, Uint64ToBytes(blockIndexOffset)...)
	// 写入索引块长度 4bytes
	sm.currentSstState.blockBytes = append(sm.currentSstState.blockBytes, Uint32ToBytes(blockIndexLen)...)
	// 写入压缩标识(TODO)
	sm.currentSstState.blockBytes = append(sm.currentSstState.blockBytes, Uint8ToBytes(entity.NO_COMPRESS)...)
	// 写入校验和(TODO)
	sm.currentSstState.blockBytes = append(sm.currentSstState.blockBytes, Uint32ToBytes(0)...)

	blockLastItem := sm.currentSstState.blockLastItem
	// 记录数据块最后一个字节的offset，key是当前数据块最后一个数据项
	sm.currentSstState.sstBlockLastKey = append(sm.currentSstState.sstBlockLastKey, &tuple{First: blockLastItem.Key, Second: sm.currentSstState.sstBytesSize})
	// 写入数据
	if _, err := sm.currentSstState.sstFile.UnsafeWrite(sm.currentSstState.blockBytes); err != nil {
		panic(err)
	}
	sm.resetSstBlock()
	return nil
}

// flush sst
func (sm *SstService) flushSst() error {
	file := sm.currentSstState.sstFile
	// 索引offset
	sstIndexOffset := sm.currentSstState.sstBytesSize
	// 一个索引项的结构是：key长度 + key + offset，其中offset是数据块的最后一个字节的偏移量
	// 4 + keyByteSize + 8
	for _, tuple := range sm.currentSstState.sstBlockLastKey {
		// 写入索引项的key长度
		sm.currentSstState.sstBytesSize += 4
		if _, err := file.UnsafeWrite(Uint32ToBytes(uint32(len(tuple.First)))); err != nil {
			panic(err)
		}
		// 写入索引项的key
		sm.currentSstState.sstBytesSize += uint64(len(tuple.First))
		if _, err := file.UnsafeWrite(tuple.First); err != nil {
			panic(err)
		}
		// 写入索引项的offset
		sm.currentSstState.sstBytesSize += 8
		if _, err := file.UnsafeWrite(Uint64ToBytes(tuple.Second)); err != nil {
			panic(err)
		}
	}
	sstIndexLen := sm.currentSstState.sstBytesSize - sstIndexOffset
	// 一个footer的结构是：index block offset + index block length + magic number
	// 8 + 8 + 4
	sm.currentSstState.sstBytesSize += 20
	if _, err := file.UnsafeWrite(Uint64ToBytes(sstIndexOffset)); err != nil {
		panic(err)
	}
	if _, err := file.UnsafeWrite(Uint64ToBytes(sstIndexLen)); err != nil {
		panic(err)
	}
	if _, err := file.UnsafeWrite(Uint32ToBytes(entity.MAGIC_NUMBER)); err != nil {
		panic(err)
	}
	if err := file.Flush(); err != nil {
		panic(err)
	}
	return nil
}

// write memTable to sst
func (sm *SstService) writeItem(item DataItem) {
	blockBytes := sm.currentSstState.blockBytes
	// 数据块中当前item的offset，指向key的起始
	sm.currentSstState.blockItemOffset = append(sm.currentSstState.blockItemOffset, sm.currentSstState.sstBytesSize)
	// 数据item的key的长度
	keyByteSize := len(item.Key)
	// 数据item的value的长度
	valueByteSize := len(item.Value)
	// 数据项目结构：key长度 + key + value长度 + value
	// 4 + keyByteSize + 4 + valueByteSize
	sm.currentSstState.sstBytesSize += 4
	blockBytes = append(blockBytes, Uint32ToBytes(uint32(keyByteSize))...)
	sm.currentSstState.sstBytesSize += uint64(keyByteSize)
	blockBytes = append(blockBytes, item.Key...)
	sm.currentSstState.sstBytesSize += 4
	blockBytes = append(blockBytes, Uint32ToBytes(uint32(valueByteSize))...)
	sm.currentSstState.sstBytesSize += uint64(valueByteSize)
	blockBytes = append(blockBytes, item.Value...)
	sm.currentSstState.blockBytes = blockBytes
	return
}

func (sm *SstService) resetSstBlock() error {
	sm.currentSstState.currentBlockSize = 0
	sm.currentSstState.blockItemOffset = sm.currentSstState.blockItemOffset[:0]
	sm.currentSstState.blockBytes = sm.currentSstState.blockBytes[:0]
	sm.currentSstState.blockLastItem = nil
	return nil
}

func (sm *SstService) resetNextSstFile() error {
	nId, err := sm.metaManager.NextSstId()
	if err != nil {
		return err
	}
	sstPath := filepath.Join(config.GlobalConfig.DBPath, strconv.FormatUint(nId, 10)+".sst")
	file := common.NewSafeFile(sstPath)
	if err := file.Open(os.O_RDWR | os.O_CREATE | os.O_APPEND); err != nil {
		return err
	}
	sm.fileCache.Put(sstPath, file)

	sm.currentSstState.sstBytesSize = 0
	sm.currentSstState.currentBlockSize = 0
	sm.currentSstState.sstFile = file
	sm.currentSstState.sstBlockLastKey = make([]*tuple, 0)
	sm.currentSstState.blockItemOffset = make([]uint64, 0)
	sm.currentSstState.blockBytes = make([]byte, config.GlobalConfig.DBBlockSize)
	return nil
}

// 接收一个sst文件写入请求
func (sm *SstService) SendSstWrite(items DataItems) {
	sm.sstReceiver <- items
}

// receive sst write request
func (sm *SstService) receiveSstWrite() {
	for items := range sm.sstReceiver {
		if err := sm.WriteTable(items); err != nil {
			panic(err)
		}
	}
	sm.done <- true
}

// Close
func (sm *SstService) Close() error {
	sm.lock.Lock()
	defer sm.lock.Unlock()
	close(sm.sstReceiver)
	// wait all sst write done
	<-sm.done
	close(sm.done)
	sm.fileCache.SyncClear()
	return nil
}
