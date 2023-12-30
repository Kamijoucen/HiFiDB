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

// data cache
// meta cache
// level manager
type sstService struct {
	lock        sync.RWMutex
	fileCache   map[string]*common.SafeFile
	metaManager *metaService
	walManager  *walManager
	sstReceiver chan DataItems
	done        chan bool
}

func NewSstService() *sstService {
	sst := &sstService{
		fileCache:   make(map[string]*common.SafeFile),
		metaManager: NewMetaService(),
		walManager:  NewWalManager(),
		sstReceiver: make(chan DataItems, 100),
		done:        make(chan bool, 1),
	}
	go sst.receiveSstWrite()
	return sst
}

// @Deprecated 不应该直接写入一个sst，sst是否写入应该在manager中控制
// TODO sst 文件初始化和文件写入需要分离，sst写入仅针对对应文件加锁
// TODO SST的写入可以改为并发写，可以使用滑动窗口协议，保证整体的写入顺序
func (sm *sstService) WriteTable(dataItems DataItems) error {
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
	sm.fileCache[sstPath] = file
	if _, err := write(file, dataItems); err != nil {
		return err
	}
	return nil
}

// write memTable to sst
func write(file *common.SafeFile, dataItems DataItems) (uint64, error) {
	// 整个sst的大小
	sstBytesSize := uint64(0)
	// sst中每个data block的最后一个key
	sstBlockLastKey := make([]*tuple, 0)
	// 数据块中每个key的offset
	blockKeyOffset := make([]uint64, 0)
	// 数据块，默认4kb，可以配置,额外.25的数据用于存放尾部信息
	dataBlockBytes := make([]byte, 0, uint32(float32(config.GlobalConfig.DBBlockSize)*1.25))

	for i, block := range dataItems {

		// 数据块中当前item的offset，指向key的起始
		blockKeyOffset = append(blockKeyOffset, sstBytesSize)

		// 数据item的key的长度
		keyByteSize := len(block.Key)
		// 数据item的value的长度
		valueByteSize := len(block.Value)

		// 数据项目结构：key长度 + key + value长度 + value
		// 4 + keyByteSize + 4 + valueByteSize
		sstBytesSize += 4
		dataBlockBytes = append(dataBlockBytes, Uint32ToBytes(uint32(keyByteSize))...)
		sstBytesSize += uint64(keyByteSize)
		dataBlockBytes = append(dataBlockBytes, block.Key...)
		sstBytesSize += 4
		dataBlockBytes = append(dataBlockBytes, Uint32ToBytes(uint32(valueByteSize))...)
		sstBytesSize += uint64(valueByteSize)
		dataBlockBytes = append(dataBlockBytes, block.Value...)

		// 如果当前数据块的大于配置的数据块大小，或者是最后一个数据项，需要将数据块写入文件
		if uint64(len(dataBlockBytes)) >= config.GlobalConfig.DBBlockSize || i == len(dataItems)-1 {

			// 数据块中索引块的起始offset
			blockIndexOffset := sstBytesSize
			// 索引块的长度，每个索引都是unit8
			blockIndexLen := uint32(len(blockKeyOffset)) * 8

			// 向sst文件大小计数添加索引块的长度
			sstBytesSize += uint64(blockIndexLen)

			// 将数据项的索引添加到数据块的尾部
			for _, offset := range blockKeyOffset {
				dataBlockBytes = append(dataBlockBytes, Uint64ToBytes(offset)...)
			}

			// block footer: index block offset + index block length + comp flag + checksum
			// 8 + 4 + 1 + 4 = 17 bytes
			sstBytesSize += 17
			// 写入索引块位置 8bytes
			dataBlockBytes = append(dataBlockBytes, Uint64ToBytes(blockIndexOffset)...)
			// 写入索引块长度 4bytes
			dataBlockBytes = append(dataBlockBytes, Uint32ToBytes(blockIndexLen)...)
			// 写入压缩标识(TODO)
			dataBlockBytes = append(dataBlockBytes, Uint8ToBytes(entity.NO_COMPRESS)...)
			// 写入校验和(TODO)
			dataBlockBytes = append(dataBlockBytes, Uint32ToBytes(0)...)

			// 记录数据块最后一个字节的offset，key是当前数据块最后一个数据项
			sstBlockLastKey = append(sstBlockLastKey, &tuple{First: block.Key, Second: sstBytesSize})

			// 写入数据
			if _, err := file.UnsafeWrite(dataBlockBytes); err != nil {
				panic(err)
			}
			// clear block bytes
			blockKeyOffset = blockKeyOffset[:0]
			// 如果存在单个数据块大于4kb * 1.5的情况，需要重新分配内存，避免大块内存长期占用
			if len(dataBlockBytes) > int(float32(config.GlobalConfig.DBBlockSize)*1.5) {
				dataBlockBytes = make([]byte, config.GlobalConfig.DBBlockSize)
			} else {
				dataBlockBytes = dataBlockBytes[:0]
			}
		}
	}
	blockKeyOffset = nil
	dataBlockBytes = nil
	if err := file.Flush(); err != nil {
		panic(err)
	}
	// 索引offset
	sstIndexOffset := sstBytesSize
	// 一个索引项的结构是：key长度 + key + offset，其中offset是数据块的最后一个字节的偏移量
	// 4 + keyByteSize + 8
	for _, tuple := range sstBlockLastKey {
		// 写入索引项的key长度
		sstBytesSize += 4
		if _, err := file.UnsafeWrite(Uint32ToBytes(uint32(len(tuple.First)))); err != nil {
			panic(err)
		}
		// 写入索引项的key
		sstBytesSize += uint64(len(tuple.First))
		if _, err := file.UnsafeWrite(tuple.First); err != nil {
			panic(err)
		}
		// 写入索引项的offset
		sstBytesSize += 8
		if _, err := file.UnsafeWrite(Uint64ToBytes(tuple.Second)); err != nil {
			panic(err)
		}
	}
	sstIndexLen := sstBytesSize - sstIndexOffset
	// 一个footer的结构是：index block offset + index block length + magic number
	// 8 + 8 + 4
	sstBytesSize += 20
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
	return sstBytesSize, nil
}

// 接收一个sst文件写入请求
func (sm *sstService) SendSstWrite(items DataItems) {
	sm.sstReceiver <- items
}

// receive sst write request
func (sm *sstService) receiveSstWrite() {
	for items := range sm.sstReceiver {
		if err := sm.WriteTable(items); err != nil {
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
