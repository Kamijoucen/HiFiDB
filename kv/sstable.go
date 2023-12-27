package kv

import (
	"os"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/kamijoucen/hifidb/common"
	"github.com/kamijoucen/hifidb/config"
)

// |-------------------------------------|
// | key size | key | value size | value |
// |-------------------------------------|

const (
	MAGIC_NUMBER = uint32(121213138)
)

const (
	// 值标识
	NORMAL_VALUE = uint8(0)
	DELETE_VALUE
	UPDATE_VALUE
)

type DataItem struct {
	Key   []byte
	Value []byte
}

// TODO
// index block设计可以优化，如果一个key的value很大，那么这个key的索引就会很大
// 这里可以参考leveldb中的共享前缀算法
// 并且index item不在指向每一个key的起始位置，而是指向一个小data block的尾部
type IndexItem struct {
	Key    []byte // key
	Offset uint64 // 数据块的偏移量
	Length uint32 // 数据块的长度
}

type FooterItem struct {
	IndexOffset uint64 // 索引块的偏移量
	IndexLen    uint64 // 索引块的长度
	Magic       uint32 // 魔数
}

// TODO
// 目前设计有点问题，sstable中一个大DataBlock中应该分为多个小block
// 考虑给每个小datablock添加校验和
// 而 index block 的索引指向小datablock的尾数据item的起始处
type ssTable struct {
	DataBlocks  []*DataItem
	IndexBlocks []*IndexItem
	FooterItem  *FooterItem
}

// new sst
func NewSSTable() *ssTable {
	return &ssTable{}
}

// data cache
// meta cache
// level manager
type sstManager struct {
	lock        sync.RWMutex
	fileCache   map[string]*common.SafeFile
	metaManager *metaManager
	walManager  *walManager
}

func NewSstManager() *sstManager {
	return &sstManager{
		fileCache:   make(map[string]*common.SafeFile),
		metaManager: NewMetaManager(),
		walManager:  NewWalManager(),
	}
}

// @Deprecated 不应该直接写入一个sst，sst是否写入应该在manager中控制
// TODO sst 文件初始化和文件写入需要分离，sst写入仅针对对应文件加锁
func (sm *sstManager) WriteTable(sst *ssTable) error {
	// check nil
	if sst == nil || len(sst.DataBlocks) == 0 {
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

	bytes, err := EnCodeSSTable(sst)
	if err != nil {
		return err
	}
	if _, err := file.Write(bytes); err != nil {
		return err
	}
	return nil
}

// read sst
