package kv

import (
	"os"
	"path/filepath"
	"strconv"

	"github.com/kamijoucen/hifidb/common"
	"github.com/kamijoucen/hifidb/config"
)

// |-------------------------------------|
// | key size | key | value size | value |
// |-------------------------------------|

const (
	MAGIC_NUMBER = uint32(121213138)
)

type DataItem struct {
	Key   []byte
	Value []byte
}

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

type SSTable struct {
	DataBlocks  []*DataItem
	IndexBlocks []*IndexItem
	FooterItem  *FooterItem
}

// data cache
// meta cache
// level manager
type sstManager struct {
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
func (sm *sstManager) WriteTable(sst *SSTable) error {
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

	bytes, err := EnCodeSSTable(sst)
	if err != nil {
		return err
	}
	if _, err := file.Write(bytes); err != nil {
		return err
	}
	return nil
}
