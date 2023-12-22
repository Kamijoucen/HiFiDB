package kv

import (
	"github.com/kamijoucen/hifidb/common"
)

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

func (sm *sstManager) Write(key string, value []byte) error {
	return sm.walManager.Write(key, value)
}
