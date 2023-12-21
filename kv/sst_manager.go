package kv

import (
	"os"
	"path/filepath"
	"sync"

	"github.com/kamijoucen/hifidb/common"
	"github.com/kamijoucen/hifidb/config"
)

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
func (sm *sstManager) WriteSSTable(sst *SSTable) error {
	sm.lock.RLock()

	path := filepath.Join(config.GlobalConfig.DBPath, "l0.sst")
	safeFile := sm.fileCache[path]
	sm.lock.Unlock()

	if safeFile == nil {
		sm.lock.Lock()
		safeFile = common.NewSafeFile(path)
		err := safeFile.Open(os.O_CREATE | os.O_RDWR | os.O_APPEND)
		if err != nil {
			sm.lock.Unlock()
			return err
		}
		sm.fileCache[path] = safeFile
		sm.lock.Unlock()
	}

	bytes, err := EnCodeSSTable(sst)
	if err != nil {
		return err
	}
	_, err = safeFile.Write(bytes)
	return err
}

func (sm *sstManager) Write(key string, value []byte) error {
	return sm.walManager.Write(key, value)
}
