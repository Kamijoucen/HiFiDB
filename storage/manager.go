package storage

import (
	"os"
	"path/filepath"
	"sync"

	"github.com/kamijoucen/hifidb/config"
	"github.com/kamijoucen/hifidb/kv"
	"github.com/kamijoucen/hifidb/kv/codec"
)

// data cache
// meta cache
// level manager
type storageManager struct {
	lock      sync.Mutex
	fileCache map[string]*safeFile
}

func NewStorageManager() *storageManager {
	return &storageManager{
		fileCache: make(map[string]*safeFile),
	}
}

func (sm *storageManager) WriteSSTable(sst *kv.SSTable) error {
	sm.lock.Lock()
	defer sm.lock.Unlock()

	path := filepath.Join(config.GlobalConfig.DBPath, "l0.sst")
	safeFile := sm.fileCache[path]
	if safeFile == nil {
		safeFile = NewSafeFile(path)
		err := safeFile.Open(os.O_CREATE | os.O_RDWR | os.O_APPEND)
		if err != nil {
			return err
		}
		sm.fileCache[path] = safeFile
	}

	bytes, err := codec.EnCodeSSTable(sst)
	if err != nil {
		return err
	}
	_, err = safeFile.Write(bytes)
	return err
}
