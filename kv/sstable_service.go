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
}

func NewSstService() *sstService {
	return &sstService{
		fileCache:   make(map[string]*common.SafeFile),
		metaManager: NewMetaService(),
		walManager:  NewWalManager(),
	}
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

	bytes, err := EnCodeSSTable(sst)
	if err != nil {
		return err
	}
	if _, err := file.UnsafeWrite(bytes); err != nil {
		return err
	}
	return nil
}

// read sst
