package kv

import (
	"os"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/kamijoucen/hifidb/common"
	"github.com/kamijoucen/hifidb/config"
)

type metaManager struct {
	lock        sync.RWMutex
	pointFile   *common.SafeFile
	curMetaFile *common.SafeFile
	curMetaId   uint64
}

func NewMetaManager() *metaManager {
	m := &metaManager{}
	if err := m.Init(); err != nil {
		panic(err)
	}
	return m
}

func (mm *metaManager) Init() error {
	mm.lock.Lock()
	defer mm.lock.Unlock()

	// init current file
	if err := mm.initCurrentPoint(); err != nil {
		return err
	}
	mm.curMetaFile = common.NewSafeFile(filepath.Join(config.GlobalConfig.DBPath, strconv.FormatUint(mm.curMetaId, 10)))
	if err := mm.curMetaFile.Open(os.O_RDWR | os.O_CREATE | os.O_APPEND); err != nil {
		return err
	}
	return nil
}

func (mm *metaManager) initCurrentPoint() error {
	current := common.NewSafeFile(filepath.Join(config.GlobalConfig.DBPath, "CURRENT"))
	if err := current.Open(os.O_RDWR | os.O_CREATE); err != nil {
		return err
	}
	mm.pointFile = current

	b := make([]byte, 8)
	n, err := current.Read(b)
	if err != nil {
		return err
	}
	if n == 0 {
		mm.curMetaId = 1
		if _, err := current.Write(Uint64ToBytes(mm.curMetaId)); err != nil {
			return err
		}
	}
	mm.curMetaId = BytesToUint64(b)
	return nil
}

func (mm *metaManager) ReadNextSstId(id uint64) error {
	return nil
}

func (mm *metaManager) WriteDeleteSstId(id uint64) error {
	return nil
}
