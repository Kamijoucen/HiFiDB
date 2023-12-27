package kv

import (
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/kamijoucen/hifidb/common"
	"github.com/kamijoucen/hifidb/config"
	"github.com/kamijoucen/hifidb/kv/entity"
)

type metaService struct {
	lock        sync.RWMutex
	pointFile   *common.SafeFile
	curMetaFile *common.SafeFile
	curMetaId   uint64
	curSstId    uint64
}

func NewMetaService() *metaService {
	m := &metaService{}
	if err := m.loadPoint(); err != nil {
		panic(err)
	}
	if err := m.loadMetaFile(); err != nil {
		panic(err)
	}
	return m
}

func (mm *metaService) loadPoint() error {
	mm.lock.Lock()
	defer mm.lock.Unlock()

	// init current file
	if err := mm.initCurrentPoint(); err != nil {
		return err
	}
	mm.curMetaFile = common.NewSafeFile(
		filepath.Join(config.GlobalConfig.DBPath, strconv.FormatUint(mm.curMetaId, 10)+".meta"))
	if err := mm.curMetaFile.Open(os.O_RDWR | os.O_CREATE | os.O_APPEND); err != nil {
		return err
	}
	return nil
}

func (mm *metaService) initCurrentPoint() error {
	current := common.NewSafeFile(filepath.Join(config.GlobalConfig.DBPath, "CURRENT"))
	if err := current.Open(os.O_RDWR | os.O_CREATE); err != nil {
		return err
	}
	mm.pointFile = current

	b := make([]byte, 8)
	n, _ := current.UnsafeRead(b)
	if n == 0 {
		mm.curMetaId = 1
		if _, err := current.UnsafeWrite(Uint64ToBytes(mm.curMetaId)); err != nil {
			return err
		}
	} else {
		mm.curMetaId = BytesToUint64(b)
	}
	return nil
}

func (mm *metaService) NextSstId() (uint64, error) {

	mm.lock.Lock()
	defer mm.lock.Unlock()

	temp := mm.curSstId
	mm.curSstId = mm.curSstId + 1
	if err := mm.writeNextSstId(mm.curSstId); err != nil {
		return 0, err
	}
	return temp, nil
}

func (mm *metaService) writeNextSstId(sstId uint64) error {
	if _, err := mm.curMetaFile.UnsafeWrite(EnCodeNextId(entity.NEXT_SST_FILE_ID_NODE, sstId)); err != nil {
		return err
	}
	return nil
}

func (mm *metaService) loadMetaFile() error {
	b := make([]byte, 1)
	for {
		n, err := mm.curMetaFile.UnsafeRead(b)
		if err != nil && err != io.EOF {
			return err
		}
		if n == 0 {
			break
		}
		switch BytesToUint8(b) {
		case entity.NEXT_SST_FILE_ID_NODE:
			bytes := make([]byte, 8)
			n, err := mm.curMetaFile.UnsafeRead(bytes)
			// TODO 处理文件损坏的情况
			if err != nil {
				return err
			}
			if n == 0 {
				break
			}
			mm.curSstId = BytesToUint64(bytes)
		}
	}
	if mm.curSstId == 0 {
		mm.curSstId = 1
	}
	return nil
}
