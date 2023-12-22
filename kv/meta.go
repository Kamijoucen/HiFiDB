package kv

import (
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/kamijoucen/hifidb/common"
	"github.com/kamijoucen/hifidb/config"
)

const (
	SST_FILE_NEXT_ID  = "sst_file_next_id"
	META_FILE_NEXT_ID = "meta_file_next_id"
)

// node flag
const (
	NEXT_SST_FILE_ID_NODE = uint8(iota)
	NEXT_META_FILE_ID_NODE
	SST_META_NODE
	DELETE_SST_FILE
)

type metaManager struct {
	lock        sync.RWMutex
	pointFile   *common.SafeFile
	curMetaFile *common.SafeFile
	curMetaId   uint64
	curSstId    uint64
}

func NewMetaManager() *metaManager {
	m := &metaManager{}
	if err := m.loadPoint(); err != nil {
		panic(err)
	}
	if err := m.loadMetaFile(); err != nil {
		panic(err)
	}
	return m
}

func (mm *metaManager) loadPoint() error {
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

func (mm *metaManager) initCurrentPoint() error {
	current := common.NewSafeFile(filepath.Join(config.GlobalConfig.DBPath, "CURRENT"))
	if err := current.Open(os.O_RDWR | os.O_CREATE); err != nil {
		return err
	}
	mm.pointFile = current

	b := make([]byte, 8)
	n, _ := current.Read(b)
	if n == 0 {
		mm.curMetaId = 1
		if _, err := current.Write(Uint64ToBytes(mm.curMetaId)); err != nil {
			return err
		}
	} else {
		mm.curMetaId = BytesToUint64(b)
	}
	return nil
}

func (mm *metaManager) NextSstId() (uint64, error) {

	mm.lock.Lock()
	defer mm.lock.Unlock()

	temp := mm.curSstId
	mm.curSstId = mm.curSstId + 1
	if err := mm.writeNextSstId(mm.curSstId); err != nil {
		return 0, err
	}
	return temp, nil
}

func (mm *metaManager) writeNextSstId(sstId uint64) error {
	if _, err := mm.curMetaFile.Write(EnCodeNextId(NEXT_SST_FILE_ID_NODE, sstId)); err != nil {
		return err
	}
	return nil
}

func (mm *metaManager) loadMetaFile() error {
	b := make([]byte, 1)
	for {
		n, err := mm.curMetaFile.Read(b)
		if err != nil && err != io.EOF {
			return err
		}
		if n == 0 {
			break
		}
		switch BytesToUint8(b) {
		case NEXT_SST_FILE_ID_NODE:
			bytes := make([]byte, 8)
			n, err := mm.curMetaFile.Read(bytes)
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
