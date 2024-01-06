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

type MetaService struct {
	lock          sync.RWMutex
	pointFile     *common.SafeFile
	curMetaFile   *common.SafeFile
	curMetaId     uint64
	nextSstId     uint64
	metaFileCache *common.LRUCache[string, common.SafeFile]
}

func NewMetaService() *MetaService {
	m := &MetaService{
		metaFileCache: common.NewLRUCache[string, common.SafeFile](1024),
	}
	if err := m.loadPoint(); err != nil {
		panic(err)
	}
	if err := m.loadMetaFile(); err != nil {
		panic(err)
	}
	return m
}

func (mm *MetaService) loadPoint() error {
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

func (mm *MetaService) initCurrentPoint() error {
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
		if err := current.Flush(); err != nil {
			return err
		}
	} else {
		mm.curMetaId = BytesToUint64(b)
	}
	return nil
}

func (mm *MetaService) NextSstId() (uint64, error) {

	mm.lock.Lock()
	defer mm.lock.Unlock()

	temp := mm.nextSstId
	mm.nextSstId = mm.nextSstId + 1
	if err := mm.writeNextSstId(mm.nextSstId); err != nil {
		return 0, err
	}
	return temp, nil
}

func (mm *MetaService) writeNextSstId(sstId uint64) error {
	if _, err := mm.curMetaFile.UnsafeWrite(EnCodeNextId(entity.NEXT_SST_FILE_ID_NODE, sstId)); err != nil {
		return err
	}
	if err := mm.curMetaFile.Flush(); err != nil {
		return err
	}
	return nil
}

func (mm *MetaService) loadMetaFile() error {
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
			mm.readNextSstId()
		case entity.NEXT_META_FILE_ID_NODE:
			panic("not implement")
		case entity.SST_META_NODE:
			mm.readSstMeta()
		case entity.DELETE_SST_FILE_NODE:
			panic("not implement")
		case entity.SST_LEVEL_NODE:
			panic("not implement")
		default:
			panic("not implement")
		}
	}
	if mm.nextSstId == 0 {
		mm.nextSstId = 1
	}
	return nil
}

// load next sst file id
func (mm *MetaService) readNextSstId() error {
	b := make([]byte, 9)
	n, err := mm.curMetaFile.UnsafeRead(b)
	if err != nil {
		return err
	}
	if n == 0 {
		return nil
	}
	mm.nextSstId = BytesToUint64(b)
	return nil
}

// load sst meta
func (mm *MetaService) readSstMeta() (*entity.SSTMeta, error) {
	
	return nil, nil
}

// Write sst file meta
func (mm *MetaService) WriteSstMeta(sstMeta *entity.SSTMeta) error {
	mm.lock.Lock()
	defer mm.lock.Unlock()
	bytes := SSTMetaToBytes(sstMeta)
	if err := common.WriteBytesAndFlush(mm.curMetaFile, bytes); err != nil {
		return err
	}
	return nil
}

// write delete sst file
func (mm *MetaService) WriteDeleteSstFile(sstId uint64) error {
	mm.lock.Lock()
	defer mm.lock.Unlock()
	if err := common.WriteBytesAndFlush(mm.curMetaFile, nil); err != nil {
		return err
	}
	return nil
}
