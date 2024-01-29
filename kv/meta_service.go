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
	lock             *sync.RWMutex
	curMetaId        uint64
	nextSstId        uint64
	curMetaFile      *common.SafeFile
	sstMetaFileCache *common.LRUCache[uint64, entity.SSTMeta]
}

func NewMetaService() *MetaService {
	m := &MetaService{
		lock:             &sync.RWMutex{},
		sstMetaFileCache: common.NewLRUCache[uint64, entity.SSTMeta](10000),
	}
	m.lock.Lock()
	defer m.lock.Unlock()

	if err := m.initCurrentPoint(); err != nil {
		panic(err)
	}
	m.curMetaFile = m.openNewMetaFile()
	if err := m.loadMetaFile(); err != nil {
		panic(err)
	}
	return m
}

// 初始化指向当前meta文件的指针文件
func (mm *MetaService) initCurrentPoint() error {
	current := common.NewSafeFile(filepath.Join(config.GlobalConfig.DBPath, "CURRENT"))
	defer current.Close()
	if err := current.Open(os.O_RDWR | os.O_CREATE); err != nil {
		return err
	}
	b := make([]byte, 8)
	n, _ := current.Read(b)
	if n == 0 {
		mm.curMetaId = 1
		if _, err := current.Write(Uint64ToBytes(mm.curMetaId)); err != nil {
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

// get meta info
func (mm *MetaService) GetSstMeta(sstId uint64) (*entity.SSTMeta, error) {
	s := mm.sstMetaFileCache.Get(sstId)
	for s == nil {
		panic("not implement")
	}
	// mm.scanSstMetaById()
	return s, nil
}

func (mm *MetaService) GetNextSstId() (uint64, error) {

	mm.lock.Lock()
	defer mm.lock.Unlock()

	temp := mm.nextSstId
	mm.nextSstId = mm.nextSstId + 1
	if err := mm.writeNextSstId(mm.nextSstId); err != nil {
		return 0, err
	}
	return temp, nil
}

// Write sst file meta
func (mm *MetaService) WriteSstMeta(sstMeta *entity.SSTMeta) error {
	mm.lock.Lock()
	defer mm.lock.Unlock()

	bytes := SSTMetaToBytes(sstMeta)
	if err := common.WriteBytesAndFlush(mm.curMetaFile, bytes); err != nil {
		return err
	}
	mm.sstMetaFileCache.Put(sstMeta.FileId, sstMeta)
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

func (mm *MetaService) writeNextSstId(sstId uint64) error {
	if _, err := mm.curMetaFile.Write(EnCodeNextId(entity.NEXT_SST_FILE_ID_NODE, sstId)); err != nil {
		return err
	}
	_ = mm.curMetaFile.Flush()
	return nil
}

// scan meta info
func (mm *MetaService) scanSstMetaById(sstId uint64) (*entity.SSTMeta, error) {
	b := make([]byte, 1)
	for {
		n, err := mm.curMetaFile.Read(b)
		if err != nil && err != io.EOF {
			return nil, err
		}
		if n == 0 {
			break
		}
		switch BytesToUint8(b) {
		case entity.NEXT_SST_FILE_ID_NODE:
			mm.curMetaFile.Seek(8, io.SeekStart)
		case entity.NEXT_META_FILE_ID_NODE:
			panic("not implement")
		case entity.SST_META_NODE:
			mm.loadSstMeta()
		case entity.DELETE_SST_FILE_NODE:
			panic("not implement")
		case entity.SST_LEVEL_NODE:
			panic("not implement")
		default:
			panic("not implement")
		}
	}
	return nil, nil
}

// open new meta
func (mm *MetaService) openNewMetaFile() *common.SafeFile {
	path := filepath.Join(config.GlobalConfig.DBPath, strconv.FormatUint(mm.curMetaId, 10)+".meta")
	metaFile := common.NewSafeFileWithLock(path, false)
	if err := metaFile.Open(os.O_RDWR | os.O_CREATE | os.O_APPEND); err != nil {
		panic(err)
	}
	return metaFile
}

func (mm *MetaService) loadMetaFile() error {
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
		case entity.NEXT_SST_FILE_ID_NODE:
			mm.loadNextSstId()
		case entity.NEXT_META_FILE_ID_NODE:
			panic("not implement")
		case entity.SST_META_NODE:
			if meta, err := mm.loadSstMeta(); err != nil {
				return err
			} else {
				mm.sstMetaFileCache.Put(meta.FileId, meta)
			}
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
func (mm *MetaService) loadNextSstId() error {
	b := make([]byte, 8)
	n, err := mm.curMetaFile.Read(b)
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
func (mm *MetaService) loadSstMeta() (*entity.SSTMeta, error) {
	// TODO 错误码处理
	b := make([]byte, 16)
	// 读取首部16个字节
	if _, err := mm.curMetaFile.Read(b); err != nil {
		return nil, err
	}
	sstMeta := &entity.SSTMeta{}
	sstMeta.FileId = BytesToUint64(b[:8])
	sstMeta.Level = BytesToUint64(b[8:16])
	// 读取最小key长度
	if _, err := mm.curMetaFile.Read(b[:4]); err != nil {
		return nil, err
	}
	minKeyLen := BytesToUint32(b[:4])
	// 读取最小key
	minKey := make([]byte, minKeyLen)
	if _, err := mm.curMetaFile.Read(minKey); err != nil {
		return nil, err
	}
	sstMeta.Range.MinKey = minKey
	// 读取最大key长度
	if _, err := mm.curMetaFile.Read(b[:4]); err != nil {
		return nil, err
	}
	maxKeyLen := BytesToUint32(b[:4])
	// 读取最大key
	maxKey := make([]byte, maxKeyLen)
	if _, err := mm.curMetaFile.Read(maxKey); err != nil {
		return nil, err
	}
	sstMeta.Range.MaxKey = maxKey
	return sstMeta, nil
}
