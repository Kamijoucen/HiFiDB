package kv

import (
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/kamijoucen/hifidb/pkg/cfg"
	"github.com/kamijoucen/hifidb/pkg/errs"
	"github.com/kamijoucen/hifidb/pkg/kv/data"
	"github.com/kamijoucen/hifidb/pkg/kv/index"
)

type DB struct {
	options    *cfg.Options
	lock       *sync.RWMutex
	activeFile *data.HFile
	olderFiles map[uint32]*data.HFile
	index      index.Indexer
}

func Open(options *cfg.Options) (*DB, error) {

	if err := cfg.CheckOptions(options); err != nil {
		return nil, err
	}

	// 检查目录是否存在
	if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {
		if err := os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	// 初始化db
	db := &DB{
		options:    options,
		lock:       &sync.RWMutex{},
		olderFiles: map[uint32]*data.HFile{},
		index:      index.NewIndex(cfg.BTree),
	}

	// 加载数据文件
	fileIds, err := db.loadDataFiles()
	if err != nil {
		return nil, err
	}

	// 加载索引
	if err := db.loadIndexFromDataFiles(fileIds); err != nil {
		return nil, err
	}
	return db, nil
}

func (db *DB) Put(key, value []byte) error {

	if len(key) == 0 {
		return errs.ErrKeyIsEmpty
	}

	logRecord := &data.LogRecord{
		Key:   key,
		Value: value,
		Type:  data.LogRecordNormal,
	}

	pos, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return err
	}

	if !db.index.Put(key, pos) {
		return errs.ErrIndexUpdateFailed
	}
	return nil
}

func (db *DB) Get(key []byte) ([]byte, error) {

	db.lock.RLock()
	defer db.lock.RUnlock()

	if len(key) == 0 {
		return nil, errs.ErrKeyIsEmpty
	}

	pos := db.index.Get(key)
	if pos == nil {
		return nil, errs.ErrKeyNotFound
	}

	var d *data.HFile
	if db.activeFile.FileId == pos.Fid {
		d = db.activeFile
	} else {
		d = db.olderFiles[pos.Fid]
	}

	if d == nil {
		return nil, errs.ErrDataFileNotFound
	}

	r, _, err := d.ReadLogRecord(pos.Offset)
	if err != nil {
		return nil, err
	}
	if r.Type == data.LogRecordDeleted {
		return nil, errs.ErrKeyNotFound
	}
	return r.Value, nil
}

func (db *DB) Delete(key []byte) error {

	if len(key) == 0 {
		return errs.ErrKeyIsEmpty
	}

	// 先在内存中查询索引是否存在
	if db.index.Get(key) == nil {
		return nil
	}

	logRecord := &data.LogRecord{
		Key:  key,
		Type: data.LogRecordDeleted,
	}

	_, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return err
	}

	if !db.index.Delete(key) {
		return errs.ErrIndexUpdateFailed
	}
	return nil
}

func (db *DB) appendLogRecordWithLock(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.lock.Lock()
	defer db.lock.Unlock()
	return db.appendLogRecord(logRecord)
}

func (db *DB) appendLogRecord(r *data.LogRecord) (*data.LogRecordPos, error) {

	if db.activeFile == nil {
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}
	encRecord, size := data.EncodeLogRecord(r)

	if db.activeFile.WriteOffset+size > db.options.DataFileSize {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}

		db.olderFiles[db.activeFile.FileId] = db.activeFile

		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}
	writeOffset := db.activeFile.WriteOffset
	if err := db.activeFile.Write(encRecord); err != nil {
		return nil, err
	}

	if db.options.EachSyncWrites {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
	}

	pos := &data.LogRecordPos{
		Fid:    db.activeFile.FileId,
		Offset: writeOffset,
	}
	return pos, nil
}

func (db *DB) setActiveDataFile() error {

	var initFleId uint32 = 0
	if db.activeFile != nil {
		initFleId = db.activeFile.FileId + 1
	}

	d, err := data.OpenDataFile(db.options.DirPath, initFleId)
	if err != nil {
		return err
	}
	db.activeFile = d
	return nil
}

func (db *DB) loadDataFiles() ([]uint32, error) {

	dirFiles, err := os.ReadDir(db.options.DirPath)
	if err != nil {
		return nil, err
	}

	var fileIds []uint32
	// 遍历所有.data文件
	for _, f := range dirFiles {
		if !strings.HasSuffix(f.Name(), data.FileSuffix) {
			continue
		}
		nameArr := strings.Split(f.Name(), ".")
		fid, err := strconv.Atoi(nameArr[0])
		if err != nil {
			return nil, errs.ErrDataDirCorrupted
		}
		fileIds = append(fileIds, uint32(fid))
	}
	// 排序
	sort.Slice(fileIds, func(i, j int) bool {
		return fileIds[i] < fileIds[j]
	})

	// 逐个加载
	for i, fid := range fileIds {
		dataFile, err := data.OpenDataFile(db.options.DirPath, fid)
		if err != nil {
			return nil, err
		}
		if i == len(fileIds)-1 {
			db.activeFile = dataFile
		} else {
			db.olderFiles[fid] = dataFile
		}
	}

	return fileIds, nil
}

func (db *DB) loadIndexFromDataFiles(fileIds []uint32) error {
	if len(fileIds) == 0 {
		return nil
	}
	for _, fid := range fileIds {
		var dataFile *data.HFile
		if fid == db.activeFile.FileId {
			dataFile = db.activeFile
		} else {
			dataFile = db.olderFiles[fid]
		}
		var offset int64 = 0
		for {
			logRecord, rSize, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}
			if logRecord.Type == data.LogRecordDeleted {
				db.index.Delete(logRecord.Key)
			} else {
				logRecordPos := &data.LogRecordPos{
					Fid:    fid,
					Offset: offset,
				}
				db.index.Put(logRecord.Key, logRecordPos)
			}
			offset += rSize
		}
		// 如果是活跃文件，需要更新offset
		if fid == db.activeFile.FileId {
			db.activeFile.WriteOffset = offset
		}
	}
	return nil
}
