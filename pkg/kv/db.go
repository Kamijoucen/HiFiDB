package kv

import (
	"io"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/kamijoucen/hifidb/pkg/kv/data"
	"github.com/kamijoucen/hifidb/pkg/kv/index"
)

type DB struct {
	options    *Options
	lock       *sync.RWMutex
	activeFile *data.DataFile
	olderFiles map[uint32]*data.DataFile
	index      index.Indexer
}

func Open(options *Options) (*DB, error) {

	if err := checkOptions(options); err != nil {
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
		olderFiles: make(map[uint32]*data.DataFile),
		index:      index.NewIndex(index.BTree),
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
		return ErrKeyIsEmpty
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
		return ErrIndexUpdateFailed
	}
	return nil
}

func (db *DB) Get(key []byte) ([]byte, error) {

	db.lock.RLock()
	defer db.lock.RUnlock()

	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}

	pos := db.index.Get(key)
	if pos == nil {
		return nil, ErrKeyNotFound
	}

	var d *data.DataFile
	if db.activeFile.FileId == pos.Fid {
		d = db.activeFile
	} else {
		d = db.olderFiles[pos.Fid]
	}

	if d == nil {
		return nil, ErrDataFileNotFound
	}

	recordBytes, err := d.ReadAt(pos.Offset)
	if err != nil {
		return nil, err
	}
	r := data.DecodeLogRecord(recordBytes)

	if r.Type == data.LogRecordDelete {
		return nil, ErrKeyNotFound
	}
	return r.Value, nil
}

func (db *DB) Delete(key []byte) error {

	db.lock.Lock()
	defer db.lock.Unlock()

	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// 先在内存中查询索引是否存在
	if db.index.Get(key) == nil {
		return nil
	}

	logRecord := &data.LogRecord{
		Key:  key,
		Type: data.LogRecordDelete,
	}

	_, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return err
	}

	if !db.index.Delete(key) {
		return ErrIndexUpdateFailed
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
		if !strings.HasSuffix(f.Name(), data.DataFileSuffix) {
			continue
		}
		nameArr := strings.Split(f.Name(), ".")
		fid, err := strconv.Atoi(nameArr[0])
		if err != nil {
			return nil, ErrDataDirCorrupted
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

	processLogRecords := func(dataFile *data.DataFile, fileId uint32) error {
		var offset int64 = 0
		for {
			logRecordBytes, err := dataFile.ReadAt(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}
			logRecord := data.DecodeLogRecord(logRecordBytes)

			if logRecord.Type == data.LogRecordDelete {
				db.index.Delete(logRecord.Key)
			} else {
				logRecordPos := &data.LogRecordPos{
					Fid:    fileId,
					Offset: offset,
				}
				db.index.Put(logRecord.Key, logRecordPos)
			}
			offset += int64(len(logRecordBytes))
		}
		// 如果是活跃文件，需要更新offset
		if fileId == db.activeFile.FileId {
			db.activeFile.WriteOffset = offset
		}
		return nil
	}

	var wg sync.WaitGroup
	wg.Add(len(fileIds))
	sem := make(chan struct{}, runtime.NumCPU()*2)

	for _, fid := range fileIds {
		sem <- struct{}{}
		go func(fid uint32) {
			defer wg.Done()
			defer func() { <-sem }()
			var dataFile *data.DataFile
			if fid == db.activeFile.FileId {
				dataFile = db.activeFile
			} else {
				dataFile = db.olderFiles[fid]
			}
			if err := processLogRecords(dataFile, fid); err != nil {
				panic(err)
			}
		}(fid)
	}

	wg.Wait()
	return nil
}
