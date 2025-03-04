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
)

type DB struct {
	options    *cfg.Options
	lock       *sync.RWMutex
	activeFile *HFile
	olderFiles map[uint32]*HFile
	index      Indexer
	seqNo      uint64
}

// Open 打开数据库
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
		olderFiles: map[uint32]*HFile{},
		index:      NewIndex(cfg.BTree),
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

// Put 添加数据
func (db *DB) Put(key, value []byte) error {

	if len(key) == 0 {
		return errs.ErrKeyIsEmpty
	}

	logRecord := &LogRecord{
		Key:   logRecordKeyWithSeq(key, nonTransactionSeqNo),
		Value: value,
		Type:  LogRecordNormal,
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

// Get 根据key获取数据
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
	return db.getValueByPosition(pos)
}

// Delete 根据key删除数据
func (db *DB) Delete(key []byte) error {

	if len(key) == 0 {
		return errs.ErrKeyIsEmpty
	}

	// 先在内存中查询索引是否存在
	if db.index.Get(key) == nil {
		return nil
	}

	logRecord := &LogRecord{
		Key:  logRecordKeyWithSeq(key, nonTransactionSeqNo),
		Type: LogRecordDeleted,
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

// ListKeys 列出所有key
func (db *DB) ListKeys() [][]byte {
	// 无需加库锁，因为btree上的锁会保证key的一致性
	indexIter := db.index.IndexIterator(false)
	defer indexIter.Close()

	keys := make([][]byte, db.index.Size())
	var idx int
	for indexIter.Rewind(); indexIter.Valid(); indexIter.Next() {
		keys[idx] = indexIter.Key()
		idx++
	}
	return keys
}

// Fold 遍历所有key
func (db *DB) Fold(f func(key, value []byte) bool) error {

	// 这里需要加，因为tree上的锁只会保证key的一致性，value的一致性需要库锁保证
	db.lock.RLock()
	defer db.lock.RUnlock()

	indexIter := db.index.IndexIterator(false)

	for indexIter.Rewind(); indexIter.Valid(); indexIter.Next() {
		valueBytes, err := db.getValueByPosition(indexIter.Value())
		if err != nil {
			return err
		}
		if !f(indexIter.Key(), valueBytes) {
			break
		}
	}

	return nil
}

// Close 关闭数据库
func (db *DB) Close() error {
	db.lock.Lock()
	defer db.lock.Unlock()

	if db.activeFile != nil {
		if err := db.activeFile.Close(); err != nil {
			return err
		}
	}

	for _, d := range db.olderFiles {
		if err := d.Close(); err != nil {
			return err
		}
	}
	return nil
}

// Sync 持久化数据
func (db *DB) Sync() error {
	if db.activeFile == nil {
		return nil
	}
	db.lock.Lock()
	defer db.lock.Unlock()
	return db.activeFile.Sync()
}

// getValueByPosition 根据位置获取数据
func (db *DB) getValueByPosition(pos *LogRecordPos) ([]byte, error) {
	var d *HFile
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
	if r.Type == LogRecordDeleted {
		return nil, errs.ErrKeyNotFound
	}
	return r.Value, nil
}

// appendLogRecordWithLock 添加日志记录 加锁
func (db *DB) appendLogRecordWithLock(logRecord *LogRecord) (*LogRecordPos, error) {
	db.lock.Lock()
	defer db.lock.Unlock()
	return db.appendLogRecord(logRecord)
}

// appendLogRecord 添加日志记录
func (db *DB) appendLogRecord(r *LogRecord) (*LogRecordPos, error) {

	if db.activeFile == nil {
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}
	encRecord, size := EncodeLogRecord(r)

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

	pos := &LogRecordPos{
		Fid:    db.activeFile.FileId,
		Offset: writeOffset,
	}
	return pos, nil
}

// setActiveDataFile 设置活跃的数据文件, 如果没有则创建一个
func (db *DB) setActiveDataFile() error {

	var initFleId uint32 = 0
	if db.activeFile != nil {
		initFleId = db.activeFile.FileId + 1
	}

	d, err := OpenDataFile(db.options.DirPath, initFleId)
	if err != nil {
		return err
	}
	db.activeFile = d
	return nil
}

// loadDataFiles 加载数据文件
func (db *DB) loadDataFiles() ([]uint32, error) {

	dirFiles, err := os.ReadDir(db.options.DirPath)
	if err != nil {
		return nil, err
	}

	var fileIds []uint32
	// 遍历所有.data文件
	for _, f := range dirFiles {
		if !strings.HasSuffix(f.Name(), FileSuffix) {
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
		dataFile, err := OpenDataFile(db.options.DirPath, fid)
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

// loadIndexFromDataFiles 从数据文件加载索引
func (db *DB) loadIndexFromDataFiles(fileIds []uint32) error {

	if len(fileIds) == 0 {
		return nil
	}

	updateIndex := func(key []byte, recordType LogRecordType, pos *LogRecordPos) {
		var ok bool
		if recordType == LogRecordDeleted {
			ok = db.index.Delete(key)
		} else {
			ok = db.index.Put(key, pos)
		}
		if !ok {
			panic("index update failed")
		}
	}

	for _, fid := range fileIds {
		var dataFile *HFile
		if fid == db.activeFile.FileId {
			dataFile = db.activeFile
		} else {
			dataFile = db.olderFiles[fid]
		}

		var offset int64 = 0
		for {

			// 构造内存位置索引
			logRecord, rSize, err := dataFile.ReadLogRecord(offset)

			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}

			logRecordPos := &LogRecordPos{
				Fid:    fid,
				Offset: offset,
			}

			realKey, seqNo := parseLogRecordKey(logRecord.Key)
			if seqNo == nonTransactionSeqNo {
				updateIndex(realKey, logRecord.Type, logRecordPos)
			} else {
				// TODO 如果事务提交才更新索引
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
