package kv

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"slices"

	"github.com/gofrs/flock"
	"github.com/kamijoucen/hifidb/pkg/errs"
)

const (
	seqNoKey     = "seq.no"
	fileLockName = "flock"
)

type DB struct {
	options     *Options
	lock        *sync.RWMutex
	activeFile  *DataFile
	olderFiles  map[uint32]*DataFile
	index       Indexer
	seqNo       uint64
	isMerging   bool
	fileLock    *flock.Flock
	bytesWrite  uint32 // 累计写入的字节数
	reclaimSize int64  // 无效数据大小
}

// Open 打开数据库
func Open(options *Options) (*DB, error) {

	if err := CheckOptions(options); err != nil {
		return nil, err
	}

	// 检查目录是否存在
	if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {
		if err := os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}
	// 判断当前库文件是否正在使用
	fileLock := flock.New(filepath.Join(options.DirPath, fileLockName))
	hold, err := fileLock.TryLock()
	if err != nil {
		return nil, err
	}
	if !hold {
		return nil, errs.ErrDataBaseIsUsing
	}
	// 初始化db
	db := &DB{
		options:    options,
		lock:       &sync.RWMutex{},
		olderFiles: map[uint32]*DataFile{},
		index:      NewIndex(BTree, options.DirPath, options.SyncWrites),
		fileLock:   fileLock,
	}

	// 加载merge文件
	if err := db.loadMergeFiles(); err != nil {
		return nil, err
	}

	// 加载数据文件
	fileIds, err := db.loadDataFiles()
	if err != nil {
		return nil, err
	}

	// 从hint文件加载索引
	if err := db.loadIndexFromHintFile(); err != nil {
		return nil, err
	}
	// 加载索引
	if err := db.loadIndexFromDataFiles(fileIds); err != nil {
		return nil, err
	}
	// 重置 mmap io 仅用于加速读
	if db.options.MMapAtStartup {
		if err := db.resetIOType(); err != nil {
			return nil, err
		}
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

	// 更新内存索引
	if oldPos := db.index.Put(key, pos); oldPos != nil {
		db.reclaimSize += int64(oldPos.Size)
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

	pos, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return err
	}
	db.reclaimSize += int64(pos.Size)

	oldPos, ok := db.index.Delete(key)
	if !ok {
		return errs.ErrIndexUpdateFailed
	}

	if oldPos != nil {
		db.reclaimSize += int64(oldPos.Size)
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
	defer indexIter.Close()

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
	defer func() {
		if err := db.fileLock.Unlock(); err != nil {
			panic(fmt.Sprintf("failed to unlock file lock: %v", err))
		}
	}()

	if err := db.index.Close(); err != nil {
		return err
	}

	// 关闭活跃文
	if db.activeFile != nil {
		if err := db.activeFile.Close(); err != nil {
			return err
		}
	}
	// 关闭非活跃文件
	for _, d := range db.olderFiles {
		if err := d.Close(); err != nil {
			return err
		}
	}
	// 清空映射
	db.activeFile = nil
	db.olderFiles = nil
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
	var d *DataFile
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

	db.bytesWrite += uint32(size)

	var needSync = db.options.SyncWrites
	if !needSync && db.options.BytesPerSync > 0 && db.bytesWrite >= db.options.BytesPerSync {
		needSync = true
	}

	if needSync {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
		db.bytesWrite = 0
	}

	pos := &LogRecordPos{
		Fid:    db.activeFile.FileId,
		Offset: writeOffset,
		Size:   uint32(size),
	}
	return pos, nil
}

// setActiveDataFile 设置活跃的数据文件, 如果没有则创建一个
func (db *DB) setActiveDataFile() error {

	var initFleId uint32 = 0
	if db.activeFile != nil {
		initFleId = db.activeFile.FileId + 1
	}

	d, err := OpenDataFile(IO_FILE, db.options.DirPath, initFleId)
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
		if !strings.HasSuffix(f.Name(), DataFileSuffix) {
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
	slices.Sort(fileIds)

	// 逐个加载
	for i, fid := range fileIds {
		ioType := IO_FILE
		if db.options.MMapAtStartup {
			ioType = IO_MMAP
		}
		dataFile, err := OpenDataFile(ioType, db.options.DirPath, fid)
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

	// 检查是否发生过merge
	hasMerge, nonMergeFileId := false, uint32(0)
	mergeFinishedFileName := filepath.Join(db.options.DirPath, MergeFinishedFileName)
	if _, err := os.Stat(mergeFinishedFileName); err == nil {
		fid, err := db.getNonMergeFileId(db.options.DirPath)
		if err != nil {
			return err
		}
		hasMerge = true
		nonMergeFileId = fid
	}

	updateIndex := func(key []byte, recordType LogRecordType, pos *LogRecordPos) {

		var oldPos *LogRecordPos
		if recordType == LogRecordDeleted {
			oldPos, _ = db.index.Delete(key)
			db.reclaimSize += int64(pos.Size)
		} else {
			oldPos = db.index.Put(key, pos)
		}

		if oldPos != nil {
			db.reclaimSize += int64(oldPos.Size)
		}
	}

	// 事务数据
	transactionRecords := make(map[uint64][]*TransactionRecord)
	var currentSeqNo = nonTransactionSeqNo

	for _, fid := range fileIds {

		// 如果是merge完成的文件，跳过
		if hasMerge && fid < nonMergeFileId {
			continue
		}
		var dataFile *DataFile
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
				Size:   uint32(rSize),
			}

			realKey, seqNo := parseLogRecordKey(logRecord.Key)
			if seqNo == nonTransactionSeqNo {
				updateIndex(realKey, logRecord.Type, logRecordPos)
			} else {
				// 如果事务提交才更新索引
				if logRecord.Type == LogRecordTxnFinished {
					for _, txnRecord := range transactionRecords[seqNo] {
						updateIndex(txnRecord.Record.Key, txnRecord.Record.Type, txnRecord.Pos)
					}
					delete(transactionRecords, seqNo)
				} else { // 未读到事务提交标记，缓存事务数据
					logRecord.Key = realKey
					transactionRecords[seqNo] = append(transactionRecords[seqNo], &TransactionRecord{
						Record: logRecord,
						Pos:    logRecordPos,
					})
				}
			}
			// 更新事务ID
			if seqNo > currentSeqNo {
				currentSeqNo = seqNo
			}
			// 更新offset
			offset += rSize
		}
		// 如果是活跃文件，需要更新offset
		if fid == db.activeFile.FileId {
			db.activeFile.WriteOffset = offset
		}
		// 更新事务ID
		db.seqNo = currentSeqNo
	}
	return nil
}

// resetIOType 重置IO类型
func (db *DB) resetIOType() error {
	if db.activeFile == nil {
		return nil
	}
	if err := db.activeFile.SetIOManager(IO_FILE); err != nil {
		return err
	}

	for _, d := range db.olderFiles {
		if err := d.SetIOManager(IO_FILE); err != nil {
			return err
		}
	}

	return nil
}
