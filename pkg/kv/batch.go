package kv

import (
	"encoding/binary"
	"sync"
	"sync/atomic"

	"github.com/kamijoucen/hifidb/pkg/errs"
)

const nonTransactionSeqNo uint64 = 0

var txnFinishedKey = []byte("txn-fin")

// WriteBatch 原子写
type WriteBatch struct {
	options       *WriteBatchOptions
	lock          *sync.Mutex
	db            *DB
	pendingWrites map[string]*LogRecord
}

func (db *DB) NewWriteBatch(options *WriteBatchOptions) *WriteBatch {
	return &WriteBatch{
		options:       options,
		lock:          &sync.Mutex{},
		db:            db,
		pendingWrites: map[string]*LogRecord{},
	}
}

// Put 添加数据
func (wb *WriteBatch) Put(key, value []byte) error {

	if len(key) == 0 {
		return errs.ErrKeyIsEmpty
	}

	wb.lock.Lock()
	defer wb.lock.Unlock()

	logRecord := &LogRecord{
		Key:   key,
		Value: value,
		Type:  LogRecordNormal,
	}
	wb.pendingWrites[string(key)] = logRecord
	return nil
}

// Delete 删除数据
func (wb *WriteBatch) Delete(key []byte) error {

	if len(key) == 0 {
		return errs.ErrKeyIsEmpty
	}

	wb.lock.Lock()
	defer wb.lock.Unlock()

	logRecordPos := wb.db.index.Get(key)
	// 如果要删除的数据在内存中不存在，直接返回
	if logRecordPos == nil {
		if wb.pendingWrites[string(key)] != nil {
			delete(wb.pendingWrites, string(key))
			return nil
		}
	}

	logRecord := &LogRecord{
		Key:   key,
		Value: nil,
		Type:  LogRecordDeleted,
	}
	wb.pendingWrites[string(key)] = logRecord
	return nil
}

// Commit 提交写入
func (wb *WriteBatch) Commit() error {

	wb.lock.Lock()
	defer wb.lock.Unlock()

	if len(wb.pendingWrites) == 0 {
		return nil
	}

	if len := len(wb.pendingWrites); len > int(wb.options.MaxBatchSize) {
		return errs.ErrExceedMaxFileSize
	}

	// DB加锁保证事务提交串行
	wb.db.lock.Lock()
	defer wb.db.lock.Unlock()

	// 获取最新的事务id
	seqNo := atomic.AddUint64(&wb.db.seqNo, 1)

	// 写入数据, 暂不更新索引, 制造一种快照读的效果
	// TODO 复用
	positions := make(map[string]*LogRecordPos, len(wb.pendingWrites))
	for _, logRecord := range wb.pendingWrites {
		logRecordPos, err := wb.db.appendLogRecord(&LogRecord{
			Key:   logRecordKeyWithSeq(logRecord.Key, seqNo),
			Value: logRecord.Value,
			Type:  logRecord.Type,
		})
		if err != nil {
			return nil
		}
		positions[string(logRecord.Key)] = logRecordPos
	}

	finishedRecord := &LogRecord{
		Key:  logRecordKeyWithSeq(txnFinishedKey, seqNo),
		Type: LogRecordTxnFinished,
	}
	// 写入事务完成标记
	if _, err := wb.db.appendLogRecord(finishedRecord); err != nil {
		return err
	}

	// 持久化
	if wb.options.EachSyncWrites {
		if err := wb.db.activeFile.Sync(); err != nil {
			return err
		}
	}

	// 更新索引
	for _, record := range wb.pendingWrites {
		pos := positions[string(record.Key)]
		var oldPos *LogRecordPos
		switch record.Type {
		case LogRecordNormal:
			oldPos = wb.db.index.Put(record.Key, pos)
		case LogRecordDeleted:
			oldPos, _ = wb.db.index.Delete(record.Key)
		}
		if oldPos != nil {
			wb.db.reclaimSize += int64(oldPos.Size)
		}
	}

	// 清空已经提交的数据
	clear(wb.pendingWrites)

	return nil
}

// logRecordKeyWithSeq 生成带事务ID的key
func logRecordKeyWithSeq(key []byte, seqNO uint64) []byte {
	// TODO 复用
	// 变长存储事务ID
	seq := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(seq[:], seqNO)

	encKey := make([]byte, len(key)+n)
	copy(encKey[:n], seq[:n])
	copy(encKey[n:], key)

	return encKey
}

// parseLogRecordKey 解析事务ID
func parseLogRecordKey(key []byte) ([]byte, uint64) {
	seqNo, n := binary.Uvarint(key)
	realKey := key[n:]
	return realKey, seqNo
}
