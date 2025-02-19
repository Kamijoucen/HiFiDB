package kv

import (
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

func (db *DB) Put(key, value []byte) error {

	db.lock.Lock()
	defer db.lock.Unlock()

	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	logRecord := &data.LogRecord{
		Key:   key,
		Value: value,
		Type:  data.LogRecordNormal,
	}

	pos, err := db.appendLogRecord(logRecord)
	if err != nil {
		return err
	}

	if !db.index.Put(key, pos) {
		return ErrIndexUpdateFailed
	}
	return nil
}

func (db *DB) Get(key []byte) ([]byte, error) {

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
