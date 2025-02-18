package kv

import (
	"sync"

	"github.com/kamijoucen/hifidb/pkg/kv/data"
)

type DB struct {
	lock       *sync.RWMutex
	activeFile *data.DataFile
}

// Put
func (db *DB) Put(key, value []byte) error {

	return nil
}

// appendLogRecord
func (db *DB) appendLogRecord(r *data.LogRecord) (*data.LogRecordPos, error) {
	db.lock.Lock()
	defer db.lock.Unlock()

	return nil, nil
}

func (db *DB) setActiveDataFile(fid uint32) error {

	var initFleId uint32 = 0

	if db.activeFile != nil {
		initFleId = db.activeFile.FileId + 1
	}

	data.OpenDataFile("", initFleId)

	return nil
}
