package kv

type Stat struct {
	KeyNum          uint  // key数量
	DataFileNum     uint  // 数据文件数量
	ReclaimableSize int64 // 可回收的空间大小
	DiskSize        int64 // 磁盘使用大小
}

func (db *DB) Stat() *Stat {

	db.lock.RLock()
	defer db.lock.RUnlock()

	var dataFiles = uint(len(db.olderFiles))
	if db.activeFile != nil {
		dataFiles++
	}

	return &Stat{
		KeyNum:          uint(db.index.Size()),
		DataFileNum:     dataFiles,
		ReclaimableSize: db.reclaimSize,
		DiskSize:        0,
	}
}
