package kv

import (
	"io"
	"os"
	"path"
	"sort"

	"github.com/kamijoucen/hifidb/pkg/errs"
)

const mergeDirName = "merge"

func (d *DB) Merge() error {
	if d.activeFile == nil {
		return nil
	}

	d.lock.Lock()
	if d.isMerging {
		d.lock.Unlock()
		return errs.ErrMergeIsProgress
	}

	d.isMerging = true
	defer func() {
		d.isMerging = false
	}()

	if err := d.activeFile.Sync(); err != nil {
		d.lock.Unlock()
		return err
	}

	d.olderFiles[d.activeFile.FileId] = d.activeFile

	// 创建新的数据文件
	if err := d.setActiveDataFile(); err != nil {
		d.lock.Unlock()
		return err
	}

	// 所有需要merge的文件
	var mergeFiles []*DataFile
	for _, file := range d.olderFiles {
		mergeFiles = append(mergeFiles, file)
	}
	// 此时可以接收新的写入， 因为所有需要合并的文件都已经快照
	d.lock.Unlock()

	// 从小到大合并
	sort.Slice(mergeFiles, func(i, j int) bool {
		return mergeFiles[i].FileId < mergeFiles[j].FileId
	})
	mergePath := d.getMergePath()

	// 如果目录存在说明发生过merge， 需要删除
	if _, err := os.Stat(mergePath); err == nil {
		if err := os.RemoveAll(mergePath); err != nil {
			return err
		}
	}

	if err := os.MkdirAll(mergePath, os.ModePerm); err != nil {
		return err
	}

	// 创建新的用的merge的db实例
	mergeOptions := *d.options
	mergeOptions.DirPath = mergePath
	mergeOptions.EachSyncWrites = false

	mergeDB, err := Open(&mergeOptions)
	if err != nil {
		return err
	}
	defer func() {
		_ = mergeDB.Sync()
		_ = mergeDB.Close()
	}()

	for _, dataFile := range mergeFiles {
		var offset int64 = 0
		for {
			logRecord, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}
			// 获取key并比对真实位置，用于判断是否需是最新
			realKey, _ := parseLogRecordKey(logRecord.Key)
			logRecordPos := d.index.Get(realKey)
			if logRecordPos != nil && logRecordPos.Fid == dataFile.FileId && logRecordPos.Offset == offset {
				// 能读到就是有效的数据，merge 文件中无需携带事务ID
				logRecord.Key = logRecordKeyWithSeq(realKey, nonTransactionSeqNo)
				pos, err := mergeDB.appendLogRecord(logRecord)
				if err != nil {
					return err
				}
				// TODO 更新hint文件
			}
		}
	}

	return nil
}

func (d *DB) getMergePath() string {
	dir := path.Dir(path.Clean(d.options.DirPath))
	base := path.Base(d.options.DirPath)
	return path.Join(dir, base+mergeDirName)
}
