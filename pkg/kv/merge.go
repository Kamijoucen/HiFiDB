package kv

import (
	"io"
	"os"
	"path"
	"sort"
	"strconv"

	"github.com/kamijoucen/hifidb/pkg/errs"
)

const (
	mergeDirName     = "-merge"
	mergeFinishedKey = "merge.finished"
)

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

	// 创建新的活跃文件
	if err := d.setActiveDataFile(); err != nil {
		d.lock.Unlock()
		return err
	}

	// 记录最近一条没有参与merge的文件ID
	nonMergeFileId := d.activeFile.FileId

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
		_ = mergeDB.Close()
	}()

	// 打开hint文件
	hintFile, err := OpenHintFile(mergePath)
	if err != nil {
		return err
	}
	defer func() {
		_ = hintFile.Close()
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
				// 将记录的位置写入hint文件
				if err := hintFile.WriteHintRecord(realKey, pos); err != nil {
					return err
				}
			}
			offset += size
		}
	}
	// 持久化索引文件
	if err := hintFile.Sync(); err != nil {
		return err
	}
	// 持久化merge文件
	if err := mergeDB.Sync(); err != nil {
		return err
	}

	// 写入 merge 完成标识
	mergeFinishedFile, err := OpenMergeFinishedFile(mergePath)
	if err != nil {
		return err
	}
	defer func() {
		_ = mergeFinishedFile.Close()
	}()

	mergeFinishedRecord := &LogRecord{
		Key:   []byte(mergeFinishedKey),
		Value: []byte(strconv.Itoa(int(nonMergeFileId))),
	}
	encMergeFinishedRecord, _ := EncodeLogRecord(mergeFinishedRecord)
	if err := mergeFinishedFile.Write(encMergeFinishedRecord); err != nil {
		return err
	}
	if err := mergeFinishedFile.Sync(); err != nil {
		return err
	}
	return nil
}

func (d *DB) getMergePath() string {
	dir := path.Dir(path.Clean(d.options.DirPath))
	base := path.Base(d.options.DirPath)
	return path.Join(dir, base+mergeDirName)
}

// loadMergeFiles 加载merge文件
func (d *DB) loadMergeFiles() error {

	mergePath := d.getMergePath()
	if _, err := os.Stat(mergePath); err != nil {
		if os.IsNotExist(err) {
			// merge目录不存在，说明没有发生过merge
			return nil
		}
	}
	defer func() {
		_ = os.RemoveAll(mergePath)
	}()

	dirEntries, err := os.ReadDir(mergePath)
	if err != nil {
		return err
	}

	// 检查merge完成标识文件
	var mergeFinished bool
	for _, entry := range dirEntries {
		if entry.Name() == MergeFinishedFileName {
			mergeFinished = true
			break
		}
	}

	if !mergeFinished {
		return nil
	}

	// TODO

	return nil
}
