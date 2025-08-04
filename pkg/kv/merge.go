package kv

import (
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"

	"github.com/kamijoucen/hifidb/pkg/errs"
)

const (
	mergeDirName     = "-merge"
	mergeFinishedKey = "merge.finished"
)

func (db *DB) Merge() error {
	if db.activeFile == nil {
		return nil
	}

	db.lock.Lock()
	if db.isMerging {
		db.lock.Unlock()
		return errs.ErrMergeIsProgress
	}

	db.isMerging = true
	defer func() {
		db.isMerging = false
	}()

	if err := db.activeFile.Sync(); err != nil {
		db.lock.Unlock()
		return err
	}

	db.olderFiles[db.activeFile.FileId] = db.activeFile

	// 创建新的活跃文件
	if err := db.setActiveDataFile(); err != nil {
		db.lock.Unlock()
		return err
	}

	// 记录最近一条没有参与merge的文件ID
	nonMergeFileId := db.activeFile.FileId

	// 所有需要merge的文件
	var mergeFiles []*DataFile
	for _, file := range db.olderFiles {
		mergeFiles = append(mergeFiles, file)
	}
	// 此时可以接收新的写入， 因为所有需要合并的文件都已经快照
	db.lock.Unlock()

	// 从小到大合并
	sort.Slice(mergeFiles, func(i, j int) bool {
		return mergeFiles[i].FileId < mergeFiles[j].FileId
	})
	mergePath := db.getMergePath()

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
	mergeOptions := *db.options
	mergeOptions.DirPath = mergePath
	mergeOptions.SyncWrites = false

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
			logRecordPos := db.index.Get(realKey)
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

func (db *DB) getMergePath() string {
	dir := path.Dir(path.Clean(db.options.DirPath))
	base := path.Base(db.options.DirPath)
	return path.Join(dir, base+mergeDirName)
}

// loadMergeFiles 加载merge文件
func (db *DB) loadMergeFiles() error {

	mergePath := db.getMergePath()
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
	var mergeFileNames []string
	for _, entry := range dirEntries {
		if entry.Name() == MergeFinishedFileName {
			mergeFinished = true
			break
		}
		if entry.Name() == fileLockName {
			continue
		}
		mergeFileNames = append(mergeFileNames, entry.Name())
	}

	if !mergeFinished {
		return nil
	}

	// 获取最近一次未被合并的文件ID
	nonMergeFileId, err := db.getNonMergeFileId(mergePath)
	if err != nil {
		return nil
	}

	// 删除所有已合并的文件
	var fileId uint32 = 0
	for ; fileId < nonMergeFileId; fileId++ {
		fileName := GetDataFileName(mergePath, fileId)
		if _, err := os.Stat(fileName); err == nil {
			if err := os.Remove(fileName); err != nil {
				return err
			}
		}
	}

	// 将merge后的数据移动到原目录
	for _, fileName := range mergeFileNames {
		srcPath := filepath.Join(mergePath, fileName)
		dstPath := filepath.Join(db.options.DirPath, fileName)
		if err := os.Rename(srcPath, dstPath); err != nil {
			return err
		}
	}
	return nil
}

// getMergeFileId 获取未merge文件ID
func (db *DB) getNonMergeFileId(dirPath string) (uint32, error) {

	mergeFinishedFile, err := OpenMergeFinishedFile(dirPath)
	if err != nil {
		return 0, err
	}

	logRecord, _, err := mergeFinishedFile.ReadLogRecord(0)
	if err != nil {
		return 0, err
	}

	nonMergeFinishedFileId, err := strconv.Atoi(string(logRecord.Value))
	if err != nil {
		return 0, err
	}
	return uint32(nonMergeFinishedFileId), nil
}

// loadIndexFromHintFile 从hint文件加载索引
func (db *DB) loadIndexFromHintFile() error {

	hintFileName := filepath.Join(db.options.DirPath, HintFileName)
	// 检查hint文件是否存在
	if _, err := os.Stat(hintFileName); os.IsNotExist(err) {
		// hint文件不存在，说明没有发生过merge
		return nil
	}

	// 打开hint文件
	hintFile, err := OpenHintFile(db.options.DirPath)
	if err != nil {
		return err
	}

	// 读取文件中的索引
	var offset int64 = 0
	for {
		logRecord, size, err := hintFile.ReadLogRecord(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		// 解析索引位置
		pos := DecodeLogRecordPos(logRecord.Value)
		db.index.Put(logRecord.Key, pos)
		offset += size
	}
	return nil
}
