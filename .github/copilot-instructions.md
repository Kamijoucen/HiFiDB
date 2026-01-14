# HiFiDB - AI Coding Instructions

## 项目概述
HiFiDB 是一个基于 Bitcask 存储模型的高频写入 KV 数据库，使用 Go 语言开发。目标演进为图形数据库。

## 核心架构

### 存储层 (pkg/kv/)
```
DB (db.go) - 数据库入口，管理文件和索引
├── DataFile (data_file.go) - 数据文件抽象，负责日志记录的读写
├── LogRecord (log_record.go) - 日志记录格式：CRC + Type + KeySize + ValueSize + Key + Value
├── IOManager (io_manager.go) - IO 抽象层
│   ├── FileIO (io_file.go) - 标准文件 IO
│   └── MMapIO (io_mmap.go) - 内存映射 IO（启动加速）
└── Indexer (index.go) - 内存索引接口
    ├── BTree (index_btree.go) - google/btree 实现
    └── ART (index_art.go) - 自适应基数树实现
```

### 数据流
- **写入**: `Put()` → `LogRecord` 编码 → 追加写入 `activeFile` → 更新内存索引
- **读取**: `Get()` → 查内存索引获取 `LogRecordPos(Fid, Offset)` → 从数据文件读取
- **删除**: 写入删除标记的 `LogRecord`（墓碑机制）
- **合并**: `Merge()` 扫描旧文件，保留有效数据，生成 hint 文件加速索引重建

### 关键数据结构
```go
// 日志记录位置索引
type LogRecordPos struct {
    Fid    uint32  // 文件 ID
    Offset int64   // 文件内偏移
    Size   uint32  // 记录大小
}

// 日志记录类型
LogRecordNormal      // 普通写入
LogRecordDeleted     // 删除标记
LogRecordTxnFinished // 事务完成标记
```

## 开发规范

### 测试
```bash
go test ./pkg/kv/... -v           # 运行所有测试
go test ./pkg/kv/ -run TestDB_Put # 运行单个测试
```
- 测试辅助: `GetTestKey(i)` 生成测试 key, `RandomValue(n)` 生成随机 value
- 测试后清理: 使用 `destroyDB(db)` 清理临时目录

### 错误处理
所有错误定义在 [pkg/errs/kv_error.go](pkg/errs/kv_error.go)，使用 `errors.Is()` 判断:
```go
if errors.Is(err, errs.ErrKeyNotFound) { ... }
```

### 配置选项
通过 `Options` 结构体配置，使用 `GetDBDefaultOptions()` 获取默认值:
- `DataFileSize`: 单个数据文件大小（默认 1GB）
- `SyncWrites`: 每次写入是否同步
- `MMapAtStartup`: 启动时是否使用 mmap 加速
- `DataFileMergeRatio`: 触发合并的无效数据比例阈值

### 事务支持
使用 `WriteBatch` 实现原子写:
```go
wb := db.NewWriteBatch(kv.GetDefaultWriteBatchOptions())
wb.Put(key, value)
wb.Delete(key2)
wb.Commit() // 原子提交
```

## 文件命名约定
- 数据文件: `{fileId:010d}.data` (如 `0000000001.data`)
- Hint 文件: `hint-index` (合并时生成的索引快照)
- 合并完成标记: `merge-finished`
- 文件锁: `flock`

## 注意事项
- 数据库目录使用文件锁保护，同一目录只能打开一个实例
- 索引仅存内存，重启时从数据文件重建（hint 文件可加速）
- `BytesPerSync > 0` 时按累计字节数触发同步，而非每次写入
- **项目处于开发阶段，所有设计可能变化，发现更优设计请主动指出**
