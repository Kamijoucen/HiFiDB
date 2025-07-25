# HiFiDB Copilot Instructions

## 项目概述
HiFiDB 是一个用 Go 语言编写的高性能图数据库，以稳定高效的商业数据库为开发目标。专门优化了高频文件写入场景，采用 LSM-Tree 架构，支持多种索引类型和 IO 模式。项目将兼容 Cypher 和 SPARQL 查询语言。

## 整体架构规划

HiFiDB 采用分层架构设计，自底向上分为：

### 🏗️ 架构层级
1. **存储层** - 当前实现的高性能键值存储引擎（`pkg/kv/`）
2. **图语义层** - 计划实现图数据库语义，支持图查询和图算法
3. **查询层** - 计划实现 Cypher/SPARQL 查询引擎
4. **分布式层** - 计划基于 Raft 算法实现分布式一致性
5. **事务/MVCC** - 计划实现多版本并发控制和事务支持
6. **API 层** - 计划提供 HTTP/gRPC 接口

### 当前包结构（存储层）
- `pkg/kv/` - 核心键值存储引擎，包含所有存储层功能
- `pkg/cfg/` - 配置管理和常量定义
- `pkg/errs/` - 错误定义和处理
- `examples/` - 使用示例

### 关键组件（存储层）
1. **DB** (`pkg/kv/db.go`) - 主数据库结构体，管理活跃文件、索引和锁
2. **DataFile** (`pkg/kv/data_file.go`) - 数据文件抽象，支持不同 IO 类型
3. **LogRecord** (`pkg/kv/log_record.go`) - 日志记录格式，包含 CRC 校验
4. **Index** (`pkg/kv/index*.go`) - 多种索引实现：BTree、ART、B+Tree
5. **WriteBatch** (`pkg/kv/batch.go`) - 原子写操作支持
6. **Merge** (`pkg/kv/merge.go`) - 数据合并和压缩功能

### 开发路线图
1. **事务支持** - 实现完整的 ACID 事务特性
2. **索引优化** - 底层参考 TiDB 的 LSM-Tree 方案
3. **分布式存储** - 参考 TiDB 的 Raft 方案实现集群
4. **MVCC** - 多版本并发控制支持
5. **GIS** - 地理信息系统扩展
6. **图查询引擎** - Cypher 和 SPARQL 兼容性

## 开发模式

### 配置管理
```go
// 使用默认配置
options := cfg.GetDBDefaultOptions()
// 自定义配置
options.DirPath = "./custom-data"
options.DataFileSize = 256 * 1024 * 1024 // 256MB
options.MemoryIndexType = cfg.BTree
```

### 数据库操作模式
```go
// 基本操作
db.Put([]byte("key"), []byte("value"))
value, err := db.Get([]byte("key"))
db.Delete([]byte("key"))

// 批量操作
batch := db.NewWriteBatch(&WriteBatchOptions{})
batch.Put(key1, value1)
batch.Put(key2, value2)
batch.Commit()
```

### 测试约定
- 测试函数使用 `destroyDB(db)` 清理测试数据
- 使用临时目录避免测试冲突：`os.MkdirTemp("", "test-prefix")`
- 测试数据文件大小通常设为 64MB 便于测试

## 重要约定

### 文件命名
- 数据文件：`{fileId:010d}.data` (如 `0000000001.data`)
- Hint 文件：`hint-index`
- 合并标识：`merge-finished`
- 序列号文件：`seq-no`

### 索引类型选择
- `cfg.BTree` - 内存 B 树索引（默认）
- `cfg.ART` - 自适应基数树索引
- `cfg.BPTree` - B+ 树索引（支持持久化）

### IO 类型
- `cfg.IO_FILE` - 标准文件 IO
- `cfg.IO_MMAP` - 内存映射 IO（启动时映射）

### 错误处理
所有自定义错误在 `pkg/errs/kv_error.go` 中定义，使用 `errors.Is()` 进行比较：
```go
if errors.Is(err, errs.ErrKeyNotFound) {
    // 处理键不存在的情况
}
```

## 关键实现细节

### 事务和序列号
- 使用递增的序列号 (`seqNo`) 保证操作顺序
- B+Tree 索引模式需要序列号文件存在
- 批量写操作通过序列号实现原子性

### 数据合并 (Merge)
- 通过 `db.Merge()` 手动触发合并
- 合并过程中设置 `isMerging` 标志防止并发
- 生成 hint 文件加速索引重建

### 并发控制
- 使用读写锁 (`sync.RWMutex`) 保护数据库状态
- 文件锁 (`github.com/gofrs/flock`) 防止多进程访问

## 扩展点
- 新增索引类型需实现 `Indexer` 接口
- 新增 IO 类型需实现 `IOManager` 接口
- 日志记录类型可扩展 `LogRecordType`

## 未来架构指导

### 图数据库层设计原则
- 在当前键值存储基础上构建图语义
- 节点和边的存储格式要考虑查询性能
- 图遍历算法要充分利用底层索引优势

### 分布式设计考虑
- Raft 共识算法实现数据一致性
- 参考 TiDB 的 PD (Placement Driver) 架构进行元数据管理
- 数据分片策略要考虑图查询的局部性

### 商业级特性要求
- 高可用性：多副本、故障恢复
- 高性能：读写分离、缓存策略
- 可扩展性：水平扩容、负载均衡
- 数据安全：加密存储、访问控制

参考 `examples/kv_operation.go` 了解基本使用模式。
