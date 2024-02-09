# 适合高频文件写入的图形数据库

## 整体结构

![结构图](docs/pic/hifidb.png)

## 存储基本数据块结构
![结构图](docs/pic/sst.png)

## 数据写入流程
![结构图](docs/pic/sstwrite.png)

## 操作语言
```json

// 准备兼容Cypher，SPARQL

```


## 开发计划
1. 事务
2. 索引（底层参考tidb的LSM-Tree方案）
3. 分布式存储（参考tidb的raft方案）
4. MVCC
5. GIS