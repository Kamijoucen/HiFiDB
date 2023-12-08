# 适合高频大文件写入的nosql数据库

## 操作语言
```javascript

// 查询
db.table_name
.find
.fields("field1", "field2", "field3")
.and(field -> field.age > 15)
.and(field -> field.name = 'xx');


// 更新
db.table_name
.update
.set("name", "xxxxx")
.and(field.age > 11) // field是内置变量，指代一条row数据

// 删除
db.table_name
.delete
.and(field.age > 15)
.and(field.name = 'xx');

// 保存
db.table_name
.save
.values({

    age: 16
    name: 'hahaha'
    
})

```

## 计划支持
1. 事务
2. 索引（底层参考tidb的LSM-Tree方案）
3. 分布式存储（参考tidb的raft方案）
4. MVCC