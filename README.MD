# 适合高频大文件写入的nosql数据库

## 操作语言
```javascript

// 查询
db.table_name.find()
.map({"age": it.field1, "name": it.field2, "sex": it.field3})
.filter(it.age > 15)
.filter(it.name = 'xx' or it.name = 'aa') // 当lambda只 有一个参数时，可以省略 -> 表达式，并且默认参数为it
.sort((it1, it2) -> it1.age > it2.age)
.limit(0, 10);

// 分组
db.table_name.find()
.map({"age": it.field1, "name": it.field2, "sex": it.field3})
.filter(it.age > 15)
.groupMap(["age", "name"], {"age": it.age, "name": it.name, "id": COUNT(it._id)});

// 链接查询
db.table_name.find()
.map({"age": it.field1, "name": it.field2, "sex": it.field3})
.joinMap(db.table_name2, (row1, row2) -> row1.id = row2.oid,
    (row1, row2) -> {age: row1.field1, name: row1.field2, oid: row2.oid})


// 更新
db.table_name.update()
.filter(it.age > 11)
.set("name", "xxxxx")

// 删除
db.table_name.delete()
.filter(it.age > 15)
.filter(it.name = 'xx');

// 保存
db.table_name.save()
.value({
    age: 16
    name: 'hahaha',
    sex: 0
});

```

## 计划支持
1. 事务
2. 索引（底层参考tidb的LSM-Tree方案）
3. 分布式存储（参考tidb的raft方案）
4. MVCC