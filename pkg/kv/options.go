package kv

// IteratorOptions 迭代器选项
type IteratorOptions struct {
	Prefix  []byte // 遍历的key前缀
	Reverse bool   // 是否逆序遍历
}

// WriteBatchOptions 写批量操作选项
type WriteBatchOptions struct {
	MaxBatchSize   int  // 最大批量大小
	EachSyncWrites bool // 每次写操作是否同步
}

func GetDefaultWriteBatchOptions() *WriteBatchOptions {
	return &WriteBatchOptions{
		MaxBatchSize:   10000,
		EachSyncWrites: true,
	}
}

func GetDefaultIteratorOptions() *IteratorOptions {
	return &IteratorOptions{
		Prefix:  nil,
		Reverse: false,
	}
}
