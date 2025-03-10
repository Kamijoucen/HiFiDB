package kv

// IteratorOptions 迭代器选项
type IteratorOptions struct {
	Prefix  []byte // 遍历的key前缀
	Reverse bool   // 是否逆序遍历
}

func WithPrefix(prefix []byte) func(*IteratorOptions) {
	return func(options *IteratorOptions) {
		options.Prefix = prefix
	}
}

func WithReverse(reverse bool) func(*IteratorOptions) {
	return func(options *IteratorOptions) {
		options.Reverse = reverse
	}
}

func NewIteratorOptions(opts ...func(*IteratorOptions)) *IteratorOptions {
	options := &IteratorOptions{
		Prefix:  nil,
		Reverse: false,
	}

	for _, opt := range opts {
		opt(options)
	}

	return options
}

// WriteBatchOptions 写批量操作选项
type WriteBatchOptions struct {
	MaxBatchSize   int  // 最大批量大小
	EachSyncWrites bool // 每次写操作是否同步
}

func WithMaxBatchSize(size int) func(*WriteBatchOptions) {
	return func(options *WriteBatchOptions) {
		options.MaxBatchSize = size
	}
}

func WithEachSyncWrites(sync bool) func(*WriteBatchOptions) {
	return func(options *WriteBatchOptions) {
		options.EachSyncWrites = sync
	}
}

func NewWriteBatchOptions(opts ...func(*WriteBatchOptions)) *WriteBatchOptions {
	options := &WriteBatchOptions{
		MaxBatchSize:   0,
		EachSyncWrites: false,
	}

	for _, opt := range opts {
		opt(options)
	}

	return options
}

func GetDefaultWriteBatchOptions() *WriteBatchOptions {
	return NewWriteBatchOptions(WithMaxBatchSize(10000), WithEachSyncWrites(true))
}

func GetDefaultIteratorOptions() *IteratorOptions {
	return NewIteratorOptions()
}
