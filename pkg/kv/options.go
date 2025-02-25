package kv

type IteratorOptions struct {

	// Prefix 遍历的key前缀
	Prefix []byte

	// Reverse 是否逆序遍历
	Reverse bool
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

func GetDefaultIteratorOptions() *IteratorOptions {
	return NewIteratorOptions()
}
