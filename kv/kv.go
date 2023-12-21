package kv

type KV interface {
	Get(key []byte) ([]byte, error)
	Put(key []byte, value []byte) error
	Delete(key []byte) error
	BatchGet(keys [][]byte) ([][]byte, error)
	BatchPut(keys [][]byte, values [][]byte) error
	BatchDelete(keys [][]byte) error

	// Range(beginKey []byte, endKey []byte) (Iterator, error)
	// Prefix(prefix []byte) (Iterator, error)
	// BatchRange(beginKey []byte, endKey []byte) (Iterator, error)
	Close() error
}
