package common

// iterator
type Iterator[K any, V any] interface {
	Next() bool
	Key() K
	Value() V
}

type SortTable[K any, V any] interface {
	Len() uint32
	Add(key K, value V) error
	Get(key K) (V, error)
	Update(key K, value V) error
	Remove(key K) error
	Iter() Iterator[K, V]
}
