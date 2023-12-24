package common

// iterator
type Iterator[K any, V any] interface {
	Next() bool
	Prev() bool
	Key() K
	Value() V
}

type SortTable[K any, V any] interface {
	Add(key K, value V) error
	Get(key K) (V, error)
	Update(key K, value V) error
	Remove(key K) error
	Iter() Iterator[K, V]
}
