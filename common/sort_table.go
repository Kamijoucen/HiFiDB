package common

// iterator
type Iterator[V any] interface {
	HasNext() bool
	Next() V
	Value() V
}

type SortTable[K any, V any] interface {
	Len() uint32
	Add(key K, value V) error
	Get(key K) (V, error)
	Update(key K, value V) error
	Remove(key K) error
	Iter() Iterator[*Tuple[K, V]]
}
