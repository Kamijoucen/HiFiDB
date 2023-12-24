package kv

type SortTable[K any, V any] interface {
	Add(key K, value V) error
	Get(key K) (V, error)
	Update(key K, value V) error
	Remove(key K) error
}
