package kv

type KVClient struct {
}

func NewKV() *KVClient {
	return &KVClient{}
}

func (client *KVClient) Get(key []byte) ([]byte, error) {
	panic("not implemented") // TODO: Implement
}

func (client *KVClient) Put(key []byte, value []byte) error {
	panic("not implemented") // TODO: Implement
}

func (client *KVClient) Delete(key []byte) error {
	panic("not implemented") // TODO: Implement
}

func (client *KVClient) BatchGet(keys [][]byte) ([][]byte, error) {
	panic("not implemented") // TODO: Implement
}

func (client *KVClient) BatchPut(keys [][]byte, values [][]byte) error {
	panic("not implemented") // TODO: Implement
}

func (client *KVClient) BatchDelete(keys [][]byte) error {
	panic("not implemented") // TODO: Implement
}

// Range(beginKey []byte, endKey []byte) (Iterator, error)
// Prefix(prefix []byte) (Iterator, error)
// BatchRange(beginKey []byte, endKey []byte) (Iterator, error)
func (client *KVClient) Close() error {
	panic("not implemented") // TODO: Implement
}
