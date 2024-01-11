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

func (client *KVClient) BatchPut(keys, values [][]byte) error {
	panic("not implemented") // TODO: Implement
}

func (client *KVClient) BatchDelete(keys [][]byte) error {
	panic("not implemented") // TODO: Implement
}

func (client *KVClient) Close() error {
	panic("not implemented") // TODO: Implement
}
