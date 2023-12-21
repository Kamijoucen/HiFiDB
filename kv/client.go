package kv

type kvClient struct {
}

func NewKV() *kvClient {
	return &kvClient{}
}

func (client *kvClient) Get(key []byte) ([]byte, error) {
	panic("not implemented") // TODO: Implement
}

func (client *kvClient) Put(key []byte, value []byte) error {
	panic("not implemented") // TODO: Implement
}

func (client *kvClient) Delete(key []byte) error {
	panic("not implemented") // TODO: Implement
}

func (client *kvClient) BatchGet(keys [][]byte) ([][]byte, error) {
	panic("not implemented") // TODO: Implement
}

func (client *kvClient) BatchPut(keys [][]byte, values [][]byte) error {
	panic("not implemented") // TODO: Implement
}

func (client *kvClient) BatchDelete(keys [][]byte) error {
	panic("not implemented") // TODO: Implement
}

// Range(beginKey []byte, endKey []byte) (Iterator, error)
// Prefix(prefix []byte) (Iterator, error)
// BatchRange(beginKey []byte, endKey []byte) (Iterator, error)
func (client *kvClient) Close() error {
	panic("not implemented") // TODO: Implement
}
