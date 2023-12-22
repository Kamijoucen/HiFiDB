package kv

type walManager struct {
}

// new
func NewWalManager() *walManager {
	return &walManager{}
}

// write
func (wl *walManager) Write(key string, value []byte) error {
	// TODO
	return nil
}
