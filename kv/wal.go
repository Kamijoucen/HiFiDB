package kv

type WalManager struct {
}

// new
func NewWalManager() *WalManager {
	return &WalManager{}
}

// write
func (wl *WalManager) Write(key string, value []byte) error {
	// TODO
	return nil
}
