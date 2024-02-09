package kv

type WalService struct {
}

// new
func NewWalService() *WalService {
	return &WalService{}
}

// write
func (wl *WalService) Write(key string, value []byte) error {
	// TODO
	return nil
}
