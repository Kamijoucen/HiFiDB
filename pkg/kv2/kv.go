package kv2

import (
	"os"
	"path/filepath"
	"strconv"

	"github.com/kamijoucen/hifidb/pkg/config"
)

type KV struct {
	CurSST *SST
}

func Open(cfg *config.DBConfig) (*KV, error) {
	// TODO 先默认打开一个sstable
	kv := &KV{}
	id := NextSSTableID(kv)
	path := filepath.Join(cfg.DBPath, strconv.FormatUint(id, 10)+".sst")
	file, err := OpenFile(os.O_RDWR|os.O_CREATE, path)
	if err != nil {
		return nil, err
	}
	kv.CurSST = &SST{
		ID:   id,
		File: file,
	}
	return kv, nil
}

func (k *KV) Put(key, value *string) error {
	return nil
}

func (k *KV) Get(key *string) (*string, error) {
	return nil, nil
}

func (k *KV) Del(key *string) (*string, error) {
	return nil, nil
}
