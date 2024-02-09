package kv

import "github.com/kamijoucen/hifidb/pkg/config"


type DBConfig config.DBConfig

type DB struct {
	Config *DBConfig
}

func Open(config *DBConfig) *DB {
	return nil
}
