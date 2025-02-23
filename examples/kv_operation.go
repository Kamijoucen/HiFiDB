package main

import (
	"errors"

	"github.com/kamijoucen/hifidb/pkg/cfg"
	"github.com/kamijoucen/hifidb/pkg/errs"
	"github.com/kamijoucen/hifidb/pkg/kv"
)

func main() {
	options := cfg.GetDefaultOptions()

	db, err := kv.Open(options)
	if err != nil {
		panic(err)
	}

	err = db.Put([]byte("key"), []byte("hello world"))
	if err != nil {
		panic(err)
	}

	value, err := db.Get([]byte("key"))
	if err != nil {
		panic(err)
	}

	println(string(value))

	err = db.Delete([]byte("key"))
	if err != nil {
		panic(err)
	}

	value, err = db.Get([]byte("key"))
	if errors.Is(err, errs.ErrKeyNotFound) {
		println("key not found")
	}

	println(string(value))
}
