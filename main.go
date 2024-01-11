package main

import "github.com/kamijoucen/hifidb/kv"

func main() {

	lsm := kv.NewLsmManager()

	lsm.Add([]byte("a"), []byte("1"))
	lsm.Add([]byte("b"), []byte("2"))

}
