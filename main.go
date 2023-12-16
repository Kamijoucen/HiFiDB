package main

import (
	"fmt"

	"github.com/kamijoucen/hifidb/kv"
)

func main() {
	fmt.Println("hello " + "world")

	mt := kv.NewBstTable()

	mt.Insert(kv.NewBstNode("hello"))

}
