package kv2

import "os"

type SST struct {
	ID   uint64
	File *os.File
}
