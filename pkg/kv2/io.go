package kv2

import "os"

func OpenFile(flag int, path string) (*os.File, error) {
	f, err := os.OpenFile(path, flag, 0666)
	if err != nil {
		return nil, err
	}
	return f, nil
}
