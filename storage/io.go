package storage

import (
	"os"

	"github.com/kamijoucen/hifidb/kv"
)

func WriteSSTableToFile(path string, table *kv.SSTable) {
	file := checkAndCreateFile(path)
	defer file.Close()
	bytes, _ := kv.EnCodeSSTable(table)
	file.Write(bytes)
}

func AppendSSTableToFile(file *os.File, table *kv.SSTable) {
	bytes, _ := kv.EnCodeSSTable(table)
	file.Write(bytes)
}

// create file
func checkAndCreateFile(path string) *os.File {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		file, err := os.Create(path)
		if err != nil {
			panic(err)
		}
		return file
	}
	return nil
}
