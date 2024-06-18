package kv

type DB struct {
	path string
}

func Open() *DB {
	return &DB{
		path: "~/tempdb",
	}
}

// Put
func (db *DB) Put(key, value []byte) error {
	return nil
}

// Get
func (db *DB) Get(key []byte) ([]byte, error) {
	return nil, nil
}

// Delete
func (db *DB) Delete(key []byte) error {
	return nil
}

// Close
func (db *DB) Close() error {
	return nil
}
