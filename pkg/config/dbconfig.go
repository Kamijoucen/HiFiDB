package config

var GlobalConfig = loadConfigFile()

func loadConfigFile() *DBConfig {
	// TODO load
	return NewDBConfig()
}

type DBConfig struct {
	DBPath       string
	MEMTableSize uint64
	DBBlockSize  uint64
	SSTableSize  uint64
	L0Size       uint64
	L1Size       uint64
	LevelRatio   uint32
}

type DBConfigOption func(*DBConfig)

// path
func WithDBPath(path string) DBConfigOption {
	return func(config *DBConfig) {
		config.DBPath = path
	}
}

// memtable size
func WithMEMTableSize(size uint64) DBConfigOption {
	return func(config *DBConfig) {
		config.MEMTableSize = size
	}
}

// block size
func WithDBBlockSize(size uint64) DBConfigOption {
	return func(config *DBConfig) {
		config.DBBlockSize = size
	}
}

// L0 size
func WithL0Size(size uint64) DBConfigOption {
	return func(config *DBConfig) {
		config.L0Size = size
	}
}

// L1 size
func WithL1Size(size uint64) DBConfigOption {
	return func(config *DBConfig) {
		config.L1Size = size
	}
}

// level ratio
func WithLevelRatio(ratio uint32) DBConfigOption {
	return func(config *DBConfig) {
		config.LevelRatio = ratio
	}
}

// sstable size
func WithSSTableSize(size uint64) DBConfigOption {
	return func(config *DBConfig) {
		config.SSTableSize = size
	}
}

func NewDBConfig(options ...DBConfigOption) *DBConfig {
	config := &DBConfig{
		DBPath:       "../../tempdb",
		MEMTableSize: 32 * 1024 * 1024,  // 32MB
		DBBlockSize:  32 * 1024,         // 32KB
		SSTableSize:  2 * 1024 * 1024,   // 2MB
		L0Size:       10 * 1024 * 1024,  // 10MB
		L1Size:       100 * 1024 * 1024, // 100MB
		LevelRatio:   10,                // 10:1
	}
	for _, option := range options {
		option(config)
	}
	return config
}
