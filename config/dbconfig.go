package config

var GlobalConfig = loadConfigFile()

func loadConfigFile() *DBConfig {
	// TODO load
	return NewDBConfig()
}

type DBConfig struct {
	DBPath     string
	L0Size     uint64
	L1Size     uint64
	LevelRatio uint32
}

type DBConfigOption func(*DBConfig)

// path
func WithDBPath(path string) DBConfigOption {
	return func(config *DBConfig) {
		config.DBPath = path
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

func NewDBConfig(options ...DBConfigOption) *DBConfig {
	config := &DBConfig{
		DBPath:     "./db",
		L0Size:     4 * 1024 * 1024,  // 4MB
		L1Size:     64 * 1024 * 1024, // 64MB
		LevelRatio: 10,               // 10:1
	}
	for _, option := range options {
		option(config)
	}
	return config
}
