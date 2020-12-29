package rocksdb

// CloudConfig baetyl-cloud config
type DBConfig struct {
	RocksDB RocksDB `yaml:"rocksdb" json:"rocksdb"`
}

type RocksDB struct {
	KVMaxBackgroundCompactions         int    `yaml:"kvMaxBackgroundCompactions" json:"kvMaxBackgroundCompactions" default:"2"`
	KVMaxBackgroundFlushes             int    `yaml:"kvMaxBackgroundFlushes" json:"kvMaxBackgroundFlushes" default:"2"`
	KVKeepLogFileNum                   int    `yaml:"kvKeepLogFileNum" json:"kvKeepLogFileNum" default:"16"`
	KVLRUCacheSize                     uint64 `yaml:"kvLRUCacheSize" json:"kvLRUCacheSize" default:"8388608"`        // 8M
	KVWriteBufferSize                  int    `yaml:"kvWriteBufferSize" json:"kvWriteBufferSize" default:"67108864"` // 64M
	KVMaxWriteBufferNumber             int    `yaml:"kvMaxWriteBufferNumber" json:"kvMaxWriteBufferNumber" default:"5"`
	KVMinWriteBufferNumberToMerge      int    `yaml:"kvMinWriteBufferNumberToMerge" json:"kvMinWriteBufferNumberToMerge" default:"2"`
	KVLevel0FileNumCompactionTrigger   int    `yaml:"kvLevel0FileNumCompactionTrigger" json:"kvLevel0FileNumCompactionTrigger" default:"4"`
	KVLevel0SlowdownWritesTrigger      int    `yaml:"kvLevel0SlowdownWritesTrigger" json:"kvLevel0SlowdownWritesTrigger" default:"17"`
	KVLevel0StopWritesTrigger          int    `yaml:"kvLevel0StopWritesTrigger" json:"kvLevel0StopWritesTrigger" default:"24"`
	KVMaxBytesForLevelBase             uint64 `yaml:"kvMaxBytesForLevelBase" json:"kvMaxBytesForLevelBase" default:"536870912"` // 64M * 2 * 4 = 512M
	KVMaxBytesForLevelMultiplier       int    `yaml:"kvMaxBytesForLevelMultiplier" json:"kvMaxBytesForLevelMultiplier" default:"10"`
	KVTargetFileSizeBase               uint64 `yaml:"kvTargetFileSizeBase" json:"kvTargetFileSizeBase" default:"33554432"` // 32M
	KVTargetFileSizeMultiplier         int    `yaml:"kvTargetFileSizeMultiplier" json:"kvTargetFileSizeMultiplier" default:"1"`
	KVLevelCompactionDynamicLevelBytes int    `yaml:"kvLevelCompactionDynamicLevelBytes" json:"kvLevelCompactionDynamicLevelBytes"`
	KVNumOfLevels                      int    `yaml:"kvNumOfLevels" json:"kvNumOfLevels" default:"7"`
	KVBlockSize                        int    `yaml:"kvBlockSize" json:"kvBlockSize" default:"4096"`                       // 4K
	MaxLogFileSize                     int    `yaml:"maxLogFileSize" json:"maxLogFileSize" default:"1073741824"`           // 1G
	MaxManifestFileSize                uint64 `yaml:"maxManifestFileSize" json:"maxManifestFileSize" default:"1073741824"` // 1G
}
