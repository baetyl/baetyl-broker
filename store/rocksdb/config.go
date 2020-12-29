package rocksdb

// CloudConfig baetyl-cloud config
type DBConfig struct {
	rocksDB RocksDB `yaml:"rocksdb" json:"rocksdb"`
}

type RocksDB struct {
	KVMaxBackgroundCompactions         uint64 `yaml:"kvMaxBackgroundCompactions" json:"kvMaxBackgroundCompactions" default:"2"`
	KVMaxBackgroundFlushes             uint64 `yaml:"kvMaxBackgroundFlushes" json:"kvMaxBackgroundFlushes" default:"2"`
	KVKeepLogFileNum                   uint64 `yaml:"kvKeepLogFileNum" json:"kvKeepLogFileNum" default:"16"`
	KVLRUCacheSize                     uint64 `yaml:"kvLRUCacheSize" json:"kvLRUCacheSize" default:"8388608"`        // 8M
	KVWriteBufferSize                  uint64 `yaml:"kvWriteBufferSize" json:"kvWriteBufferSize" default:"67108864"` // 64M
	KVMaxWriteBufferNumber             uint64 `yaml:"kvMaxWriteBufferNumber" json:"kvMaxWriteBufferNumber" default:"5"`
	KVMinWriteBufferNumberToMerge      uint64 `yaml:"kvMinWriteBufferNumberToMerge" json:"kvMinWriteBufferNumberToMerge" default:"2"`
	KVLevel0FileNumCompactionTrigger   uint64 `yaml:"kvLevel0FileNumCompactionTrigger" json:"kvLevel0FileNumCompactionTrigger" default:"4"`
	KVLevel0SlowdownWritesTrigger      uint64 `yaml:"kvLevel0SlowdownWritesTrigger" json:"kvLevel0SlowdownWritesTrigger" default:"17"`
	KVLevel0StopWritesTrigger          uint64 `yaml:"kvLevel0StopWritesTrigger" json:"kvLevel0StopWritesTrigger" default:"24"`
	KVMaxBytesForLevelBase             uint64 `yaml:"kvMaxBytesForLevelBase" json:"kvMaxBytesForLevelBase" default:"536870912"` // 64M * 2 * 4 = 512M
	KVMaxBytesForLevelMultiplier       uint64 `yaml:"kvMaxBytesForLevelMultiplier" json:"kvMaxBytesForLevelMultiplier" default:"10"`
	KVTargetFileSizeBase               uint64 `yaml:"kvTargetFileSizeBase" json:"kvTargetFileSizeBase" default:"33554432"` // 32M
	KVTargetFileSizeMultiplier         uint64 `yaml:"kvTargetFileSizeMultiplier" json:"kvTargetFileSizeMultiplier" default:"1"`
	KVLevelCompactionDynamicLevelBytes uint64 `yaml:"kvLevelCompactionDynamicLevelBytes" json:"kvLevelCompactionDynamicLevelBytes"`
	KVNumOfLevels                      uint64 `yaml:"kvNumOfLevels" json:"kvNumOfLevels" default:"7"`
	KVBlockSize                        uint64 `yaml:"kvBlockSize" json:"kvBlockSize" default:"4096"`                       // 4K
	MaxLogFileSize                     uint64 `yaml:"maxLogFileSize" json:"maxLogFileSize" default:"1073741824"`           // 1G
	MaxManifestFileSize                uint64 `yaml:"maxManifestFileSize" json:"maxManifestFileSize" default:"1073741824"` // 1G
}
