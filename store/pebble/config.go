package pebble

// CloudConfig baetyl-cloud config
type DBConfig struct {
	Pebble struct {
		KVKeepLogFileNum                   uint64 `yaml:"kvKeepLogFileNum" json:"kvKeepLogFileNum"`
		KVMaxBackgroundCompactions         uint64 `yaml:"kvMaxBackgroundCompactions" json:"kvMaxBackgroundCompactions"`
		KVMaxBackgroundFlushes             uint64 `yaml:"kvMaxBackgroundFlushes" json:"kvMaxBackgroundFlushes"`
		KVLRUCacheSize                     uint64 `yaml:"kvLRUCacheSize" json:"kvLRUCacheSize"`
		KVWriteBufferSize                  uint64 `yaml:"kvWriteBufferSize" json:"kvWriteBufferSize"`
		KVMaxWriteBufferNumber             uint64 `yaml:"kvMaxWriteBufferNumber" json:"kvMaxWriteBufferNumber"`
		KVLevel0FileNumCompactionTrigger   uint64 `yaml:"kvLevel0FileNumCompactionTrigger" json:"kvLevel0FileNumCompactionTrigger"`
		KVLevel0SlowdownWritesTrigger      uint64 `yaml:"kvLevel0SlowdownWritesTrigger" json:"kvLevel0SlowdownWritesTrigger"`
		KVLevel0StopWritesTrigger          uint64 `yaml:"kvLevel0StopWritesTrigger" json:"kvLevel0StopWritesTrigger"`
		KVMaxBytesForLevelBase             uint64 `yaml:"kvMaxBytesForLevelBase" json:"kvMaxBytesForLevelBase"`
		KVMaxBytesForLevelMultiplier       uint64 `yaml:"kvMaxBytesForLevelMultiplier" json:"kvMaxBytesForLevelMultiplier"`
		KVTargetFileSizeBase               uint64 `yaml:"kvTargetFileSizeBase" json:"kvTargetFileSizeBase"`
		KVTargetFileSizeMultiplier         uint64 `yaml:"kvTargetFileSizeMultiplier" json:"kvTargetFileSizeMultiplier"`
		KVLevelCompactionDynamicLevelBytes uint64 `yaml:"kvLevelCompactionDynamicLevelBytes" json:"kvLevelCompactionDynamicLevelBytes"`
		KVRecycleLogFileNum                uint64 `yaml:"kvRecycleLogFileNum" json:"kvRecycleLogFileNum"`
		KVNumOfLevels                      uint64 `yaml:"kvNumOfLevels" json:"kvNumOfLevels"`
		KVBlockSize                        uint64 `yaml:"kvBlockSize" json:"kvBlockSize"`
		SaveBufferSize                     uint64 `yaml:"saveBufferSize" json:"saveBufferSize"`
		MaxSaveBufferSize                  uint64 `yaml:"maxSaveBufferSize" json:"maxSaveBufferSize"`
	} `yaml:"pebble" json:"pebble"`
}
