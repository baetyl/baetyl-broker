package utils

import (
	"fmt"
	"time"

	"github.com/baetyl/baetyl-broker/utils/log"
)

// Trace print elapsed time
func Trace(format string, args ...interface{}) func() {
	start := time.Now()
	return func() {
		format += fmt.Sprintf(" <-- elapsed time: %v", time.Since(start))
		log.Debugf(format, args...)
	}
}
