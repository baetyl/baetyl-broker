package utils

import (
	"fmt"
	"time"
)

// Trace print elapsed time
func Trace(f func(string, ...interface{}), format string, args ...interface{}) func() {
	start := time.Now()
	return func() {
		format += fmt.Sprintf(" <-- elapsed time: %v", time.Since(start))
		f(format, args...)
	}
}
