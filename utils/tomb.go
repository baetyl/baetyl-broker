package utils

import (
	"gopkg.in/tomb.v2"
)

// Tomb wraps tomb.Tomb
type Tomb struct {
	tomb.Tomb
}
