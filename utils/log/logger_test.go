package log

import (
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestLogger(t *testing.T) {
	Debugf("Debugf %d", 1)
	Infof("Infof %d", 2)
	Warnf("Warnf %d", 3)
	Errorf("Errorf %d", 4)
	// zap.Fatalf("Fatalf %d", 5)

	err := Error("failed to do", nil)
	assert.NoError(t, err)
	err = errors.New("xxx")
	err = Error("failed to do", err)
	assert.EqualError(t, err, "xxx")
}

func TestZap(t *testing.T) {
	logger, _ := zap.NewProduction()
	defer logger.Sync()
	logger.Info("failed to xxx", zap.String("t", "1"))
	logger.Debug("xxx", zap.String("t", "1"))
}

func TestError(t *testing.T) {
	logger, _ := zap.NewProduction()
	defer logger.Sync()
	logger.Error("failed to do", zap.Error(do2()))
}

func do1() error {
	return errors.New("1")
}

func do2() error {
	return do1()
}
