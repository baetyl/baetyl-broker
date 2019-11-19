package log

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Level log level
type Level = zapcore.Level

// all log level
const (
	DebugLevel Level = iota - 1
	InfoLevel
	WarnLevel
	ErrorLevel
	FatalLevel
)

// Field lgo field
type Field = zap.Field

var _log *zap.Logger
var _slog *zap.SugaredLogger

func init() {
	// _log, _ = zap.NewProduction(zap.AddCallerSkip(1))
	_log, _ = zap.NewDevelopment(zap.AddCallerSkip(1))
	_slog = _log.Sugar()
}

// String constructs a field with the given key and value.
func String(key string, val string) Field {
	return Field{Key: key, Type: zapcore.StringType, String: val}
}

// Check returns a CheckedEntry if logging a message at the specified level is enabled
func Check(l Level, msg string) *zapcore.CheckedEntry {
	return _log.Check(l, msg)
}

// Debug prints log in debug level
func Debug(msg string) {
	_log.Debug(msg)
}

// Info prints log in info level
func Info(msg string) {
	_log.Info(msg)
}

// Warn prints log in warn level
func Warn(msg string, err error) error {
	_log.Warn(msg, zap.Error(err))
	return err
}

// Error prints log in error level
func Error(msg string, err error) error {
	_log.Error(msg, zap.Error(err))
	return err
}

// Fatal prints log in fatal level
func Fatal(msg string, err error) {
	_log.Fatal(msg, zap.Error(err))
}

// Debugf prints log in debug level
func Debugf(format string, args ...interface{}) {
	_slog.Debugf(format, args...)
}

// Infof prints log in info level
func Infof(format string, args ...interface{}) {
	_slog.Infof(format, args...)
}

// Warnf prints log in warn level
func Warnf(format string, args ...interface{}) {
	_slog.Warnf(format, args...)
}

// Errorf prints log in error level
func Errorf(format string, args ...interface{}) {
	_slog.Errorf(format, args...)
}

// Fatalf prints log in fatal level
func Fatalf(format string, args ...interface{}) {
	_slog.Fatalf(format, args...)
}

// // ErrorIfArises prints error if an error arises
// func ErrorIfArises(msg string, err error) error {
// 	if err != nil {
// 		return nil
// 	}
// 	return Error(msg, err)
// }
