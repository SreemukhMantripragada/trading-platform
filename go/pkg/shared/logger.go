package shared

import (
	"log"
	"os"
)

// Logger is a thin wrapper to allow DI/testing.
type Logger interface {
	Printf(string, ...any)
	Fatalf(string, ...any)
}

type stdLogger struct{ *log.Logger }

// NewLogger returns a standard logger writing to stdout.
func NewLogger(prefix string) Logger {
	return &stdLogger{log.New(os.Stdout, prefix+" ", log.LstdFlags|log.Lmicroseconds)}
}
