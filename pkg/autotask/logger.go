package autotask

import (
	"fmt"
	"io"
	"os"
)

// LogLevel represents the severity level of a log message
type LogLevel int

const (
	// Debug level for detailed information
	Debug LogLevel = iota
	// Info level for general operational information
	Info
	// Warn level for warning messages
	Warn
	// Error level for error messages
	Error
)

// Logger handles logging operations
type Logger struct {
	level  LogLevel
	output io.Writer
}

// NewLogger creates a new Logger instance
func NewLogger(level LogLevel, output io.Writer) *Logger {
	if output == nil {
		output = os.Stdout
	}
	return &Logger{
		level:  level,
		output: output,
	}
}

// formatLogMessage formats a log message with its level and fields
func (l *Logger) formatLogMessage(level LogLevel, msg string, fields map[string]interface{}) string {
	// Basic format: [LEVEL] message {field1: value1, field2: value2}
	fieldStr := ""
	if len(fields) > 0 {
		fieldStr = " {"
		for k, v := range fields {
			fieldStr += fmt.Sprintf("%s: %v, ", k, v)
		}
		fieldStr = fieldStr[:len(fieldStr)-2] + "}"
	}
	return fmt.Sprintf("[%v] %s%s", level, msg, fieldStr)
}

// log writes a log message to the output
func (l *Logger) log(level LogLevel, msg string, fields map[string]interface{}) {
	if level < l.level {
		return
	}

	logMsg := l.formatLogMessage(level, msg, fields)
	if _, err := fmt.Fprintln(l.output, logMsg); err != nil {
		// If we can't write to the output, write to stderr as a fallback
		fmt.Fprintf(os.Stderr, "Failed to write log message: %v\n", err)
	}
}
