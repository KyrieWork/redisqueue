package redisqueue

import (
	"errors"
	"fmt"
)

// Define our own error types to avoid those damn hardcoded strings
var (
	ErrNoConsumers      = errors.New("at least one consumer function needs to be registered")
	ErrGroupExists      = errors.New("consumer group already exists")
	ErrRedisConnection  = errors.New("redis connection failed")
	ErrStreamNotFound   = errors.New("stream not found")
	ErrInvalidMessageID = errors.New("invalid message ID format")
)

// Redis-specific error checking function
func IsGroupExistsError(err error) bool {
	if err == nil {
		return false
	}
	// Redis error messages might change, but keywords are more stable
	errStr := err.Error()
	return errStr == "BUSYGROUP Consumer Group name already exists"
}

// Error severity levels - critical errors should stop processing
type ErrorLevel int

const (
	ErrorLevelInfo ErrorLevel = iota
	ErrorLevelWarning
	ErrorLevelCritical
)

func (l ErrorLevel) String() string {
	switch l {
	case ErrorLevelInfo:
		return "INFO"
	case ErrorLevelWarning:
		return "WARN"
	case ErrorLevelCritical:
		return "CRIT"
	default:
		return "UNKNOWN"
	}
}

// Enhanced error with rich context information
type RedisQueueError struct {
	Level     ErrorLevel             // error severity
	Op        string                 // operation name
	Stream    string                 // stream name (if applicable)
	MessageID string                 // message ID (if applicable)
	Context   map[string]interface{} // additional context
	Err       error                  // original error
}

func (e *RedisQueueError) Error() string {
	msg := fmt.Sprintf("[%s] redisqueue %s", e.Level, e.Op)

	if e.Stream != "" {
		msg += fmt.Sprintf(" stream=%s", e.Stream)
	}
	if e.MessageID != "" {
		msg += fmt.Sprintf(" msg_id=%s", e.MessageID)
	}
	if len(e.Context) > 0 {
		msg += fmt.Sprintf(" ctx=%+v", e.Context)
	}

	return fmt.Sprintf("%s: %v", msg, e.Err)
}

func (e *RedisQueueError) Unwrap() error {
	return e.Err
}

func (e *RedisQueueError) IsCritical() bool {
	return e.Level == ErrorLevelCritical
}

// Create error with minimal context
func NewError(op string, err error) *RedisQueueError {
	return &RedisQueueError{
		Level: ErrorLevelWarning,
		Op:    op,
		Err:   err,
	}
}

// Create error with full context
func NewErrorWithContext(level ErrorLevel, op string, err error, stream, messageID string, ctx map[string]interface{}) *RedisQueueError {
	return &RedisQueueError{
		Level:     level,
		Op:        op,
		Stream:    stream,
		MessageID: messageID,
		Context:   ctx,
		Err:       err,
	}
}
