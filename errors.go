package redisqueue

import (
	"errors"
	"fmt"
)

// Common errors returned by redisqueue operations.
var (
	ErrNoConsumers      = errors.New("at least one consumer function needs to be registered")
	ErrGroupExists      = errors.New("consumer group already exists")
	ErrRedisConnection  = errors.New("redis connection failed")
	ErrStreamNotFound   = errors.New("stream not found")
	ErrInvalidMessageID = errors.New("invalid message ID format")
)

// IsGroupExistsError checks if an error indicates a Redis consumer group already exists.
// This handles Redis-specific error messages that may vary across versions.
func IsGroupExistsError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return errStr == "BUSYGROUP Consumer Group name already exists"
}

// ErrorLevel classifies errors by severity for appropriate handling and monitoring.
type ErrorLevel int

const (
	// ErrorLevelInfo indicates informational messages for monitoring and debugging.
	ErrorLevelInfo ErrorLevel = iota

	// ErrorLevelWarning indicates recoverable errors that don't stop processing.
	ErrorLevelWarning

	// ErrorLevelCritical indicates severe errors that may require immediate attention.
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

// RedisQueueError provides detailed error information with contextual data
// for debugging and monitoring. It wraps the original error with operation
// details and relevant metadata.
type RedisQueueError struct {
	// Level indicates the severity of the error
	Level ErrorLevel

	// Op identifies the operation that failed (e.g., "process_message", "claim_messages")
	Op string

	// Stream name where the error occurred (empty if not applicable)
	Stream string

	// MessageID of the message being processed when error occurred (empty if not applicable)
	MessageID string

	// Context contains additional debugging information specific to the error
	Context map[string]interface{}

	// Err is the underlying error that caused this failure
	Err error
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

// NewError creates a RedisQueueError with Warning level and minimal context.
// This is a convenience function for simple error wrapping.
func NewError(op string, err error) *RedisQueueError {
	return &RedisQueueError{
		Level: ErrorLevelWarning,
		Op:    op,
		Err:   err,
	}
}

// NewErrorWithContext creates a RedisQueueError with full contextual information.
// Use this for detailed error reporting with stream, message, and operation context.
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
