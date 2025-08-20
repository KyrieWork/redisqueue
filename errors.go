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

// Wrap errors with context information
type RedisQueueError struct {
	Op  string // operation name
	Err error  // original error
}

func (e *RedisQueueError) Error() string {
	return fmt.Sprintf("redisqueue %s: %v", e.Op, e.Err)
}

func (e *RedisQueueError) Unwrap() error {
	return e.Err
}

// Create error with context
func NewError(op string, err error) *RedisQueueError {
	return &RedisQueueError{Op: op, Err: err}
}
