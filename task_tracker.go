package redisqueue

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// TaskStatus represents the current state of a task
type TaskStatus string

const (
	// TaskStatusQueued indicates the task is waiting to be processed
	TaskStatusQueued TaskStatus = "queued"

	// TaskStatusProcessing indicates the task is currently being processed
	TaskStatusProcessing TaskStatus = "processing"

	// TaskStatusCompleted indicates the task has been successfully completed
	TaskStatusCompleted TaskStatus = "completed"

	// TaskStatusFailed indicates the task processing failed
	TaskStatusFailed TaskStatus = "failed"

	// TaskStatusCancelled indicates the task was cancelled
	TaskStatusCancelled TaskStatus = "cancelled"
)

// TaskInfo contains comprehensive information about a task
type TaskInfo struct {
	ID          string                 `json:"id"`
	Status      TaskStatus             `json:"status"`
	Payload     map[string]interface{} `json:"payload"`
	CreatedAt   time.Time              `json:"created_at"`
	StartedAt   *time.Time             `json:"started_at,omitempty"`
	CompletedAt *time.Time             `json:"completed_at,omitempty"`
	Error       string                 `json:"error,omitempty"`
	Progress    int                    `json:"progress"`
	RetryCount  int                    `json:"retry_count"`
}

// TaskTracker manages task states using Redis Hash for fast access
type TaskTracker struct {
	redis      redis.UniversalClient
	keyPrefix  string
	expiration time.Duration
}

// TaskTrackerOptions configures a TaskTracker instance
type TaskTrackerOptions struct {
	// RedisClient allows providing a custom Redis client
	RedisClient redis.UniversalClient

	// RedisOptions configures Redis connection when RedisClient is not provided
	RedisOptions *RedisOptions

	// KeyPrefix is prepended to all task keys (default: "task:")
	KeyPrefix string

	// Expiration defines how long completed/failed tasks are kept (default: 24h)
	Expiration time.Duration
}

// NewTaskTracker creates a new TaskTracker with default settings
func NewTaskTracker() (*TaskTracker, error) {
	return NewTaskTrackerOptions(&TaskTrackerOptions{})
}

// NewTaskTrackerOptions creates a new TaskTracker with custom options
func NewTaskTrackerOptions(opts *TaskTrackerOptions) (*TaskTracker, error) {
	var r redis.UniversalClient

	if opts.RedisClient != nil {
		r = opts.RedisClient
	} else {
		r = NewRedisClient(opts.RedisOptions)
	}

	if err := redisPreflightChecks(context.Background(), r); err != nil {
		return nil, err
	}

	keyPrefix := opts.KeyPrefix
	if keyPrefix == "" {
		keyPrefix = "task:"
	}

	expiration := opts.Expiration
	if expiration == 0 {
		expiration = 24 * time.Hour
	}

	return &TaskTracker{
		redis:      r,
		keyPrefix:  keyPrefix,
		expiration: expiration,
	}, nil
}

// CreateTask creates a new task and enqueues it for processing
func (t *TaskTracker) CreateTask(ctx context.Context, taskID string, stream string, payload map[string]interface{}) error {
	now := time.Now()

	// Store task info in Redis Hash
	key := t.keyPrefix + taskID
	fields := map[string]interface{}{
		"status":      string(TaskStatusQueued),
		"created_at":  now.Unix(),
		"progress":    0,
		"retry_count": 0,
	}

	// Store payload as JSON
	if payloadJSON, err := json.Marshal(payload); err == nil {
		fields["payload"] = string(payloadJSON)
	}

	err := t.redis.HMSet(ctx, key, fields).Err()
	if err != nil {
		return NewError("create_task", err)
	}

	return nil
}

// GetTask retrieves complete task information
func (t *TaskTracker) GetTask(ctx context.Context, taskID string) (*TaskInfo, error) {
	key := t.keyPrefix + taskID

	fields, err := t.redis.HGetAll(ctx, key).Result()
	if err != nil {
		return nil, NewError("get_task", err)
	}

	if len(fields) == 0 {
		return nil, fmt.Errorf("task %s not found", taskID)
	}

	return t.parseTaskInfo(taskID, fields)
}

// GetTaskStatus quickly retrieves just the task status
func (t *TaskTracker) GetTaskStatus(ctx context.Context, taskID string) (TaskStatus, error) {
	key := t.keyPrefix + taskID

	status, err := t.redis.HGet(ctx, key, "status").Result()
	if err != nil {
		if err == redis.Nil {
			return "", fmt.Errorf("task %s not found", taskID)
		}
		return "", NewError("get_task_status", err)
	}

	return TaskStatus(status), nil
}

// UpdateTaskStatus updates the status of a task
func (t *TaskTracker) UpdateTaskStatus(ctx context.Context, taskID string, status TaskStatus) error {
	key := t.keyPrefix + taskID
	now := time.Now()

	fields := map[string]interface{}{
		"status": string(status),
	}

	// Set timestamps based on status
	switch status {
	case TaskStatusProcessing:
		fields["started_at"] = now.Unix()
	case TaskStatusCompleted, TaskStatusFailed, TaskStatusCancelled:
		fields["completed_at"] = now.Unix()
		// Set expiration for finished tasks
		t.redis.Expire(ctx, key, t.expiration)
	}

	err := t.redis.HMSet(ctx, key, fields).Err()
	if err != nil {
		return NewError("update_task_status", err)
	}

	return nil
}

// UpdateTaskProgress updates the progress percentage of a task
func (t *TaskTracker) UpdateTaskProgress(ctx context.Context, taskID string, progress int) error {
	if progress < 0 {
		progress = 0
	}
	if progress > 100 {
		progress = 100
	}

	key := t.keyPrefix + taskID
	err := t.redis.HSet(ctx, key, "progress", progress).Err()
	if err != nil {
		return NewError("update_task_progress", err)
	}

	return nil
}

// MarkTaskFailed marks a task as failed with an error message
func (t *TaskTracker) MarkTaskFailed(ctx context.Context, taskID string, errMsg string) error {
	key := t.keyPrefix + taskID

	fields := map[string]interface{}{
		"status":       string(TaskStatusFailed),
		"completed_at": time.Now().Unix(),
		"error":        errMsg,
	}

	err := t.redis.HMSet(ctx, key, fields).Err()
	if err != nil {
		return NewError("mark_task_failed", err)
	}

	// Set expiration for failed task
	t.redis.Expire(ctx, key, t.expiration)
	return nil
}

// IncrementRetryCount increments the retry count for a task
func (t *TaskTracker) IncrementRetryCount(ctx context.Context, taskID string) (int, error) {
	key := t.keyPrefix + taskID

	count, err := t.redis.HIncrBy(ctx, key, "retry_count", 1).Result()
	if err != nil {
		return 0, NewError("increment_retry_count", err)
	}

	return int(count), nil
}

// ListTasksByStatus returns task IDs with the specified status
func (t *TaskTracker) ListTasksByStatus(ctx context.Context, status TaskStatus, limit int) ([]string, error) {
	// Note: This is a simple implementation using SCAN
	// For production with millions of tasks, consider using Redis Sets or Sorted Sets

	var taskIDs []string
	iter := t.redis.Scan(ctx, 0, t.keyPrefix+"*", int64(limit*2)).Iterator()

	for iter.Next(ctx) && len(taskIDs) < limit {
		key := iter.Val()
		taskStatus, err := t.redis.HGet(ctx, key, "status").Result()
		if err != nil {
			continue
		}

		if TaskStatus(taskStatus) == status {
			// Extract task ID from key
			taskID := key[len(t.keyPrefix):]
			taskIDs = append(taskIDs, taskID)
		}
	}

	if err := iter.Err(); err != nil {
		return nil, NewError("list_tasks_by_status", err)
	}

	return taskIDs, nil
}

// CleanupCompletedTasks removes completed/failed tasks older than the expiration time
func (t *TaskTracker) CleanupCompletedTasks(ctx context.Context) (int, error) {
	cutoff := time.Now().Add(-t.expiration).Unix()

	var deletedCount int
	iter := t.redis.Scan(ctx, 0, t.keyPrefix+"*", 1000).Iterator()

	for iter.Next(ctx) {
		key := iter.Val()

		// Check if task is completed/failed and old enough
		fields, err := t.redis.HMGet(ctx, key, "status", "completed_at").Result()
		if err != nil || len(fields) != 2 {
			continue
		}

		status := fields[0]
		completedAtStr := fields[1]

		if status != string(TaskStatusCompleted) && status != string(TaskStatusFailed) {
			continue
		}

		if completedAtStr == nil {
			continue
		}

		if completedAt, ok := completedAtStr.(string); ok {
			if timestamp := parseInt64(completedAt); timestamp > 0 && timestamp < cutoff {
				if err := t.redis.Del(ctx, key).Err(); err == nil {
					deletedCount++
				}
			}
		}
	}

	if err := iter.Err(); err != nil {
		return deletedCount, NewError("cleanup_completed_tasks", err)
	}

	return deletedCount, nil
}

// parseTaskInfo converts Redis hash fields to TaskInfo struct
func (t *TaskTracker) parseTaskInfo(taskID string, fields map[string]string) (*TaskInfo, error) {
	info := &TaskInfo{
		ID: taskID,
	}

	if status, ok := fields["status"]; ok {
		info.Status = TaskStatus(status)
	}

	if createdAt := parseInt64(fields["created_at"]); createdAt > 0 {
		info.CreatedAt = time.Unix(createdAt, 0)
	}

	if startedAt := parseInt64(fields["started_at"]); startedAt > 0 {
		t := time.Unix(startedAt, 0)
		info.StartedAt = &t
	}

	if completedAt := parseInt64(fields["completed_at"]); completedAt > 0 {
		t := time.Unix(completedAt, 0)
		info.CompletedAt = &t
	}

	if err, ok := fields["error"]; ok {
		info.Error = err
	}

	if progress := parseInt64(fields["progress"]); progress >= 0 {
		info.Progress = int(progress)
	}

	if retryCount := parseInt64(fields["retry_count"]); retryCount >= 0 {
		info.RetryCount = int(retryCount)
	}

	if payloadJSON, ok := fields["payload"]; ok && payloadJSON != "" {
		var payload map[string]interface{}
		if err := json.Unmarshal([]byte(payloadJSON), &payload); err == nil {
			info.Payload = payload
		}
	}

	return info, nil
}

// Helper function to parse int64 from string
func parseInt64(s string) int64 {
	var result int64
	fmt.Sscanf(s, "%d", &result)
	return result
}
