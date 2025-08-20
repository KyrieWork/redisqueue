package redisqueue

import (
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"
)

// Metrics contains all performance metrics for the consumer
type Metrics struct {
	// Message processing metrics
	MessagesProcessed int64 `json:"messages_processed"`
	MessagesSucceeded int64 `json:"messages_succeeded"`
	MessagesFailed    int64 `json:"messages_failed"`
	MessagesReclaimed int64 `json:"messages_reclaimed"`

	// Processing time metrics (in milliseconds)
	TotalProcessingTime int64 `json:"total_processing_time_ms"`
	MinProcessingTime   int64 `json:"min_processing_time_ms"`
	MaxProcessingTime   int64 `json:"max_processing_time_ms"`

	// Queue metrics
	CurrentQueueDepth int32 `json:"current_queue_depth"`
	MaxQueueDepth     int32 `json:"max_queue_depth"`

	// Error metrics by type
	ErrorCounts map[string]int64 `json:"error_counts"`

	// System metrics
	StartTime      time.Time `json:"start_time"`
	ActiveWorkers  int32     `json:"active_workers"`
	RedisConnected bool      `json:"redis_connected"`

	// Calculated metrics
	SuccessRate       float64 `json:"success_rate"`
	ErrorRate         float64 `json:"error_rate"`
	AvgProcessingTime float64 `json:"avg_processing_time_ms"`
	MessagesPerSecond float64 `json:"messages_per_second"`

	// Internal state
	mu sync.RWMutex
}

// NewMetrics creates a new metrics collector
func NewMetrics() *Metrics {
	return &Metrics{
		ErrorCounts:       make(map[string]int64),
		StartTime:         time.Now(),
		MinProcessingTime: int64(^uint64(0) >> 1), // Max int64
		RedisConnected:    true,
	}
}

// Processing time tracking
func (m *Metrics) RecordProcessingStart() func() {
	start := time.Now()
	return func() {
		duration := time.Since(start).Milliseconds()
		m.recordProcessingTime(duration)
	}
}

func (m *Metrics) recordProcessingTime(durationMs int64) {
	atomic.AddInt64(&m.TotalProcessingTime, durationMs)

	// Update min/max with atomic operations
	for {
		old := atomic.LoadInt64(&m.MinProcessingTime)
		if durationMs >= old || atomic.CompareAndSwapInt64(&m.MinProcessingTime, old, durationMs) {
			break
		}
	}

	for {
		old := atomic.LoadInt64(&m.MaxProcessingTime)
		if durationMs <= old || atomic.CompareAndSwapInt64(&m.MaxProcessingTime, old, durationMs) {
			break
		}
	}
}

// Message processing metrics
func (m *Metrics) IncrementProcessed() {
	atomic.AddInt64(&m.MessagesProcessed, 1)
}

func (m *Metrics) IncrementSucceeded() {
	atomic.AddInt64(&m.MessagesSucceeded, 1)
}

func (m *Metrics) IncrementFailed() {
	atomic.AddInt64(&m.MessagesFailed, 1)
}

func (m *Metrics) IncrementReclaimed() {
	atomic.AddInt64(&m.MessagesReclaimed, 1)
}

// Queue depth tracking
func (m *Metrics) SetQueueDepth(depth int) {
	current := int32(depth)
	atomic.StoreInt32(&m.CurrentQueueDepth, current)

	// Update max queue depth
	for {
		old := atomic.LoadInt32(&m.MaxQueueDepth)
		if current <= old || atomic.CompareAndSwapInt32(&m.MaxQueueDepth, old, current) {
			break
		}
	}
}

// Error tracking
func (m *Metrics) IncrementError(errorType string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ErrorCounts[errorType]++
}

// Worker tracking
func (m *Metrics) SetActiveWorkers(count int) {
	atomic.StoreInt32(&m.ActiveWorkers, int32(count))
}

func (m *Metrics) SetRedisConnected(connected bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.RedisConnected = connected
}

// GetSnapshot returns a snapshot of current metrics with calculated fields
func (m *Metrics) GetSnapshot() *Metrics {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Create a copy of the metrics
	snapshot := &Metrics{
		MessagesProcessed:   atomic.LoadInt64(&m.MessagesProcessed),
		MessagesSucceeded:   atomic.LoadInt64(&m.MessagesSucceeded),
		MessagesFailed:      atomic.LoadInt64(&m.MessagesFailed),
		MessagesReclaimed:   atomic.LoadInt64(&m.MessagesReclaimed),
		TotalProcessingTime: atomic.LoadInt64(&m.TotalProcessingTime),
		MinProcessingTime:   atomic.LoadInt64(&m.MinProcessingTime),
		MaxProcessingTime:   atomic.LoadInt64(&m.MaxProcessingTime),
		CurrentQueueDepth:   atomic.LoadInt32(&m.CurrentQueueDepth),
		MaxQueueDepth:       atomic.LoadInt32(&m.MaxQueueDepth),
		StartTime:           m.StartTime,
		ActiveWorkers:       atomic.LoadInt32(&m.ActiveWorkers),
		RedisConnected:      m.RedisConnected,
		ErrorCounts:         make(map[string]int64),
	}

	// Copy error counts
	for k, v := range m.ErrorCounts {
		snapshot.ErrorCounts[k] = v
	}

	// Calculate derived metrics
	if snapshot.MessagesProcessed > 0 {
		snapshot.SuccessRate = float64(snapshot.MessagesSucceeded) / float64(snapshot.MessagesProcessed) * 100
		snapshot.ErrorRate = float64(snapshot.MessagesFailed) / float64(snapshot.MessagesProcessed) * 100
		snapshot.AvgProcessingTime = float64(snapshot.TotalProcessingTime) / float64(snapshot.MessagesProcessed)

		// Messages per second since start
		elapsed := time.Since(snapshot.StartTime).Seconds()
		if elapsed > 0 {
			snapshot.MessagesPerSecond = float64(snapshot.MessagesProcessed) / elapsed
		}
	}

	// Handle edge case for min processing time
	if snapshot.MinProcessingTime == int64(^uint64(0)>>1) {
		snapshot.MinProcessingTime = 0
	}

	return snapshot
}

// ToJSON returns metrics as JSON string
func (m *Metrics) ToJSON() (string, error) {
	snapshot := m.GetSnapshot()
	data, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		return "", err
	}
	return string(data), nil
}
