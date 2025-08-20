package redisqueue

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// Integration test that demonstrates the full capabilities of our refactored redis queue
func TestRedisQueueIntegration(t *testing.T) {
	// Skip if no Redis available
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   15, // Use test DB
	})

	ctx := context.Background()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skip("Redis not available:", err)
	}

	// Clean up test data
	defer func() {
		client.FlushDB(ctx)
		client.Close()
	}()

	t.Run("Full Producer-Consumer Flow", func(t *testing.T) {
		testProducerConsumerFlow(t, client)
	})

	t.Run("Error Handling and Recovery", func(t *testing.T) {
		testErrorHandling(t, client)
	})

	t.Run("Performance Metrics", func(t *testing.T) {
		testPerformanceMetrics(t, client)
	})

	t.Run("Graceful Shutdown", func(t *testing.T) {
		testGracefulShutdown(t, client)
	})
}

func testProducerConsumerFlow(t *testing.T, client *redis.Client) {
	const streamName = "test-stream"
	const messageCount = 100

	// Create producer
	producer, err := NewProducerOptions(&ProducerOptions{
		RedisClient: client,
	})
	if err != nil {
		t.Fatal("Failed to create producer:", err)
	}

	// Create consumer
	consumerOpts := &ConsumerOptions{
		Name:        "test-consumer",
		GroupName:   "test-group",
		Concurrency: 3,
		BufferSize:  10,
		RedisClient: client,
	}

	consumer, err := NewConsumerOptions(consumerOpts)
	if err != nil {
		t.Fatal("Failed to create consumer:", err)
	}

	// Track processed messages
	var processedMsgs sync.Map
	var processedCount int32

	// Register consumer function
	consumer.Register(streamName, func(msg *Message) error {
		if data, ok := msg.Values["data"].(string); ok {
			processedMsgs.Store(data, true)
			processedCount++
		}
		// Simulate some processing time
		time.Sleep(10 * time.Millisecond)
		return nil
	})

	// Start consumer in background
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		consumer.Run(ctx)
	}()

	// Give consumer time to start
	time.Sleep(100 * time.Millisecond)

	// Produce messages
	for i := 0; i < messageCount; i++ {
		msg := &Message{
			Stream: streamName,
			Values: map[string]interface{}{
				"data":      fmt.Sprintf("message-%d", i),
				"timestamp": time.Now().Unix(),
			},
		}

		if err := producer.Enqueue(ctx, msg); err != nil {
			t.Fatal("Failed to enqueue message:", err)
		}
	}

	// Wait for processing to complete
	timeout := time.After(30 * time.Second)
	for {
		metrics := consumer.GetMetrics()
		if metrics.MessagesProcessed >= int64(messageCount) {
			break
		}
		select {
		case <-timeout:
			t.Fatal("Timeout waiting for message processing")
		default:
			time.Sleep(100 * time.Millisecond)
		}
	}

	// Validate metrics
	metrics := consumer.GetMetrics()
	if metrics.MessagesProcessed != int64(messageCount) {
		t.Errorf("Expected %d processed messages, got %d", messageCount, metrics.MessagesProcessed)
	}

	if metrics.MessagesSucceeded != int64(messageCount) {
		t.Errorf("Expected %d succeeded messages, got %d", messageCount, metrics.MessagesSucceeded)
	}

	if metrics.MessagesFailed != 0 {
		t.Errorf("Expected 0 failed messages, got %d", metrics.MessagesFailed)
	}

	if metrics.SuccessRate != 100.0 {
		t.Errorf("Expected 100%% success rate, got %.2f%%", metrics.SuccessRate)
	}

	// Validate all messages were processed
	for i := 0; i < messageCount; i++ {
		key := fmt.Sprintf("message-%d", i)
		if _, ok := processedMsgs.Load(key); !ok {
			t.Errorf("Message %s was not processed", key)
		}
	}

	// Test metrics JSON export
	metricsJSON, err := consumer.GetMetricsJSON()
	if err != nil {
		t.Fatal("Failed to get metrics JSON:", err)
	}

	var parsedMetrics map[string]interface{}
	if err := json.Unmarshal([]byte(metricsJSON), &parsedMetrics); err != nil {
		t.Fatal("Failed to parse metrics JSON:", err)
	}

	// Validate JSON contains expected fields
	expectedFields := []string{"messages_processed", "success_rate", "avg_processing_time_ms", "messages_per_second"}
	for _, field := range expectedFields {
		if _, ok := parsedMetrics[field]; !ok {
			t.Errorf("Missing field in metrics JSON: %s", field)
		}
	}

	consumer.Shutdown()
}

func testErrorHandling(t *testing.T, client *redis.Client) {
	const streamName = "error-test-stream"

	consumer, err := NewConsumerOptions(&ConsumerOptions{
		Name:        "error-consumer",
		GroupName:   "error-group",
		Concurrency: 1,
		BufferSize:  5,
		RedisClient: client,
	})
	if err != nil {
		t.Fatal("Failed to create consumer:", err)
	}

	errorCount := 0

	// Register consumer that fails on specific messages
	consumer.Register(streamName, func(msg *Message) error {
		if data, ok := msg.Values["data"].(string); ok {
			if data == "error-message" {
				return fmt.Errorf("simulated error for %s", data)
			}
		}
		return nil
	})

	// Monitor errors
	go func() {
		for err := range consumer.Errors {
			if rqErr, ok := err.(*RedisQueueError); ok {
				if rqErr.Op == "process_message" {
					errorCount++
				}
			}
		}
	}()

	// Start consumer
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		consumer.Run(ctx)
	}()

	time.Sleep(100 * time.Millisecond)

	// Create producer and send mixed messages
	producer, err := NewProducerOptions(&ProducerOptions{
		RedisClient: client,
	})
	if err != nil {
		t.Fatal("Failed to create producer:", err)
	}

	messages := []string{"good-message-1", "error-message", "good-message-2", "error-message", "good-message-3"}

	for _, msgData := range messages {
		msg := &Message{
			Stream: streamName,
			Values: map[string]interface{}{
				"data": msgData,
			},
		}

		if err := producer.Enqueue(ctx, msg); err != nil {
			t.Fatal("Failed to enqueue message:", err)
		}
	}

	// Wait for processing
	time.Sleep(2 * time.Second)

	// Validate error handling
	metrics := consumer.GetMetrics()
	if metrics.MessagesSucceeded != 3 {
		t.Errorf("Expected 3 successful messages, got %d", metrics.MessagesSucceeded)
	}

	if metrics.MessagesFailed != 2 {
		t.Errorf("Expected 2 failed messages, got %d", metrics.MessagesFailed)
	}

	if errorCount != 2 {
		t.Errorf("Expected 2 errors reported, got %d", errorCount)
	}

	consumer.Shutdown()
}

func testPerformanceMetrics(t *testing.T, client *redis.Client) {
	const streamName = "perf-test-stream"

	consumer, err := NewConsumerOptions(&ConsumerOptions{
		Name:        "perf-consumer",
		GroupName:   "perf-group",
		Concurrency: 2,
		BufferSize:  20,
		RedisClient: client,
	})
	if err != nil {
		t.Fatal("Failed to create consumer:", err)
	}

	// Variable processing time to test timing metrics
	consumer.Register(streamName, func(msg *Message) error {
		if delay, ok := msg.Values["delay"].(string); ok {
			if delay == "slow" {
				time.Sleep(100 * time.Millisecond)
			} else {
				time.Sleep(10 * time.Millisecond)
			}
		}
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		consumer.Run(ctx)
	}()

	time.Sleep(100 * time.Millisecond)

	// Send messages with different processing times
	producer, err := NewProducerOptions(&ProducerOptions{
		RedisClient: client,
	})
	if err != nil {
		t.Fatal("Failed to create producer:", err)
	}

	// Send fast and slow messages
	for i := 0; i < 5; i++ {
		// Fast message
		msg := &Message{
			Stream: streamName,
			Values: map[string]interface{}{
				"data":  fmt.Sprintf("fast-%d", i),
				"delay": "fast",
			},
		}
		producer.Enqueue(ctx, msg)

		// Slow message
		msg = &Message{
			Stream: streamName,
			Values: map[string]interface{}{
				"data":  fmt.Sprintf("slow-%d", i),
				"delay": "slow",
			},
		}
		producer.Enqueue(ctx, msg)
	}

	// Wait for processing
	time.Sleep(3 * time.Second)

	// Validate timing metrics
	metrics := consumer.GetMetrics()

	if metrics.MinProcessingTime <= 0 {
		t.Error("Min processing time should be > 0")
	}

	if metrics.MaxProcessingTime <= metrics.MinProcessingTime {
		t.Error("Max processing time should be > min processing time")
	}

	if metrics.AvgProcessingTime <= 0 {
		t.Error("Average processing time should be > 0")
	}

	if metrics.MessagesPerSecond <= 0 {
		t.Error("Messages per second should be > 0")
	}

	t.Logf("Performance metrics - Min: %dms, Max: %dms, Avg: %.2fms, MPS: %.2f",
		metrics.MinProcessingTime, metrics.MaxProcessingTime, metrics.AvgProcessingTime, metrics.MessagesPerSecond)

	consumer.Shutdown()
}

func testGracefulShutdown(t *testing.T, client *redis.Client) {
	const streamName = "shutdown-test-stream"

	consumer, err := NewConsumerOptions(&ConsumerOptions{
		Name:        "shutdown-consumer",
		GroupName:   "shutdown-group",
		Concurrency: 2,
		BufferSize:  10,
		RedisClient: client,
	})
	if err != nil {
		t.Fatal("Failed to create consumer:", err)
	}

	var processedCount int32
	var shutdownSignalReceived bool

	consumer.Register(streamName, func(msg *Message) error {
		processedCount++
		// Simulate some work
		time.Sleep(50 * time.Millisecond)
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())

	// Run consumer in background
	done := make(chan bool)
	go func() {
		defer func() {
			shutdownSignalReceived = true
			done <- true
		}()
		consumer.Run(ctx)
	}()

	// Give consumer time to start
	time.Sleep(100 * time.Millisecond)

	// Send some messages
	producer, err := NewProducerOptions(&ProducerOptions{
		RedisClient: client,
	})
	if err != nil {
		t.Fatal("Failed to create producer:", err)
	}

	for i := 0; i < 5; i++ {
		msg := &Message{
			Stream: streamName,
			Values: map[string]interface{}{
				"data": fmt.Sprintf("shutdown-test-%d", i),
			},
		}
		producer.Enqueue(ctx, msg)
	}

	// Let some messages process
	time.Sleep(200 * time.Millisecond)

	// Trigger graceful shutdown
	consumer.Shutdown()
	cancel()

	// Wait for shutdown to complete
	select {
	case <-done:
		// Success
	case <-time.After(5 * time.Second):
		t.Fatal("Consumer didn't shut down gracefully within timeout")
	}

	if !shutdownSignalReceived {
		t.Error("Shutdown signal was not properly received")
	}

	if processedCount == 0 {
		t.Error("No messages were processed before shutdown")
	}

	t.Logf("Graceful shutdown completed, processed %d messages", processedCount)
}

// Benchmark test for performance validation
func BenchmarkMessageProcessing(b *testing.B) {
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   15,
	})

	ctx := context.Background()
	if err := client.Ping(ctx).Err(); err != nil {
		b.Skip("Redis not available:", err)
	}

	defer func() {
		client.FlushDB(ctx)
		client.Close()
	}()

	consumer, err := NewConsumerOptions(&ConsumerOptions{
		Name:        "bench-consumer",
		GroupName:   "bench-group",
		Concurrency: 4,
		BufferSize:  100,
		RedisClient: client,
	})
	if err != nil {
		b.Fatal("Failed to create consumer:", err)
	}

	var processed int64

	consumer.Register("bench-stream", func(msg *Message) error {
		processed++
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		consumer.Run(ctx)
	}()

	time.Sleep(100 * time.Millisecond)

	producer, err := NewProducerOptions(&ProducerOptions{
		RedisClient: client,
	})
	if err != nil {
		b.Fatal("Failed to create producer:", err)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			msg := &Message{
				Stream: "bench-stream",
				Values: map[string]interface{}{
					"data": "benchmark-data",
				},
			}
			producer.Enqueue(ctx, msg)
		}
	})

	// Wait for processing to complete
	time.Sleep(time.Second)
	consumer.Shutdown()

	b.Logf("Processed %d messages", processed)
}
