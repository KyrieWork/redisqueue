# RedisQueue

A production-ready Redis Streams consumer/producer with automatic error recovery, message reclaiming, and comprehensive performance monitoring.

## Features

- **Bulletproof Error Handling**: 3-level error classification with rich context
- **Auto Message Recovery**: Automatic reclaiming of abandoned messages  
- **Performance Monitoring**: 13 comprehensive metrics with JSON export
- **Graceful Shutdown**: Context-driven cancellation across all goroutines
- **Zero Configuration**: Sensible defaults that just work

## Installation

```bash
go get github.com/KyrieWork/redisqueue
```

## Quick Start

### Producer

```go
package main

import (
    "context"
    "github.com/KyrieWork/redisqueue"
)

func main() {
    producer, err := redisqueue.NewProducer()
    if err != nil {
        panic(err)
    }
    
    msg := &redisqueue.Message{
        Stream: "events",
        Values: map[string]interface{}{
            "type": "user_signup",
            "user_id": "12345",
        },
    }
    
    err = producer.Enqueue(context.Background(), msg)
    if err != nil {
        panic(err)
    }
}
```

### Consumer

```go
package main

import (
    "context"
    "log"
    "github.com/KyrieWork/redisqueue"
)

func main() {
    consumer, err := redisqueue.NewConsumer()
    if err != nil {
        panic(err)
    }
    
    // Register message processor
    consumer.Register("events", func(msg *redisqueue.Message) error {
        log.Printf("Processing: %+v", msg.Values)
        // Your business logic here
        return nil
    })
    
    // Monitor errors
    go func() {
        for err := range consumer.Errors {
            log.Printf("Error: %v", err)
        }
    }()
    
    // Start processing (blocks until shutdown)
    consumer.Run(context.Background())
}
```

### Performance Monitoring

```go
// Get real-time metrics
metrics := consumer.GetMetrics()
log.Printf("Processed: %d, Success Rate: %.2f%%", 
    metrics.MessagesProcessed, metrics.SuccessRate)

// Export as JSON for monitoring systems
json, _ := consumer.GetMetricsJSON()
log.Println(json)
```

## Configuration

```go
consumer, err := redisqueue.NewConsumerOptions(&redisqueue.ConsumerOptions{
    Name:              "my-service",
    GroupName:         "my-group", 
    VisibilityTimeout: 60 * time.Second,  // Message claim timeout
    Concurrency:       20,                // Worker goroutines
    BufferSize:        1000,              // Internal queue size
})
```

## Error Handling

The library provides intelligent error classification:

- **Critical**: System-level errors requiring immediate attention
- **Warning**: Recoverable errors that are logged but don't stop processing  
- **Info**: Statistical information for monitoring

All errors include rich context: operation, stream, message ID, and debugging data.

## Requirements

- Redis 5.0+ (Redis Streams support)
- Go 1.21+

## Production Notes

This library was built for production workloads. It handles:

- Redis connection failures with automatic retry
- Message processing failures with proper error tracking
- Memory management with configurable buffer sizes
- Graceful shutdown with in-flight message completion
- Comprehensive metrics for monitoring and alerting

## License

MIT License. Use it, abuse it, just don't blame us when you realize how much better your infrastructure could be.
