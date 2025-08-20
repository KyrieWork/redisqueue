// Package redisqueue provides a robust, production-ready Redis Streams consumer/producer
// with automatic message reclaiming, error recovery, and comprehensive performance metrics.
//
// This package implements the Redis Streams consumer groups pattern with additional
// reliability features like visibility timeouts, automatic reclaiming of abandoned
// messages, and graceful shutdown handling.
package redisqueue

import (
	"context"
	"os"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// ConsumerFun defines the signature for message processing functions.
// The function receives a Message and should return an error if processing fails.
// Returning an error will cause the message to be marked as failed in metrics.
type ConsumerFun func(msg *Message) error

// ConsumerOptions configures a Consumer instance.
type ConsumerOptions struct {
	// Name identifies this consumer instance. Defaults to hostname if empty.
	Name string

	// GroupName is the Redis consumer group name. Defaults to "redisqueue" if empty.
	GroupName string

	// VisibilityTimeout defines how long a message is invisible after being claimed.
	// Messages exceeding this timeout will be reclaimed. Set to 0 to disable reclaiming.
	VisibilityTimeout time.Duration

	// BlockingTimeout is the maximum time to wait for new messages when polling.
	BlockingTimeout time.Duration

	// ReclaimInterval defines how often to check for messages that need reclaiming.
	ReclaimInterval time.Duration

	// BufferSize is the internal message queue capacity.
	BufferSize int

	// Concurrency controls the number of worker goroutines for message processing.
	Concurrency int

	// RedisClient allows providing a custom Redis client. Takes precedence over RedisOptions.
	RedisClient redis.UniversalClient

	// RedisOptions configures the Redis connection when RedisClient is not provided.
	RedisOptions *RedisOptions
}

// Consumer processes messages from Redis streams using consumer groups with
// automatic error recovery, message reclaiming, and performance monitoring.
//
// The consumer supports multiple streams, concurrent processing, and graceful shutdown.
// It automatically handles Redis connection failures and message reprocessing.
type Consumer struct {
	// Errors is a channel where all processing and system errors are sent.
	// Callers should read from this channel to handle errors appropriately.
	Errors chan error

	options   *ConsumerOptions
	redis     redis.UniversalClient
	consumers map[string]registeredConsumer
	streams   []string
	queue     chan *Message
	wg        *sync.WaitGroup
	ctx       context.Context
	cancel    context.CancelFunc
	metrics   *Metrics
}

// registeredConsumer holds a consumer function and its starting message ID
type registeredConsumer struct {
	fn ConsumerFun
	id string
}

// Default options for new consumers
var defaultConsumerOptions = &ConsumerOptions{
	VisibilityTimeout: 60 * time.Second,
	BlockingTimeout:   5 * time.Second,
	ReclaimInterval:   1 * time.Second,
	BufferSize:        100,
	Concurrency:       10,
}

// NewConsumer creates a new Consumer with production-ready default settings.
//
// Default configuration:
//   - VisibilityTimeout: 60s (messages will be reclaimed after this timeout)
//   - BlockingTimeout: 5s (polling timeout)
//   - ReclaimInterval: 1s (check for stale messages every second)
//   - BufferSize: 100 (internal queue capacity)
//   - Concurrency: 10 (worker goroutines)
//
// Returns an error if Redis connection cannot be established.
func NewConsumer() (*Consumer, error) {
	return NewConsumerOptions(defaultConsumerOptions)
}

// NewConsumerOptions creates a new Consumer with the provided configuration.
//
// The opts parameter is validated and defaults are applied for zero values:
//   - Name defaults to hostname
//   - GroupName defaults to "redisqueue"
//   - BlockingTimeout defaults to 5s
//   - ReclaimInterval defaults to 1s
//
// Redis connectivity is verified during initialization.
// Returns an error if configuration is invalid or Redis is unreachable.
func NewConsumerOptions(opts *ConsumerOptions) (*Consumer, error) {
	hostname, _ := os.Hostname()
	if opts.Name == "" {
		opts.Name = hostname
	}
	if opts.GroupName == "" {
		opts.GroupName = "redisqueue"
	}
	if opts.BlockingTimeout == 0 {
		opts.BlockingTimeout = 5 * time.Second
	}
	if opts.ReclaimInterval == 0 {
		opts.ReclaimInterval = 1 * time.Second
	}

	var r redis.UniversalClient

	if opts.RedisClient != nil {
		r = opts.RedisClient
	} else {
		r = NewRedisClient(opts.RedisOptions)
	}

	if err := redisPreflightChecks(context.Background(), r); err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())

	consumer := &Consumer{
		Errors:    make(chan error),
		options:   opts,
		redis:     r,
		consumers: make(map[string]registeredConsumer),
		streams:   make([]string, 0),
		queue:     make(chan *Message, opts.BufferSize),
		wg:        &sync.WaitGroup{},

		ctx:     ctx,
		cancel:  cancel,
		metrics: NewMetrics(),
	}

	// Set initial Redis connection status
	consumer.metrics.SetRedisConnected(true)

	return consumer, nil
}

// RegisterWithLastID registers a consumer function for the specified stream,
// starting consumption from the given message ID.
//
// Parameters:
//   - stream: Redis stream name to consume from
//   - id: starting message ID (use "0" for beginning, "$" for end, or specific ID)
//   - fn: function to process each message
//
// The id parameter defaults to "0" if empty. Multiple streams can be registered
// with different consumer functions. This method is not thread-safe and should
// be called before Run().
func (c *Consumer) RegisterWithLastID(stream string, id string, fn ConsumerFun) {
	if len(id) == 0 {
		id = "0"
	}
	c.consumers[stream] = registeredConsumer{
		fn: fn,
		id: id,
	}
}

// Register registers a consumer function for the specified stream, starting
// consumption from the beginning of the stream.
//
// This is a convenience method equivalent to RegisterWithLastID(stream, "0", fn).
//
// Parameters:
//   - stream: Redis stream name to consume from
//   - fn: function to process each message
func (c *Consumer) Register(stream string, fn ConsumerFun) {
	c.RegisterWithLastID(stream, "", fn)
}

// Run starts the consumer and blocks until shutdown or context cancellation.
//
// This method:
//  1. Validates that at least one consumer is registered
//  2. Creates Redis consumer groups for all registered streams
//  3. Starts background goroutines for polling and message reclaiming
//  4. Starts configured number of worker goroutines for message processing
//  5. Blocks until Shutdown() is called or the context is cancelled
//
// All errors during processing are sent to the Errors channel and should be
// monitored by the caller. The method returns when all workers have stopped.
//
// Parameters:
//   - ctx: context for cancellation (typically context.Background())
func (c *Consumer) Run(ctx context.Context) {
	if len(c.consumers) == 0 {
		c.reportCriticalError("validate_consumers", ErrNoConsumers)
		return
	}

	for stream, consumer := range c.consumers {
		c.streams = append(c.streams, stream)
		err := c.redis.XGroupCreateMkStream(ctx, stream, c.options.GroupName, consumer.id).Err()
		if err != nil && !IsGroupExistsError(err) {
			c.reportError(ErrorLevelCritical, "create_consumer_group", err, stream, "",
				map[string]interface{}{"group": c.options.GroupName})
			return
		}
	}

	for i := 0; i < len(c.consumers); i++ {
		c.streams = append(c.streams, ">")
	}

	// Start all goroutines with our cancellable context
	go c.reclaim(c.ctx)
	go c.poll(c.ctx)

	// Handle OS signals for graceful shutdown
	stop := newSignalHandle()
	go func() {
		<-stop
		c.Shutdown()
	}()

	c.wg.Add(c.options.Concurrency)

	// Set initial metrics
	c.metrics.SetActiveWorkers(c.options.Concurrency)

	for i := 0; i < c.options.Concurrency; i++ {
		go c.work(c.ctx)
	}

	c.wg.Wait()
}

func (c *Consumer) reportError(level ErrorLevel, op string, err error, stream, messageID string, ctx map[string]interface{}) {
	errorObj := NewErrorWithContext(level, op, err, stream, messageID, ctx)
	c.metrics.IncrementError(op)

	select {
	case c.Errors <- errorObj:
	default:
		// Drop error if channel is full to prevent blocking
	}
}

func (c *Consumer) reportSimpleError(op string, err error) {
	c.reportError(ErrorLevelWarning, op, err, "", "", nil)
}

func (c *Consumer) reportMessageError(op string, err error, stream, messageID string) {
	c.reportError(ErrorLevelWarning, op, err, stream, messageID, nil)
}

func (c *Consumer) reportCriticalError(op string, err error) {
	c.reportError(ErrorLevelCritical, op, err, "", "", nil)
}

// GetMetrics returns a snapshot of current performance metrics.
//
// The returned Metrics struct contains comprehensive statistics including:
//   - Message processing counters (processed, succeeded, failed, reclaimed)
//   - Timing metrics (min, max, average processing time)
//   - Success/error rates and throughput (messages per second)
//   - Queue depth and worker status
//   - Detailed error counts by operation type
//
// This is a read-only snapshot; modifications won't affect the actual metrics.
func (c *Consumer) GetMetrics() *Metrics {
	return c.metrics.GetSnapshot()
}

// GetMetricsJSON returns current performance metrics as a JSON string.
//
// The JSON output includes all metrics from GetMetrics() in a structured format
// suitable for monitoring systems, logging, or API responses.
//
// Returns an error if JSON serialization fails (which should be rare).
func (c *Consumer) GetMetricsJSON() (string, error) {
	return c.metrics.ToJSON()
}

// Shutdown gracefully stops the consumer and all its background goroutines.
//
// This method:
//  1. Cancels the internal context, signaling all goroutines to stop
//  2. Stops message polling and reclaiming
//  3. Allows currently processing messages to complete
//  4. Returns immediately (non-blocking)
//
// After calling Shutdown(), Run() will return once all workers have finished.
// The consumer cannot be restarted after shutdown.
func (c *Consumer) Shutdown() {
	c.cancel()
}
