package redisqueue

import (
	"context"
	"os"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// ConsumerFun is the function signature for message processing
type ConsumerFun func(msg *Message) error

// ConsumerOptions contains all configuration options for a Consumer
type ConsumerOptions struct {
	Name              string
	GroupName         string
	VisibilityTimeout time.Duration
	BlockingTimeout   time.Duration
	ReclaimInterval   time.Duration
	BufferSize        int
	Concurrency       int
	RedisClient       redis.UniversalClient
	RedisOptions      *RedisOptions
}

// Consumer processes messages from Redis streams using consumer groups
type Consumer struct {
	Errors    chan error
	options   *ConsumerOptions
	redis     redis.UniversalClient
	consumers map[string]registeredConsumer
	streams   []string
	queue     chan *Message
	wg        *sync.WaitGroup

	ctx    context.Context
	cancel context.CancelFunc

	// Performance metrics
	metrics *Metrics
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

// NewConsumer creates a new Consumer with default options
func NewConsumer() (*Consumer, error) {
	return NewConsumerOptions(defaultConsumerOptions)
}

// NewConsumerOptions creates a new Consumer with custom options
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

// RegisterWithLastID registers a consumer function for a stream starting from a specific message ID
func (c *Consumer) RegisterWithLastID(stream string, id string, fn ConsumerFun) {
	if len(id) == 0 {
		id = "0"
	}
	c.consumers[stream] = registeredConsumer{
		fn: fn,
		id: id,
	}
}

// Register registers a consumer function for a stream starting from the beginning
func (c *Consumer) Register(stream string, fn ConsumerFun) {
	c.RegisterWithLastID(stream, "", fn)
}

// Run starts the consumer and blocks until shutdown
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

// Unified error handling methods
func (c *Consumer) reportError(level ErrorLevel, op string, err error, stream, messageID string, ctx map[string]interface{}) {
	errorObj := NewErrorWithContext(level, op, err, stream, messageID, ctx)

	// Update metrics
	c.metrics.IncrementError(op)

	// For critical errors, we might want to trigger shutdown
	// For now, just send to error channel
	select {
	case c.Errors <- errorObj:
	default:
		// Error channel is full - this is bad!
		// But don't block, just drop the error
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

// GetMetrics returns a snapshot of current performance metrics
func (c *Consumer) GetMetrics() *Metrics {
	return c.metrics.GetSnapshot()
}

// GetMetricsJSON returns metrics as JSON string
func (c *Consumer) GetMetricsJSON() (string, error) {
	return c.metrics.ToJSON()
}

// Shutdown gracefully stops the consumer
func (c *Consumer) Shutdown() {
	c.cancel()
}
