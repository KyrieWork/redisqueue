package redisqueue

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
)

type ConsumerFun func(msg *Message) error

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

type Consumer struct {
	Errors    chan error
	options   *ConsumerOptions
	redis     redis.UniversalClient
	consumers map[string]registeredConsumer
	streams   []string
	queue     chan *Message
	wg        *sync.WaitGroup

	stopReclaim chan struct{}
	stopPoll    chan struct{}
	stopWorkers chan struct{}
}

func NewConsumer() (*Consumer, error) {
	return NewConsumerOptions(defaultConsumerOptions)
}

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

	return &Consumer{
		Errors:    make(chan error),
		options:   opts,
		redis:     r,
		consumers: make(map[string]registeredConsumer),
		streams:   make([]string, 0),
		queue:     make(chan *Message, opts.BufferSize),
		wg:        &sync.WaitGroup{},

		stopReclaim: make(chan struct{}, 1),
		stopPoll:    make(chan struct{}, 1),
		stopWorkers: make(chan struct{}, opts.Concurrency),
	}, nil
}

func (c *Consumer) RegisterWithLastID(stream string, id string, fn ConsumerFun) {
	if len(id) == 0 {
		id = "0"
	}
	c.consumers[stream] = registeredConsumer{
		fn: fn,
		id: id,
	}
}

func (c *Consumer) Register(stream string, fn ConsumerFun) {
	c.RegisterWithLastID(stream, "", fn)
}

func (c *Consumer) Run(ctx context.Context) {
	if len(c.consumers) == 0 {
		c.Errors <- errors.New("at least one consumer function needs to be registered")
		return
	}

	for stream, consumer := range c.consumers {
		c.streams = append(c.streams, stream)
		err := c.redis.XGroupCreateMkStream(ctx, stream, c.options.GroupName, consumer.id).Err()
		if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
			c.Errors <- errors.New(err.Error() + "\nerror creating consumer group")
			return
		}
	}

	for i := 0; i < len(c.consumers); i++ {
		c.streams = append(c.streams, ">")
	}

	go c.reclaim(ctx)
	go c.poll(ctx)

	stop := newSignalHandle()
	go func() {
		<-stop
		c.Shutdown()
	}()

	c.wg.Add(c.options.Concurrency)

	for i := 0; i < c.options.Concurrency; i++ {
		go c.work(ctx)
	}

	c.wg.Wait()
}

func (c *Consumer) Shutdown() {
	c.stopReclaim <- struct{}{}
	if c.options.VisibilityTimeout == 0 {
		c.stopPoll <- struct{}{}
	}
}

// ====================================================
// Internal Functions
// ====================================================

type registeredConsumer struct {
	fn ConsumerFun
	id string
}

var defaultConsumerOptions = &ConsumerOptions{
	VisibilityTimeout: 60 * time.Second,
	BlockingTimeout:   5 * time.Second,
	ReclaimInterval:   1 * time.Second,
	BufferSize:        100,
	Concurrency:       10,
}

func (c *Consumer) reclaim(ctx context.Context) {
	if c.options.VisibilityTimeout == 0 {
		return
	}

	ticker := time.NewTicker(c.options.ReclaimInterval)
	for {
		select {
		case <-c.stopReclaim:
			c.stopPoll <- struct{}{}
			return
		case <-ticker.C:
			for stream := range c.consumers {
				start := "-"
				end := "+"

				for {
					res, err := c.redis.XPendingExt(ctx, &redis.XPendingExtArgs{
						Stream: stream,
						Group:  c.options.GroupName,
						Start:  start,
						End:    end,
						Count:  int64(c.options.BufferSize - len(c.queue))},
					).Result()
					if err != nil && !errors.Is(err, redis.Nil) {
						c.Errors <- errors.New(err.Error() + "\nerror listing pending messages")
						break
					}
					if len(res) == 0 {
						break
					}

					msgs := make([]string, 0)
					for _, r := range res {
						if r.Idle >= c.options.VisibilityTimeout {
							claimres, err := c.redis.XClaim(ctx, &redis.XClaimArgs{
								Stream:   stream,
								Group:    c.options.GroupName,
								Consumer: c.options.Name,
								MinIdle:  c.options.VisibilityTimeout,
								Messages: []string{r.ID}},
							).Result()
							if err != nil && !errors.Is(err, redis.Nil) {
								c.Errors <- errors.New(err.Error() + "\n" + fmt.Sprintf("error claiming %d message(s)", len(msgs)))
							}

							if errors.Is(err, redis.Nil) {
								err = c.redis.XAck(ctx, stream, c.options.GroupName, r.ID).Err()
								if err != nil {
									c.Errors <- errors.New(err.Error() + "\n" + fmt.Sprintf("error acknowledging after failed claim for %q stream and %q message", stream, r.ID))
								}
							}
							c.enqueue(ctx, stream, claimres)
						}
					}

					newID, err := incrementMessageID(res[len(res)-1].ID)
					if err != nil {
						c.Errors <- err
						break
					}

					start = newID
				}
			}
		}
	}
}

func (c *Consumer) poll(ctx context.Context) {
	for {
		select {
		case <-c.stopPoll:
			for i := 0; i < c.options.Concurrency; i++ {
				c.stopWorkers <- struct{}{}
			}
			return
		default:
			res, err := c.redis.XReadGroup(ctx, &redis.XReadGroupArgs{
				Group:    c.options.GroupName,
				Consumer: c.options.Name,
				Streams:  c.streams,
				Count:    int64(c.options.BufferSize - len(c.queue)),
				Block:    c.options.BlockingTimeout,
			}).Result()
			if err != nil {
				if err, ok := err.(net.Error); ok && err.Timeout() {
					continue
				}
				if errors.Is(err, redis.Nil) {
					continue
				}
				c.Errors <- errors.New(err.Error() + "\nerror reading redis stream")
				continue
			}

			for _, r := range res {
				c.enqueue(ctx, r.Stream, r.Messages)
			}
		}
	}
}

func (c *Consumer) enqueue(ctx context.Context, stream string, msgs []redis.XMessage) {
	for _, m := range msgs {
		msg := &Message{
			ID:     m.ID,
			Stream: stream,
			Values: m.Values,
		}
		c.queue <- msg
	}
}

func (c *Consumer) work(ctx context.Context) {
	defer c.wg.Done()

	for {
		select {
		case msg := <-c.queue:
			err := c.process(msg)
			if err != nil {
				c.Errors <- errors.New(err.Error() + "\n" + fmt.Sprintf("error calling ConsumerFunc for %q stream and %q message", msg.Stream, msg.ID))
				continue
			}
			err = c.redis.XAck(ctx, msg.Stream, c.options.GroupName, msg.ID).Err()
			if err != nil {
				c.Errors <- errors.New(err.Error() + "\n" + fmt.Sprintf("error acknowledging after success for %q stream and %q message", msg.Stream, msg.ID))
				continue
			}
		case <-c.stopWorkers:
			return
		}
	}
}

func (c *Consumer) process(msg *Message) (err error) {
	defer func() {
		if r := recover(); r != nil {
			if e, ok := r.(error); ok {
				c.Errors <- errors.New(e.Error() + "\nConsumerFunc panic")
				return
			}
			err = fmt.Errorf("ConsumerFunc panic: %v", r)
		}
	}()
	err = c.consumers[msg.Stream].fn(msg)
	return
}

func newSignalHandle() <-chan struct{} {
	stop := make(chan struct{})
	c := make(chan os.Signal, 2)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-c
		close(stop)
		<-c
		os.Exit(1)
	}()
	return stop
}
