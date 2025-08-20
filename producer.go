package redisqueue

import (
	"context"

	"github.com/redis/go-redis/v9"
)

type ProducerOptions struct {
	StreamMaxLength      int64
	ApproximateMaxLength bool
	RedisClient          redis.UniversalClient
	RedisOptions         *RedisOptions
}

type Producer struct {
	options *ProducerOptions
	redis   redis.UniversalClient
}

func NewProducer() (*Producer, error) {
	return NewProducerOptions(defaultProducerOptions)
}

func NewProducerOptions(opts *ProducerOptions) (*Producer, error) {
	var r redis.UniversalClient

	if opts.RedisClient != nil {
		r = opts.RedisClient
	} else {
		r = NewRedisClient(opts.RedisOptions)
	}

	if err := redisPreflightChecks(context.Background(), r); err != nil {
		return nil, err
	}

	return &Producer{
		options: opts,
		redis:   r,
	}, nil
}

func (p *Producer) Enqueue(ctx context.Context, msg *Message) error {
	id, err := p.redis.XAdd(ctx, &redis.XAddArgs{
		ID:     msg.ID,
		Stream: msg.Stream,
		Values: msg.Values,
		MaxLen: p.options.StreamMaxLength,
		Approx: p.options.ApproximateMaxLength,
	}).Result()
	if err != nil {
		return err
	}
	msg.ID = id
	return nil
}

// ====================================================
// Internal Functions
// ====================================================

var defaultProducerOptions = &ProducerOptions{
	StreamMaxLength:      1000,
	ApproximateMaxLength: true,
}
