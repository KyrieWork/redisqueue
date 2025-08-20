package redisqueue

import (
	"context"
	"errors"
	"net"

	"github.com/redis/go-redis/v9"
)

// poll continuously reads new messages from Redis streams
func (c *Consumer) poll(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
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
				// This is a serious error - Redis connection issues
				c.reportError(ErrorLevelCritical, "read_stream", err, "", "",
					map[string]interface{}{
						"streams":     c.streams,
						"group":       c.options.GroupName,
						"consumer":    c.options.Name,
						"buffer_size": c.options.BufferSize - len(c.queue),
					})
				continue
			}

			for _, r := range res {
				c.enqueue(ctx, r.Stream, r.Messages)
			}
		}
	}
}

// enqueue adds messages to the internal processing queue
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
