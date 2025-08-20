package redisqueue

import (
	"context"
	"fmt"
)

// work is the main worker goroutine that processes messages from the queue
func (c *Consumer) work(ctx context.Context) {
	defer c.wg.Done()

	for {
		select {
		case msg := <-c.queue:
			err := c.process(msg)
			if err != nil {
				c.Errors <- NewError("process_message", err)
				continue
			}
			err = c.redis.XAck(ctx, msg.Stream, c.options.GroupName, msg.ID).Err()
			if err != nil {
				c.Errors <- NewError("ack_after_success", err)
				continue
			}
		case <-ctx.Done():
			return
		}
	}
}

// process executes the consumer function for a message with panic recovery
func (c *Consumer) process(msg *Message) (err error) {
	defer func() {
		if r := recover(); r != nil {
			if e, ok := r.(error); ok {
				err = NewError("consumer_func_panic", e)
				return
			}
			err = NewError("consumer_func_panic", fmt.Errorf("panic: %v", r))
		}
	}()
	err = c.consumers[msg.Stream].fn(msg)
	return
}
