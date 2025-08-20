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
			// Start timing message processing
			endTiming := c.metrics.RecordProcessingStart()
			c.metrics.IncrementProcessed()

			err := c.process(msg)
			if err != nil {
				c.metrics.IncrementFailed()
				c.reportMessageError("process_message", err, msg.Stream, msg.ID)
				endTiming() // Still record the timing even for failed messages
				continue
			}

			err = c.redis.XAck(ctx, msg.Stream, c.options.GroupName, msg.ID).Err()
			if err != nil {
				c.metrics.IncrementFailed()
				c.reportMessageError("ack_after_success", err, msg.Stream, msg.ID)
				endTiming()
				continue
			}

			c.metrics.IncrementSucceeded()
			endTiming()
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
				err = NewErrorWithContext(ErrorLevelCritical, "consumer_func_panic", e, msg.Stream, msg.ID,
					map[string]interface{}{"panic_value": r})
				return
			}
			err = NewErrorWithContext(ErrorLevelCritical, "consumer_func_panic", fmt.Errorf("panic: %v", r),
				msg.Stream, msg.ID, map[string]interface{}{"panic_value": r})
		}
	}()
	err = c.consumers[msg.Stream].fn(msg)
	return
}
