package redisqueue

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

// reclaim handles message reclaiming for visibility timeout
// It periodically checks for pending messages that have exceeded the visibility timeout
// and reclaims them for reprocessing
func (c *Consumer) reclaim(ctx context.Context) {
	if c.options.VisibilityTimeout == 0 {
		return
	}

	ticker := time.NewTicker(c.options.ReclaimInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
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
						c.reportError(ErrorLevelWarning, "list_pending_messages", err, stream, "",
							map[string]interface{}{
								"group": c.options.GroupName,
								"start": start,
								"end":   end,
							})
						break
					}
					if len(res) == 0 {
						break
					}

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
								c.reportMessageError("claim_messages", err, stream, r.ID)
							}

							if errors.Is(err, redis.Nil) {
								err = c.redis.XAck(ctx, stream, c.options.GroupName, r.ID).Err()
								if err != nil {
									c.reportMessageError("ack_after_failed_claim", err, stream, r.ID)
								}
							}
							c.enqueue(ctx, stream, claimres)
						}
					}

					newID, err := incrementMessageID(res[len(res)-1].ID)
					if err != nil {
						c.reportError(ErrorLevelWarning, "increment_message_id", err, stream, res[len(res)-1].ID, nil)
						break
					}

					start = newID
				}
			}
		}
	}
}
