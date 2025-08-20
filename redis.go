package redisqueue

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/redis/go-redis/v9"
)

type RedisOptions = redis.Options

func NewRedisClient(opts *RedisOptions) *redis.Client {
	if opts == nil {
		opts = &RedisOptions{}
	}
	return redis.NewClient(opts)
}

// ====================================================
// Internal Functions
// ====================================================

var redisVersionRE = regexp.MustCompile(`redis_version:(.+)`)

func redisPreflightChecks(ctx context.Context, client redis.UniversalClient) error {
	info, err := client.Info(ctx, "server").Result()
	if err != nil {
		return err
	}
	matches := redisVersionRE.FindAllStringSubmatch(info, -1)
	if len(matches) < 1 {
		return fmt.Errorf("redis version not found in %q", info)
	}
	version := strings.TrimSpace(matches[0][1])
	parts := strings.Split(version, ".")
	major, err := strconv.Atoi(parts[0])
	if err != nil {
		return err
	}
	if major < 5 {
		return fmt.Errorf("redis version %s is too old", version)
	}
	return nil
}

func incrementMessageID(id string) (string, error) {
	parts := strings.Split(id, "-")
	index := parts[1]
	parsed, err := strconv.ParseInt(index, 10, 64)
	if err != nil {
		return "", errors.New(err.Error() + "\n" + fmt.Sprintf("error parsing message ID %q", id))
	}
	return fmt.Sprintf("%s-%d", parts[0], parsed+1), nil
}
