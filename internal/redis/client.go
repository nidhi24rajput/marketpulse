// Package redis wraps go-redis with domain-aware helpers.
package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/nidhi24rajput/marketpulse/internal/config"
)

// Client wraps a Redis client with convenience methods used across services.
type Client struct {
	rdb *redis.Client
	ttl time.Duration
}

// New creates and pings a Redis connection.
func New(cfg config.RedisConfig) (*Client, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:         cfg.Addr,
		Password:     cfg.Password,
		DB:           cfg.DB,
		PoolSize:     20,
		MinIdleConns: 5,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis: ping failed: %w", err)
	}

	return &Client{rdb: rdb, ttl: cfg.TTL}, nil
}

// Get retrieves a cached value and unmarshals it into dest.
// Returns (false, nil) on cache miss.
func (c *Client) Get(ctx context.Context, key string, dest any) (bool, error) {
	val, err := c.rdb.Get(ctx, key).Bytes()
	if err == redis.Nil {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("redis: get %s: %w", key, err)
	}
	if err := json.Unmarshal(val, dest); err != nil {
		return false, fmt.Errorf("redis: unmarshal %s: %w", key, err)
	}
	return true, nil
}

// Set marshals v and stores it with the default TTL.
func (c *Client) Set(ctx context.Context, key string, v any) error {
	return c.SetWithTTL(ctx, key, v, c.ttl)
}

// SetWithTTL marshals v and stores it with an explicit TTL.
func (c *Client) SetWithTTL(ctx context.Context, key string, v any, ttl time.Duration) error {
	payload, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("redis: marshal %s: %w", key, err)
	}
	return c.rdb.Set(ctx, key, payload, ttl).Err()
}

// Del removes one or more keys.
func (c *Client) Del(ctx context.Context, keys ...string) error {
	return c.rdb.Del(ctx, keys...).Err()
}

// IncrWithExpiry atomically increments a counter and resets the TTL.
func (c *Client) IncrWithExpiry(ctx context.Context, key string, ttl time.Duration) (int64, error) {
	pipe := c.rdb.Pipeline()
	incr := pipe.Incr(ctx, key)
	pipe.Expire(ctx, key, ttl)
	if _, err := pipe.Exec(ctx); err != nil {
		return 0, err
	}
	return incr.Val(), nil
}

// AddToSetWithExpiry adds a member to a set and refreshes TTL.
func (c *Client) AddToSetWithExpiry(ctx context.Context, key, member string, ttl time.Duration) error {
	pipe := c.rdb.Pipeline()
	pipe.SAdd(ctx, key, member)
	pipe.Expire(ctx, key, ttl)
	_, err := pipe.Exec(ctx)
	return err
}

// SCard returns the cardinality of a set (used for unique sessions count).
func (c *Client) SCard(ctx context.Context, key string) (int64, error) {
	return c.rdb.SCard(ctx, key).Result()
}

// Pipeline returns a new Redis pipeline for batch operations.
func (c *Client) Pipeline(ctx context.Context) redis.Pipeliner {
	return c.rdb.Pipeline()
}

// Close shuts down the Redis connection pool.
func (c *Client) Close() error {
	return c.rdb.Close()
}
