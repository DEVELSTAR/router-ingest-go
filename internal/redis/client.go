package redis

import (
	"context"

	"github.com/redis/go-redis/v9"
)

var ctx = context.Background()

type Client struct {
	Rdb *redis.Client
}

func New(addr string) *Client {
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	return &Client{Rdb: rdb}
}

func (c *Client) GetMetadata(deviceID string) (map[string]string, error) {
	return c.Rdb.HGetAll(ctx, "device:"+deviceID).Result()
}
