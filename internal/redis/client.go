// router-ingest-go/internal/redis/client.go

package redis

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

type Client struct {
	Rdb *redis.Client
}

func New(addr string) *Client {
	rdb := redis.NewClient(&redis.Options{
		Addr:        addr,
		DialTimeout: 5 * time.Second,
		ReadTimeout: 3 * time.Second,
	})
	return &Client{Rdb: rdb}
}

func (c *Client) GetMetadata(ctx context.Context, deviceID string) (map[string]string, error) {
	if deviceID == "" {
		return map[string]string{}, nil
	}

	// enforce timeout
	ctxTimeout, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()

	res, err := c.Rdb.HGetAll(ctxTimeout, "device:"+deviceID).Result()
	if err != nil {
		// return empty map so ingestion doesn’t block
		return map[string]string{}, err
	}
	return res, nil
}