package redis

import (
	"context"
	"errors"
	"time"
	_ "time"

	"github.com/redis/go-redis/v9"
)

type RClient struct {
	client interface{}
}

var ctx = context.Background()

func (rc *RClient) Ping() (interface{}, error) {
	switch c := rc.client.(type) {
	case *redis.Client:
		return c.Ping(ctx).Result()

	case *redis.ClusterClient:
		return c.Ping(ctx).Result()
	default:
		return nil, errors.New("invalid client type")
	}
}

func (rc *RClient) Delete(s string) (int64, error) {

	switch c := rc.client.(type) {
	case *redis.Client:
		return c.Del(ctx, s).Result()
	case *redis.ClusterClient:
		return c.Del(ctx, s).Result()
	default:
		return 0, errors.New("invalid client type")
	}
}

func (rc *RClient) Set(key string, value interface{}, expiration time.Duration) error {
	switch c := rc.client.(type) {
	case *redis.Client:
		return c.Set(ctx, key, value, expiration).Err()
	case *redis.ClusterClient:
		return c.Set(ctx, key, value, expiration).Err()
	default:
		return errors.New("invalid client type")
	}
}

func (rc *RClient) Get(key string) (string, error) {
	switch c := rc.client.(type) {
	case *redis.Client:
		return c.Get(ctx, key).Result()
	case *redis.ClusterClient:
		return c.Get(ctx, key).Result()
	default:
		return "", errors.New("invalid client type")
	}
}

func (rc *RClient) SAdd(key string, value ...string) error {
	switch c := rc.client.(type) {
	case *redis.Client:
		return c.SAdd(ctx, key, value).Err()
	case *redis.ClusterClient:
		return c.SAdd(ctx, key, value).Err()

	default:
		return errors.New("invalid client type")
	}
}

func (rc *RClient) SMembers(key string) ([]string, error) {
	switch c := rc.client.(type) {
	case *redis.Client:
		return c.SMembers(ctx, key).Result()
	case *redis.ClusterClient:
		return c.SMembers(ctx, key).Result()
	default:
		return nil, errors.New("invalid client type")
	}
}

func (rc *RClient) Close() error {
	switch c := rc.client.(type) {
	case *redis.Client:
		return c.Close()
	case *redis.ClusterClient:
		return c.Close()
	default:
		return errors.New("invalid client type")
	}
}

func (rc *RClient) Range(key string, start, end int64) ([]string, error) {
	switch c := rc.client.(type) {
	case *redis.Client:
		return c.ZRange(ctx, key, start, end).Result()
	case *redis.ClusterClient:
		return c.ZRange(ctx, key, start, end).Result()
	default:
		return nil, errors.New("invalid client type")
	}
}

func (rc *RClient) RangeWithScores(key string, start, end int64) ([]redis.Z, error) {
	switch c := rc.client.(type) {
	case *redis.Client:
		return c.ZRangeWithScores(ctx, key, start, end).Result()
	case *redis.ClusterClient:
		return c.ZRangeWithScores(ctx, key, start, end).Result()
	default:
		return nil, errors.New("invalid client type")
	}
}

func (rc *RClient) GetTime() (time.Time, error) {
	switch c := rc.client.(type) {
	case *redis.Client:
		return c.Time(ctx).Result()
	case *redis.ClusterClient:
		return c.Time(ctx).Result()
	default:
		return time.Time{}, errors.New("invalid client type")
	}
}

func (rc *RClient) GetExpire(key string) (time.Duration, error) {

	switch c := rc.client.(type) {
	case *redis.Client:
		return c.TTL(ctx, key).Result()
	case *redis.ClusterClient:
		return c.TTL(ctx, key).Result()
	default:
		return 0, errors.New("invalid client type")
	}
}

func (rc *RClient) Incr(key string) (int64, error) {
	switch c := rc.client.(type) {
	case *redis.Client:
		return c.Incr(ctx, key).Result()
	case *redis.ClusterClient:
		return c.Incr(ctx, key).Result()
	default:
		return 0, errors.New("invalid client type")
	}
}

func (rc *RClient) Decr(key string) (int64, error) {
	switch c := rc.client.(type) {
	case *redis.Client:
		currentVal, err := c.Get(ctx, key).Int64()
		if err != nil && err != redis.Nil {
			return 0, err
		}

		if err == redis.Nil {
			err = c.Set(ctx, key, 0, 0).Err()
			return 0, err
		}

		if currentVal <= 0 {
			err = c.Set(ctx, key, 0, 0).Err()
			return 0, err
		}

		return c.Decr(ctx, key).Result()

	case *redis.ClusterClient:
		currentVal, err := c.Get(ctx, key).Int64()
		if err != nil && err != redis.Nil {
			return 0, err
		}
		if err == redis.Nil {
			err = c.Set(ctx, key, 0, 0).Err()
			return 0, err
		}
		if currentVal <= 0 {
			err = c.Set(ctx, key, 0, 0).Err()
			return 0, err
		}
		return c.Decr(ctx, key).Result()

	default:
		return 0, errors.New("invalid client type")
	}
}

func (rc *RClient) Expire(key string, expiration time.Duration) error {
	switch c := rc.client.(type) {
	case *redis.Client:
		return c.Expire(ctx, key, expiration).Err()
	case *redis.ClusterClient:
		return c.Expire(ctx, key, expiration).Err()
	default:
		return errors.New("invalid client type")
	}
}
