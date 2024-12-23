package redis

import (
	"context"
	"strings"
	"sync"

	"github.com/qz-io/tcode-modules/pkg/common"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
)

var client *RClient
var initOnce sync.Once

func SetRedisLogger(l *zerolog.Logger) {
	common.Logger = l

}

func newRedisClient(address, user, password string) (*RClient, error) {
	addresses := strings.Split(address, ",")

	if len(addresses) == 1 && !strings.Contains(address, ",") {
		client := redis.NewClient(&redis.Options{
			Addr:     addresses[0],
			Username: user,
			Password: password,
		})
		rs, err0 := client.ClusterSlots(context.Background()).Result()
		if err0 == nil {
			if rs != nil && len(rs) > 1 {
				if common.Logger != nil {
					common.Logger.Error().Msgf("Server is in cluster mode but configured as standalone mode. Please modify the configuration. If there is only one IP address, add a comma at the end to set it to cluster mode")
				}

				for _, r := range rs {
					if common.Logger != nil {
						common.Logger.Error().Msgf("Slot start:%d,end:%d node:%v", r.Start, r.End, r.Nodes)
					}

				}
				panic("Please check the redis configuration and set it to cluster mode again")
			}
		} else {
			// common.Logger.Err(err0).Msgf("")
		}
		nodes, err0 := client.ClusterNodes(context.Background()).Result()
		if err0 == nil {
			common.Logger.Info().Msgf("standalone mode,ClusterNodes:\n%s", nodes)
		}

		_, err := client.Ping(context.Background()).Result()
		if err != nil {
			return nil, err
		}

		return &RClient{client: client}, nil
	} else {
		addr2 := make([]string, 0)
		for _, addr := range addresses {
			if addr != "" {
				addr2 = append(addr2, addr)
			}
		}

		client := redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:    addr2,
			Username: user,
			Password: password,
		})
		nodes, err0 := client.ClusterNodes(context.Background()).Result()
		if err0 == nil {
			if common.Logger != nil {
				common.Logger.Info().Msgf("cluster mode,ClusterNodes:\n%s", nodes)
			}

		}
		_, err := client.Ping(context.Background()).Result()
		if err != nil {
			return nil, err
		}

		return &RClient{client: client}, nil
	}
}

func InitClient(server, user, password string) {
	initOnce.Do(func() {
		if common.Logger != nil {
			common.Logger.Debug().Msgf("redis init")
		}
		var err error
		client, err = newRedisClient(server, user, password)
		if err != nil {
			if common.Logger != nil {
				common.Logger.Err(err).Msg("redis init error")
				if strings.Contains(err.Error(), "ERR This instance has cluster support disabled") {
					common.Logger.Error().Msg("Redis does not support cluster mode, but configuration is set to cluster mode. Please check configuration redisServer=" + server)
					panic("Redis does not support cluster mode")
				}

			}
			panic(err)
		}
		// ping test
		v, err := client.Ping()
		if err != nil {
			if common.Logger != nil {
				common.Logger.Err(err).Msg("redis ping error")
			}
			panic(err)
		} else {
			if common.Logger != nil {
				common.Logger.Debug().Msgf("redis ping success. %v", v)
			}
		}
		if common.Logger != nil {
			common.Logger.Debug().Msgf("redis init success.")
		}

	})
}

func GetClient() *RClient {
	return client
}

func SAdd(key string, value string) error {
	if key == "" || value == "" {
		return nil
	}

	tempValue := strings.ReplaceAll(value, "index", "")
	tempValue = strings.ReplaceAll(tempValue, "audio", "")
	tempValue = strings.ReplaceAll(tempValue, ".ts", "")
	tempValue = strings.ReplaceAll(tempValue, "subtitle", "")
	tempValue = strings.ReplaceAll(tempValue, ".vtt", "")
	return client.SAdd(key, tempValue)
}
