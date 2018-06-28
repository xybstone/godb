package xdb

import (
	"errors"
	"fmt"
	"time"

	"github.com/garyburd/redigo/redis"
)

var (
	MaxIdle     = 500
	MaxActive   = 4000
	IdleTimeout = 30 * time.Second
	Wait        = true
)

func init() {
	redisConfigs = make(map[string]RedisConfig)
	redisPools = make(map[string]*redis.Pool)
}

//RedisConfig for config
type RedisConfig interface {
	GetHost() string
	GetPort() string
	GetAuth() string
}

var redisConfigs map[string]RedisConfig
var redisPools map[string]*redis.Pool

var errConfig = errors.New("dont have the config")

// AddRedisConfig Add redis
func AddRedisConfig(key string, rc RedisConfig) {
	redisConfigs[key] = rc
}

//GetRedisPool get pool
func GetRedisPool(key string) (*redis.Pool, error) {
	rp, has := redisPools[key]
	if !has || rp == nil {
		if rc, has := redisConfigs[key]; has {
			rs := fmt.Sprintf("%s:%s", rc.GetHost(), rc.GetPort())
			rpd := rc.GetAuth()
			rp, err := newPool(rs, rpd)
			redisPools[key] = rp
			return rp, err
		}
	}
	return rp, nil
}

func newPool(server, pwd string) (*redis.Pool, error) {
	return &redis.Pool{
		MaxIdle:     MaxIdle,
		MaxActive:   MaxActive,
		IdleTimeout: IdleTimeout,
		Wait:        Wait,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", server)
			if err != nil {
				return nil, err
			}
			if pwd != "" {
				if _, err := c.Do("AUTH", pwd); err != nil {
					c.Close()
					return nil, err
				}
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			if time.Since(t) < time.Minute {
				return nil
			}
			_, err := c.Do("PING")
			return err
		},
	}, nil
}
