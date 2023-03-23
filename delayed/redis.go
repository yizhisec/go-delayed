package delayed

import (
	"time"

	"github.com/gomodule/redigo/redis"
)

// NewRedisPool creates a new redis pool.
func NewRedisPool(address string, options ...redis.DialOption) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     1,
		MaxActive:   10,
		IdleTimeout: 60 * time.Second,
		Dial: func() (c redis.Conn, err error) {
			return redis.Dial(
				"tcp",
				address,
				options...,
			)
		},
	}
}
