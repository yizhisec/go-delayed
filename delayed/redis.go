package delayed

import (
	"time"

	"github.com/gomodule/redigo/redis"
)

func NewRedisPool(address, password string) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     1,
		MaxActive:   10,
		IdleTimeout: 60 * time.Second,
		Dial: func() (c redis.Conn, err error) {
			if password == "" {
				return redis.Dial(
					"tcp",
					address,
				)
			}
			return redis.Dial(
				"tcp",
				address,
				redis.DialPassword(password),
			)
		},
	}
}
