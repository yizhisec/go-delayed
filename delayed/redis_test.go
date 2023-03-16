package delayed

import (
	"os"
	"sync"
)

var (
	once      sync.Once
	redisAddr string
)

func getRedisAddress() string {
	once.Do(func() {
		host := os.Getenv("REDIS_HOST")
		port := os.Getenv("REDIS_PORT")
		if port == "" {
			port = "6379"
		}
		redisAddr = host + ":" + port
	})
	return redisAddr
}
