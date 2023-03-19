package delayed

import "testing"

const redisAddr = ":6379"

func TestNewRedisPool(t *testing.T) {
	pool := NewRedisPool(redisAddr)
	conn := pool.Get()
	defer conn.Close()
	_, err := conn.Do("PING")
	if err != nil {
		t.Error(err)
	}
}
