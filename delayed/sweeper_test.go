package delayed

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/keakon/golog"
)

func TestSweeperRun(t *testing.T) {
	initLogger(golog.DebugLevel)

	q := NewQueue("test", NewRedisPool(redisAddr))
	defer q.Clear()

	task := NewGoTask("test")
	q.Enqueue(task)
	q.Dequeue()

	count, err := q.Len()
	if err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.FailNow()
	}

	var failed uint32
	sweeper := NewSweeper(q)
	sweeper.SetInterval(time.Millisecond)
	go func() {
		for {
			time.Sleep(time.Millisecond)
			count, err := q.Len()
			if err != nil {
				atomic.StoreUint32(&failed, 1)
				return
			} else if count == 1 {
				sweeper.Stop()
				return
			}
		}
	}()
	sweeper.Run()

	if atomic.LoadUint32(&failed) == 1 {
		t.FailNow()
	}
}
