package delayed

import (
	"os"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/gomodule/redigo/redis"
	"github.com/keakon/golog"
)

type redisArgs struct {
	Address string
	Cmd     string
	Args    []interface{}
}

func panicFunc(a interface{}) {
	panic(a)
}

func redisCall(arg *redisArgs) {
	conn, err := redis.Dial("tcp", arg.Address)
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	conn.Do(arg.Cmd, arg.Args...)
}

func redisCall2(arg *redisArgs) {
	redisCall(arg)
}

func TestWorkerRegisterHandlers(t *testing.T) {
	w := NewWorker("test", "localhost:6379", "", 2)
	w.RegisterHandlers(f1, f2, f3)
	if len(w.handlers) != 3 {
		t.FailNow()
	}
	w.RegisterHandlers(f4)
	if len(w.handlers) != 4 {
		t.FailNow()
	}
}

func TestWorkerRun(t *testing.T) {
	initLogger(golog.DebugLevel)

	w := NewWorker("test", "localhost:6379", "", 2)
	w.RegisterHandlers(panicFunc, redisCall)

	q := NewQueue("1", "test", "localhost:6379", "", 2)
	conn := q.redis.Get()
	defer conn.Close()
	defer q.Clear()

	key := "test" + w.id
	defer conn.Do("DEL", key)
	task := NewTaskOfFunc(0, panicFunc, 1)
	q.Enqueue(task)
	task = NewTaskOfFunc(0, redisCall2, redisArgs{Address: "localhost:6379", Cmd: "RPUSH", Args: []interface{}{key, 2}})
	q.Enqueue(task)
	task = NewTaskOfFunc(0, redisCall, redisArgs{Address: "localhost:6379", Cmd: "RPUSH", Args: []interface{}{key, 1}})
	q.Enqueue(task)

	count, err := q.Len()
	if err != nil {
		t.Fatal(err)
	}
	if count != 3 {
		t.FailNow()
	}

	failed := atomic.Bool{}

	go func() {
		defer w.Stop()
		reply, err := redis.Values(conn.Do("BLPOP", key, 0))
		if err != nil {
			failed.Store(true)
			return
		}

		if len(reply) != 2 {
			failed.Store(true)
			return
		}

		popped, ok := reply[1].([]uint8)
		if !ok || len(popped) != 1 {
			failed.Store(true)
			return
		}

		if popped[0] != '1' {
			failed.Store(true)
		}
	}()

	w.Run()

	if failed.Load() {
		t.FailNow()
	}

	count, err = q.Len()
	if err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.FailNow()
	}
}

func TestWorkerSignal(t *testing.T) {
	w := NewWorker("test", "localhost:6379", "", 2)
	w.RegisterHandlers(redisCall)

	q := NewQueue("1", "test", "localhost:6379", "", 2)
	conn := q.redis.Get()
	defer conn.Close()
	defer q.Clear()

	key := "test" + w.id
	defer conn.Do("DEL", key)
	task := NewTaskOfFunc(0, redisCall, redisArgs{Address: "localhost:6379", Cmd: "RPUSH", Args: []interface{}{key, 1}})
	q.Enqueue(task)

	pid := os.Getpid()

	go func() {
		conn.Do("BLPOP", key, 0)
		syscall.Kill(pid, syscall.SIGHUP)
	}()

	w.Run()
}
