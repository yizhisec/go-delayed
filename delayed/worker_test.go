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

func panicFunc(e interface{}) {
	panic(e)
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
	w := NewWorker("test", NewQueue("test", NewRedisPool(redisAddr)))
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

	w := NewWorker("test", NewQueue("test", NewRedisPool(redisAddr), DequeueTimeout(2)))
	w.RegisterHandlers(panicFunc, redisCall)

	q := NewQueue("test", NewRedisPool(redisAddr))
	conn := q.redis.Get()
	defer conn.Close()
	defer q.Clear()

	key := "test" + w.id
	defer conn.Do("DEL", key)
	task := NewGoTaskOfFunc(0, panicFunc, "test")
	q.Enqueue(task)
	task = NewGoTaskOfFunc(0, redisCall2, redisArgs{Address: redisAddr, Cmd: "RPUSH", Args: []interface{}{key, 2}})
	q.Enqueue(task)
	task = NewGoTaskOfFunc(0, redisCall, redisArgs{Address: redisAddr, Cmd: "RPUSH", Args: []interface{}{key, 1}})
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
	w := NewWorker("test", NewQueue("test", NewRedisPool(redisAddr), DequeueTimeout(2)))
	w.RegisterHandlers(syscall.Kill)

	q := NewQueue("test", NewRedisPool(redisAddr))
	task := NewGoTaskOfFunc(0, syscall.Kill, []interface{}{os.Getpid(), syscall.SIGHUP})
	q.Enqueue(task)

	w.Run()
}

func noArgFunc()                   {}
func intFunc(int)                  {}
func intPFunc(*int)                {}
func int2Func(int, int)            {}
func structFunc(testArg)           {}
func structPFunc(*testArg)         {}
func struct2Func(testArg, testArg) {}

func BenchmarkWorkerExecute(b *testing.B) {
	w := Worker{handlers: map[string]*Handler{}}

	tests := []struct {
		name string
		fn   interface{}
		arg  interface{}
	}{
		{
			name: "no arg",
			fn:   noArgFunc,
			arg:  nil,
		},
		{
			name: "int arg",
			fn:   intFunc,
			arg:  1,
		},
		{
			name: "*int arg",
			fn:   intPFunc,
			arg:  new(int),
		},
		{
			name: "int 2 args",
			fn:   int2Func,
			arg:  []int{1, 2},
		},
		{
			name: "struct arg",
			fn:   structFunc,
			arg:  testArg{},
		},
		{
			name: "*struct arg",
			fn:   structPFunc,
			arg:  &testArg{},
		},
		{
			name: "struct 2 args",
			fn:   struct2Func,
			arg:  []testArg{{}, {}},
		},
	}

	for _, tt := range tests {
		w.RegisterHandlers(tt.fn)
	}

	for _, tt := range tests {
		task := NewGoTaskOfFunc(0, tt.fn, nil)
		err := task.Serialize()
		if err != nil {
			b.FailNow()
		}
		b.ResetTimer()
		b.Run(tt.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				w.Execute(task)
			}
		})
	}
}
