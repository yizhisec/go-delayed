package delayed

import (
	"bytes"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
)

func TestNewQueue(t *testing.T) {
	q := NewQueue("test", NewRedisPool(redisAddr))
	if q == nil {
		t.FailNow()
	}
}

func TestQueueEnqueue(t *testing.T) {
	q := NewQueue("test", NewRedisPool(redisAddr))
	defer q.Clear()

	err := q.Enqueue(NewGoTaskOfFunc(f1, nil))
	if err != nil {
		t.Fatal(err)
	}
	err = q.Enqueue(NewGoTaskOfFunc(f1, tArg))
	if err != nil {
		t.Fatal(err)
	}
}

func TestQueueLen(t *testing.T) {
	q := NewQueue("test", NewRedisPool(redisAddr))

	count, err := q.Len()
	if err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.FailNow()
	}

	for i, tt := range taskTestCases {
		task := NewGoTask(tt.funcPath, tt.arg)
		*task.getID() = tt.id
		err := q.Enqueue(task)
		if err != nil {
			t.Fatal(err)
		}
		count, err = q.Len()
		if err != nil {
			t.Fatal(err)
		}
		if count != i+1 {
			t.FailNow()
		}
	}

	q.Clear()
	count, err = q.Len()
	if err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.FailNow()
	}
}

func TestQueueDequeue(t *testing.T) {
	q := NewQueue("test", NewRedisPool(redisAddr), DequeueTimeout(time.Millisecond*2))
	defer q.Clear()

	task, err := q.Dequeue()
	if err != nil {
		t.Fatal(err)
	}
	if task != nil {
		t.FailNow()
	}

	for _, tt := range taskTestCases {
		t.Run(tt.name, func(t *testing.T) {
			task1 := NewGoTask(tt.funcPath, tt.arg)
			*task1.getID() = tt.id
			err := q.Enqueue(task1)
			if err != nil {
				t.Fatal(err)
			}

			task2, err := q.Dequeue()
			if err != nil {
				t.Fatal(err)
			}

			if task2 == nil {
				t.FailNow()
			}

			if !task1.Equal(task2) {
				t.FailNow()
			}
		})
	}

	q.Clear()
	tasks := []*GoTask{}
	for _, tt := range taskTestCases {
		task := NewGoTask(tt.funcPath, tt.arg)
		*task.getID() = tt.id
		err := q.Enqueue(task)
		if err != nil {
			t.Fatal(err)
		}
		tasks = append(tasks, task)
	}

	for _, task1 := range tasks {
		task2, err := q.Dequeue()
		if err != nil {
			t.Fatal(err)
		}

		if !task1.Equal(task2) {
			t.FailNow()
		}
	}
}

func TestQueueRelease(t *testing.T) {
	q := NewQueue("test", NewRedisPool(redisAddr))
	defer q.Clear()

	conn := q.redis.Get()
	defer conn.Close()

	for _, tt := range taskTestCases {
		task := NewGoTask(tt.funcPath, tt.arg)
		*task.getID() = tt.id
		err := q.Enqueue(task)
		if err != nil {
			t.Fatal(err)
		}
	}

	count, err := q.Len()
	if err != nil {
		t.Fatal(err)
	}
	total := len(taskTestCases)
	if count != total {
		t.FailNow()
	}

	for i := 0; i < total; i++ {
		task, err := q.Dequeue()
		if err != nil {
			t.Fatal(err)
		}

		count, err := redis.Int(conn.Do("LLEN", q.name))
		if err != nil {
			t.Fatal(err)
		}
		if count != total-i-1 {
			t.FailNow()
		}

		count, err = redis.Int(conn.Do("LLEN", q.notiKey))
		if err != nil {
			t.Fatal(err)
		}
		if count != total-i-1 {
			t.FailNow()
		}

		count, err = redis.Int(conn.Do("HLEN", q.processingKey))
		if err != nil {
			t.Fatal(err)
		}
		if count != 1 {
			t.FailNow()
		}

		data, err := redis.Bytes(conn.Do("HGET", q.processingKey, q.workerID))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(data, task.data) {
			t.FailNow()
		}

		q.Release()

		count, err = redis.Int(conn.Do("LLEN", q.name))
		if err != nil {
			t.Fatal(err)
		}
		if count != total-i-1 {
			t.FailNow()
		}

		count, err = redis.Int(conn.Do("LLEN", q.notiKey))
		if err != nil {
			t.Fatal(err)
		}
		if count != total-i-1 {
			t.FailNow()
		}

		count, err = redis.Int(conn.Do("HLEN", q.processingKey))
		if err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.FailNow()
		}
	}
}

func TestQueueRequeueLost(t *testing.T) {
	q := NewQueue("test", NewRedisPool(redisAddr))
	defer q.Clear()

	for _, tt := range taskTestCases {
		task := NewGoTask(tt.funcPath, tt.arg)
		*task.getID() = tt.id
		q.Enqueue(task)
		q.Dequeue()
	}

	assertLen := func(c int) {
		count, err := q.Len()
		if err != nil {
			t.Fatal(err)
		}
		if count != c {
			t.FailNow()
		}
	}

	assertLostLen := func(c int) {
		count, err := q.RequeueLost()
		if err != nil {
			t.Fatal(err)
		}
		if count != c {
			t.FailNow()
		}
	}

	assertLen(0)

	assertLostLen(1)
	assertLen(1)

	task, err := q.Dequeue()
	if err != nil {
		t.Fatal(err)
	}
	if task == nil {
		t.FailNow()
	}
	if task.raw.ID != taskTestCases[len(taskTestCases)-1].id {
		t.FailNow()
	}

	err = q.Release()
	if err != nil {
		t.Fatal(err)
	}

	assertLostLen(0)
	assertLen(0)

	q.keepAlive()
	tt := taskTestCases[0]
	task = NewGoTask(tt.funcPath, tt.arg)
	*task.getID() = tt.id
	q.Enqueue(task)
	q.Dequeue()
	assertLostLen(0)
	assertLen(0)

	q.die()
	assertLostLen(1)
	assertLen(1)

	task, err = q.Dequeue()
	if err != nil {
		t.Fatal(err)
	}
	if task == nil {
		t.FailNow()
	}
	if task.raw.ID != tt.id {
		t.FailNow()
	}

	err = q.Release()
	if err != nil {
		t.Fatal(err)

	}

	assertLostLen(0)
	assertLen(0)
}
