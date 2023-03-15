package delayed

import (
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
)

func TestNewQueue(t *testing.T) {
	q := NewQueue("test", "localhost:6379", "", 600000, 2, 10000, 10)
	if q == nil {
		t.FailNow()
	}
}

func TestQueueEnqueue(t *testing.T) {
	q := NewQueue("test", "localhost:6379", "", 600000, 2, 10000, 10)
	defer q.Clear()

	err := q.Enqueue(NewTaskOfFunc(1, f1, nil, 0, false))
	if err != nil {
		t.Fatal(err)
	}
	err = q.Enqueue(NewTaskOfFunc(1, f1, tArg, 0, false))
	if err != nil {
		t.Fatal(err)
	}
}

func TestQueueLen(t *testing.T) {
	q := NewQueue("test", "localhost:6379", "", 600000, 2, 10000, 10)

	count, err := q.Len()
	if err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.FailNow()
	}

	for i, tt := range taskTestCases {
		task := NewTask(tt.id, tt.funcPath, tt.arg, tt.timeout, tt.prior)
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
	q := NewQueue("test", "localhost:6379", "", 600000, 2, 10000, 10)
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
			task1 := NewTask(tt.id, tt.funcPath, tt.arg, tt.timeout, tt.prior)
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
		task := NewTask(tt.id, tt.funcPath, tt.arg, tt.timeout, tt.prior)
		err := q.Enqueue(task)
		if err != nil {
			t.Fatal(err)
		}
		if tt.prior { // prepend
			tasks = append(tasks, nil)
			copy(tasks[1:], tasks)
			tasks[0] = task
		} else {
			tasks = append(tasks, task)
		}
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
	q := NewQueue("test", "localhost:6379", "", 600000, 2, 10000, 10)
	defer q.Clear()

	conn := q.redis.Get()
	defer conn.Close()

	for _, tt := range taskTestCases {
		task := NewTask(tt.id, tt.funcPath, tt.arg, tt.timeout, tt.prior)
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

		count, err := redis.Int(conn.Do("ZCARD", q.enqueuedKey))
		if err != nil {
			t.Fatal(err)
		}
		if count != total-i {
			t.FailNow()
		}

		count, err = redis.Int(conn.Do("ZCARD", q.dequeuedKey))
		if err != nil {
			t.Fatal(err)
		}
		if count != 1 {
			t.FailNow()
		}

		q.Release(task)

		count, err = redis.Int(conn.Do("ZCARD", q.enqueuedKey))
		if err != nil {
			t.Fatal(err)
		}
		if count != total-i-1 {
			t.FailNow()
		}

		count, err = redis.Int(conn.Do("ZCARD", q.dequeuedKey))
		if err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.FailNow()
		}
	}
}

func TestQueueRequeue(t *testing.T) {
	q := NewQueue("test", "localhost:6379", "", 600000, 2, 10000, 10)
	defer q.Clear()

	for _, tt := range taskTestCases {
		task := NewTask(tt.id, tt.funcPath, tt.arg, tt.timeout, tt.prior)
		err := q.Enqueue(task)
		if err != nil {
			t.Fatal(err)
		}
	}

	count, err := q.Len()
	if err != nil {
		t.Fatal(err)
	}
	if count != len(taskTestCases) {
		t.FailNow()
	}

	tasks := []*GoTask{}
	for i := 0; i < len(taskTestCases); i++ {
		task, err := q.Dequeue()
		if err != nil {
			t.Fatal(err)
		}
		tasks = append(tasks, task)
	}

	count, err = q.Len()
	if err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.FailNow()
	}

	for i, task := range tasks {
		ok, err := q.Requeue(task)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			t.FailNow()
		}

		count, err := q.Len()
		if err != nil {
			t.Fatal(err)
		}
		if count != i+1 {
			t.FailNow()
		}
	}
}

func TestQueueRequeueLost(t *testing.T) {
	q := NewQueue("test", "localhost:6379", "", 0, 2, 1, 10)
	defer q.Clear()

	for _, tt := range taskTestCases {
		task := NewTask(tt.id, tt.funcPath, tt.arg, tt.timeout, tt.prior)
		q.Enqueue(task)
		q.Dequeue()
	}

	count, err := q.Len()
	if err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.FailNow()
	}

	time.Sleep(time.Millisecond * 5) // should be larger than max(q.defaultTimeout, task.timeout) + q.requeueTimeout
	count, err = q.RequeueLost()
	if err != nil {
		t.Fatal(err)
	}
	if count != len(taskTestCases) {
		t.FailNow()
	}

	count, err = q.Len()
	if err != nil {
		t.Fatal(err)
	}
	if count != len(taskTestCases) {
		t.FailNow()
	}

	for i := 0; i < len(taskTestCases); i++ {
		task, err := q.Dequeue()
		if err != nil {
			t.Fatal(err)
		}
		if task == nil {
			t.FailNow()
		}

		err = q.Release(task)
		if err != nil {
			t.Fatal(err)
		}

		count, err = q.RequeueLost()
		if err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.FailNow()
		}

		count, err = q.Len()
		if err != nil {
			t.Fatal(err)
		}
		if count != len(taskTestCases)-i-1 {
			t.FailNow()
		}
	}
}
