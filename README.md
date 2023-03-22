# go-delayed
[![GoDoc](https://pkg.go.dev/badge/github.com/yizhisec/go-delayed)](https://pkg.go.dev/github.com/yizhisec/go-delayed)
[![Build Status](https://github.com/yizhisec/go-delayed/actions/workflows/go.yml/badge.svg)](https://github.com/yizhisec/go-delayed/actions)
[![Go Report Card](https://goreportcard.com/badge/github.com/yizhisec/go-delayed)](https://goreportcard.com/report/github.com/yizhisec/go-delayed)
[![codecov](https://codecov.io/gh/yizhisec/go-delayed/branch/main/graph/badge.svg?token=YKJLNCK2P4)](https://codecov.io/gh/yizhisec/go-delayed)

Go-delayed is a simple but robust task queue inspired by [rq](https://python-rq.org/).

## Features

* Robust: all the enqueued tasks will run exactly once, even if the worker got killed at any time.
* Clean: finished tasks (including failed) take no space of your Redis.
* Distributed: workers as more as needed can run in the same time without further config.
* Portable: its [Go](https://github.com/yizhisec/go-delayed) and [Python](https://github.com/yizhisec/delayed) version can call each other.

## Requirements

1. Go 1.13 or later, tested on Go 1.13 and 1.20.
2. To gracefully stop the workers, Unix-like systems (with Unix signal) are required, tested on Ubuntu 22.04 and macOS Monterey 12.
3. Redis 2.6.0 or later (with Lua scripts).

## Getting started

1. Run a redis server:

    ```bash
    $ redis-server
    ```

2. Create a task queue:

    ```Go
	import "github.com/yizhisec/go-delayed/delayed"

	var queue = delayed.NewQueue("default", delayed.NewRedisPool(":6379")) // "default" is the queue name
    ```

3. Enqueue tasks:
	* Two ways to enqueue Go tasks:
		1. Define task functions:

			```Go
			type Arg struct {
				A int
				B string
			}

			func f1(a Arg) int {
				return a.A + len(a.B)
			}

			func f2(a int, b *Arg) int {
				return a + b.A + len(b.B)
			}

			func f3(a int, b *Arg, c []int) int {
				return a + b.A + len(b.B) + len(c)
			}

			func f4(a ...int) int {
				return len(a)
			}

			func f5(a int, b *Arg, c []int, d ...int) int {
				return a + b.A + len(b.B) + len(c) + len(d)
			}

			var task = delayed.NewGoTaskOfFunc(f1, Arg{A: 1, B: "test"})
			queue.Enqueue(task)
			task = delayed.NewGoTaskOfFunc(f2, []interface{}{1, &Arg{A: 1, B: "test"}})
			queue.Enqueue(task)
			task = delayed.NewGoTaskOfFunc(f2, 1, &Arg{A: 1, B: "test"}) // same as the above task
			queue.Enqueue(task)
			task = delayed.NewGoTaskOfFunc(f2, 1, []interface{}{1, "test"}) // use slice as strut is also ok
			queue.Enqueue(task)
			task = delayed.NewGoTaskOfFunc(f3, new(uint), Arg{A: 1, B: "test"}, []interface{}{uint(2), int8(3)}) // use compatible arguments is also ok
			queue.Enqueue(task)
			task = delayed.NewGoTaskOfFunc(f4, []interface{}{1, 2, 3})
			queue.Enqueue(task)
			task = delayed.NewGoTaskOfFunc(f4, 1, 2, 3) // if the variadic argument is the only argument, it's not required to build a slice
			queue.Enqueue(task)
			task = delayed.NewGoTaskOfFunc(f5, 1, &Arg{A: 1, B: "test"}, []int{2, 3}, []int{4, 5, 6})
			queue.Enqueue(task)
			task = delayed.NewGoTaskOfFunc(syscall.Kill, os.Getpid(), syscall.SIGHUP)
			queue.Enqueue(task)
			```
		2. Create a task by func path:

			```Go
			task = delayed.NewGoTask("main.f2", 1, &Arg{A: 1, B: "test"})
			queue.Enqueue(task)

			task = delayed.NewGoTask("net/http.Get", "http://example.com/")
			queue.Enqueue(task)
			```
			This is the preferred way because `delayed.NewGoTask()` is 100x faster than `delayed.NewGoTaskOfFunc()`.
	* Enqueue a Python task:

		```Go
		var task = delayed.NewPyTask(
			"module.path:func_name", // eg: os.path:join
			[]interface{}{1, 2},     // args must be a slice, array or nil
			Arg{A: 1, B: "test"},    // kwArgs must be a map, struct or nil
		)
		queue.Enqueue(task)
		```

5. Run a task worker (or more) in a separated process:

    ```Go
	w := delayed.NewWorker(delayed.NewQueue("test", delayed.NewRedisPool(":6379")))
	w.RegisterHandlers(f1, f2, syscall.Kill) // tasks with function not been registered will be ignored
	w.Run()
    ```

6. Run a task sweeper in a separated process to recovery lost tasks (mainly due to the worker got killed):

    ```Go
	delayed.NewSweeper(
		delayed.NewQueue("default", delayed.NewRedisPool(":6379")),
		delayed.NewQueue("test", delayed.NewRedisPool(":6380")),
	).Run()
    ```

## QA

1. **Q: What's the limitation on a task function?**  
A: A Go task function must be exported and has a name. So `func f(){}` and `var F = func(){}` cannot be task functions.
Its args should be exported and be serializable by [MessagePack](https://msgpack.org/).

2. **Q: What's the `name` param of a queue?**  
A: It's the key used to store the tasks of the queue. A queue with name "default" will use those keys:
    * default: list, enqueued tasks.
    * default_id: str, the next task id.
    * default_noti: list, the same length as enqueued tasks.
    * default_processing: hash, the processing task of workers.

3. **Q: What's lost tasks?**  
A: There are 2 situations a task might get lost:
    * a worker popped a task notification, then got killed before dequeueing the task.
    * a worker dequeued a task, then got killed before releasing the task.

4. **Q: How to recovery lost tasks?**  
A: Runs a sweeper. It dose two things:
    * it keeps the task notification length the same as the task queue.
    * it checks the processing list, if the worker is dead, moves the processing task back to the task queue.

5. **Q: How to turn on the debug logs?**  
A: Sets the default logger to debug level:

    ```Go
	import (
		"github.com/keakon/golog"
		"github.com/keakon/golog/log"
	)

	h := golog.NewHandler(golog.DebugLevel, golog.DefaultFormatter)
	h.AddWriter(golog.NewStdoutWriter())
	l := golog.NewLogger(golog.DebugLevel)
	l.AddHandler(h)
	log.SetDefaultLogger(l)
    ```
