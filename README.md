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

			func f2(a, b *Arg) int {
				return a.A + len(a.B) + b.A + len(b.B)
			}

			var task = delayed.NewGoTaskOfFunc(f1, Arg{A: 1, B: "test"})
			queue.Enqueue(task)
			task = delayed.NewGoTaskOfFunc(f2, []interface{}{1, &Arg{A: 1, B: "test"}})
			queue.Enqueue(task)
			task = delayed.NewGoTaskOfFunc(f2, 1, &Arg{A: 1, B: "test"}) // same as the above task
			queue.Enqueue(task)
			task = NewGoTaskOfFunc(syscall.Kill, os.Getpid(), syscall.SIGHUP)
			queue.Enqueue(task)
			```
		2. Create a task by func path:

			```Go
			task = delayed.NewGoTask("main.f2", 1, &Arg{A: 1, B: "test"})
			queue.Enqueue(task)
			```
			This is the preferred way because `delayed.NewGoTask()` is 100x faster than `delayed.NewGoTaskOfFunc()`.
	* Enqueue a Python task:

		```Go
		var task = delayed.NewPyTask(
			"module_path:func_name",
			[]interface{}{1, 2},  // args must be slice, array or nil
			Arg{A: 1, B: "test"}, // kwArgs must be map, struct or nil
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
