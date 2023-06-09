package delayed

import (
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/keakon/golog/log"
)

const (
	StatusStopped uint32 = iota
	StatusRunning
	StatusStopping
)

const (
	defaultKeepAliveDuration = 15 * time.Second
	defaultSleepTime         = time.Second
	maxSleepTime             = time.Minute
)

type WorkerOption func(*Worker)

// KeepAliveDuration sets the keep alive duration of a worker.
func KeepAliveDuration(d time.Duration) WorkerOption {
	return func(w *Worker) {
		if d > 0 {
			w.keepAliveDuration = d
		} else {
			w.keepAliveDuration = defaultKeepAliveDuration
		}
	}
}

// Worker keeps dequeuing and processing Go tasks.
type Worker struct {
	id                string
	queue             *Queue
	handlers          map[string]*Handler
	status            uint32
	keepAliveDuration time.Duration
	sigChan           chan os.Signal
}

// NewWorker creates a new worker.
func NewWorker(queue *Queue, options ...WorkerOption) *Worker {
	id := RandHexString(16)
	queue.workerID = id
	worker := &Worker{
		id:                id,
		queue:             queue,
		handlers:          map[string]*Handler{},
		keepAliveDuration: defaultKeepAliveDuration,
	}

	for _, option := range options {
		option(worker)
	}

	return worker
}

// RegisterHandlers registers handlers.
// Tasks with function not been registered will be ignored.
func (w *Worker) RegisterHandlers(funcs ...interface{}) {
	for _, f := range funcs {
		h := NewHandler(f)
		if h != nil {
			w.handlers[h.path] = h
		} else {
			log.Warnf("%#v is not a valid handler", f)
		}
	}
}

// Run starts the worker.
func (w *Worker) Run() {
	log.Debugf("Starting worker %s.", w.id)

	atomic.StoreUint32(&w.status, StatusRunning)
	defer func() { atomic.StoreUint32(&w.status, StatusStopped) }()

	w.KeepAlive()
	defer w.Die()

	w.registerSignals()
	defer w.unregisterSignals()

	for atomic.LoadUint32(&w.status) == StatusRunning {
		w.run()
	}
}

func (w *Worker) run() {
	defer Recover() // try recover() out of execute() to reduce its overhead

	sleepTime := defaultSleepTime
	for atomic.LoadUint32(&w.status) == StatusRunning {
		task, err := w.queue.Dequeue()
		if err != nil {
			log.Errorf("Failed to dequeue task: %v", err)
			time.Sleep(sleepTime)
			sleepTime *= 2
			if sleepTime > maxSleepTime {
				sleepTime = maxSleepTime
			}
		} else {
			sleepTime = defaultSleepTime
		}
		if task == nil {
			continue
		}

		w.Execute(task)
	}
}

// Stop stops the worker.
func (w *Worker) Stop() {
	if atomic.LoadUint32(&w.status) == StatusRunning {
		log.Debugf("Stopping worker %s.", w.id)
		atomic.StoreUint32(&w.status, StatusStopping)
	}
}

func (w *Worker) registerSignals() {
	w.sigChan = make(chan os.Signal, 1)
	signal.Notify(w.sigChan, syscall.SIGHUP)
}

func (w *Worker) unregisterSignals() {
	signal.Reset()
	close(w.sigChan)
	w.sigChan = nil
}

// Execute executes a task.
func (w *Worker) Execute(t *GoTask) {
	h, ok := w.handlers[t.raw.FuncPath]
	if ok {
		_, err := h.Call(t.raw.Payload)
		if err != nil {
			log.Errorf("Failed to execute task %s: %v", t.raw.FuncPath, err)
		}
	} else {
		log.Debugf("Ignore unregistered task: %s", t.raw.FuncPath)
	}
}

// KeepAlive keeps the worker alive.
func (w *Worker) KeepAlive() {
	w.keepAlive()

	go func() {
		ticker := time.NewTicker(w.keepAliveDuration)
		defer ticker.Stop()

		for atomic.LoadUint32(&w.status) != StatusStopped { // should keep alive even stopping
			w.keepAlive()

			select {
			case <-w.sigChan:
				w.Stop()
			case <-ticker.C:
				continue
			}
		}
	}()
}

func (w *Worker) keepAlive() {
	err := w.queue.keepAlive()
	if err != nil {
		log.Error(err)
	}
}

// Die marks the worker as dead.
func (w *Worker) Die() {
	err := w.queue.die()
	if err != nil {
		log.Error(err)
	}
}
