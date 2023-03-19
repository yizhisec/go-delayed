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
	WorkerStatusStopped uint32 = iota
	WorkerStatusRunning
	WorkerStatusStopping
)

const (
	defaultKeepAliveDuration = 15 * time.Second
	defaultSleepTime         = time.Second
	maxSleepTime             = time.Minute
)

type WorkerOption func(*Worker)

func KeepAliveDuration(d time.Duration) WorkerOption {
	return func(w *Worker) {
		if d > 0 {
			w.keepAliveDuration = d
		} else {
			w.keepAliveDuration = defaultKeepAliveDuration
		}
	}
}

type Worker struct {
	id                string
	queue             *Queue
	handlers          map[string]*Handler
	status            uint32
	keepAliveDuration time.Duration
	sigChan           chan os.Signal
}

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

func (w *Worker) RegisterHandlers(funcs ...interface{}) {
	for _, f := range funcs {
		h := NewHandler(f)
		if f != nil {
			w.handlers[h.path] = h
		}
	}
}

func (w *Worker) Run() {
	atomic.StoreUint32(&w.status, WorkerStatusRunning)
	defer func() { atomic.StoreUint32(&w.status, WorkerStatusStopped) }()

	w.KeepAlive()

	w.registerSignals()
	defer w.unregisterSignals()

	for atomic.LoadUint32(&w.status) == WorkerStatusRunning {
		w.run()
	}
}

func (w *Worker) run() {
	defer Recover() // try recover() out of execute() to reduce its overhead

	sleepTime := defaultSleepTime
	for atomic.LoadUint32(&w.status) == WorkerStatusRunning {
		task, err := w.queue.Dequeue()
		if err != nil {
			log.Errorf("dequeue task error: %v", err)
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

func (w *Worker) Stop() {
	if atomic.LoadUint32(&w.status) == WorkerStatusRunning {
		atomic.StoreUint32(&w.status, WorkerStatusStopping)
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

func (w *Worker) Execute(t *GoTask) {
	h, ok := w.handlers[t.raw.FuncPath]
	if ok {
		h.Call(t.raw.Payload)
	}
}

func (w *Worker) KeepAlive() {
	w.keepAlive()

	go func() {
		ticker := time.NewTicker(w.keepAliveDuration)
		defer ticker.Stop()

		for atomic.LoadUint32(&w.status) != WorkerStatusStopped { // should keep alive even stopping
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
