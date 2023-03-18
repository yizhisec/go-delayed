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

const defaultKeepAliveDuration uint16 = 15

type WorkerOption func(*Worker)

func KeepAliveDuration(s uint16) WorkerOption {
	return func(w *Worker) {
		if s > 0 {
			w.keepAliveDuration = s
		} else {
			w.keepAliveDuration = defaultKeepAliveDuration
		}
	}
}

type Worker struct {
	id                string
	queue             *Queue
	handlers          map[string]*Handler
	status            atomic.Uint32
	keepAliveDuration uint16 // seconds
	sigChan           chan os.Signal
}

func NewWorker(name string, queue *Queue, options ...WorkerOption) *Worker {
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
	w.status.Store(WorkerStatusRunning)
	defer func() { w.status.Store(WorkerStatusStopped) }()

	w.KeepAlive()

	w.registerSignals()
	defer w.unregisterSignals()

	for w.status.Load() == WorkerStatusRunning {
		w.run()
	}
}

func (w *Worker) run() {
	defer Recover() // try recover() out of execute() to reduce its overhead

	for w.status.Load() == WorkerStatusRunning {
		task, err := w.queue.Dequeue()
		if err != nil {
			log.Errorf("dequeue task error: %v", err)
			time.Sleep(time.Second) // TODO: increase sleep time
		}
		if task == nil {
			continue
		}

		w.Execute(task)
	}
}

func (w *Worker) Stop() {
	if w.status.Load() == WorkerStatusRunning {
		w.status.Store(WorkerStatusStopping)
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
		ticker := time.NewTicker(time.Second * time.Duration(w.keepAliveDuration))
		defer ticker.Stop()

		for w.status.Load() != WorkerStatusStopped { // should keep alive even stopping
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
