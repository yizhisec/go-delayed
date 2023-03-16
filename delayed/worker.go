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

type Worker struct {
	id       string
	queue    *Queue
	handlers map[string]*Handler
	status   atomic.Uint32
	sigChan  chan os.Signal
}

func NewWorker(name, redisAddr, redisPassword string, dequeueTimeout uint32) *Worker {
	id := RandHexString(16)
	return &Worker{
		id:       id,
		queue:    NewQueue(id, name, redisAddr, redisPassword, dequeueTimeout),
		handlers: map[string]*Handler{},
	}
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
		defer Recover()
		h.Call(t.raw.Payload)
	}
}

func (w *Worker) KeepAlive() {
	w.keepAlive()

	go func() {
		ticker := time.NewTicker(time.Second * 15)
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
