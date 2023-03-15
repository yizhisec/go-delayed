package delayed

import (
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/keakon/golog/log"
)

type WorkerStatus uint8

const (
	WorkerStatusStopped WorkerStatus = iota
	WorkerStatusRunning
	WorkerStatusStopping
)

type Worker struct {
	queue    *Queue
	handlers map[string]*Handler
	status   WorkerStatus
	sigChan  chan os.Signal

	childPid     int
	taskReader   *os.File
	taskWriter   *os.File
	resultReader *os.File
	resultWriter *os.File
}

func NewWorker(queue *Queue) *Worker {
	return &Worker{
		queue:    queue,
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

func (w *Worker) GetHandler(name string) *Handler {
	return w.handlers[name]
}

func (w *Worker) Run() {
	defer Recover()

	w.status = WorkerStatusRunning
	defer func() { w.status = WorkerStatusStopped }()

	w.registerSignals()
	defer w.unregisterSignals()

	for w.status == WorkerStatusRunning {
		task, err := w.queue.Dequeue()
		if err != nil {
			log.Errorf("dequeue task error: %v", err)
			time.Sleep(time.Second) // TODO: increase sleep time
		}
		if task == nil {
			continue
		}

		if w.childPid == 0 { // fork child worker
			err = w.createPipes()
			if err != nil {
				w.requeue(task)
				continue
			}

			r1, r2, e := syscall.Syscall(syscall.SYS_FORK, 0, 0, 0)
			if e != 0 {
				log.Errorf("fork error: %v", e)
				w.requeue(task)
				continue
			}
			if r2 == 0 { // parent
				w.childPid = int(r1)
				log.Debugf("forked a child worker: %d", w.childPid)

				w.taskReader.Close()
				w.resultWriter.Close()
			} else { // child
				w.unregisterSignals()
				w.runTasks()
				os.Exit(1) // should run forever except got killed or its parent exited
			}
		}

		w.monitorTask(task)

		if w.childPid == 0 {
			w.taskWriter.Close()
			w.resultReader.Close()
		}
	}
}

func (w *Worker) Stop() {
	if w.status == WorkerStatusRunning {
		w.status = WorkerStatusStopping
	}
}

func (w *Worker) registerSignals() {
	w.sigChan = make(chan os.Signal, 1)
	signal.Notify(w.sigChan, syscall.SIGCHLD)
}

func (w *Worker) unregisterSignals() {
	signal.Reset()
	close(w.sigChan)
	w.sigChan = nil
}

func (w *Worker) createPipes() (err error) {
	w.taskReader, w.taskWriter, err = os.Pipe()
	if err != nil {
		log.Errorf("create pipe error: %v", err)
		time.Sleep(time.Second) // TODO: increase sleep time
		return
	}

	w.resultReader, w.resultWriter, err = os.Pipe()
	if err != nil {
		log.Errorf("create pipe error: %v", err)
		time.Sleep(time.Second) // TODO: increase sleep time
	}
	io.Pipe()
	return
}

func (w *Worker) requeue(task *GoTask) {
	_, err := w.queue.Requeue(task)
	if err != nil {
		log.Errorf("requeue task error: %v", err)
	}
	time.Sleep(time.Second) // TODO: increase sleep time
}

func (w *Worker) monitorTask(task *GoTask) {
	var timeout uint32
	if task.raw.Timeout > 0 {
		timeout = task.raw.Timeout
	} else {
		timeout = w.queue.defaultTimeout
	}

	w.sendTask(task)
	timer := time.NewTimer(time.Duration(timeout) * time.Millisecond)

	for {
		select {
		case <-timer.C:
			log.Debugf("task %d timeout", task.raw.ID)
			syscall.Kill(w.childPid, syscall.SIGKILL)
		case <-w.sigChan:
			var status syscall.WaitStatus
			pid, err := syscall.Wait4(w.childPid, &status, syscall.WNOHANG, nil)
			if err != nil {
				log.Errorf("wait4 error: %v", err)
				return
			}
			if pid == w.childPid {
				if status.Exited() {
					log.Debugf("child worker %d exited: %d", w.childPid, status.ExitStatus())
					w.childPid = 0
					return
				}
				if status.Signaled() {
					log.Debugf("child worker %d signaled: %d", w.childPid, status.Signal())
					w.childPid = 0
					return
				}
				// TODO: rerun task
			}
		}
	}
}

func (w *Worker) sendTask(task *GoTask) {
}

func (w *Worker) runTasks() {

}
