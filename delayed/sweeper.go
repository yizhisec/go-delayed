package delayed

import (
	"sync/atomic"
	"time"

	"github.com/keakon/golog/log"
)

const defaultSweeperInterval = time.Minute

type Sweeper struct {
	queues   []*Queue
	interval time.Duration
	status   uint32
}

func NewSweeper(queues ...*Queue) *Sweeper {
	return &Sweeper{
		queues:   queues,
		interval: defaultSweeperInterval,
	}
}

func (s *Sweeper) SetInterval(interval time.Duration) {
	if interval > 0 {
		s.interval = interval
	}
}

func (s *Sweeper) Run() {
	s.status = StatusRunning
	defer func() { atomic.StoreUint32(&s.status, StatusStopped) }()

	for atomic.LoadUint32(&s.status) == StatusRunning {
		s.run()
		time.Sleep(s.interval)
	}
}

func (s *Sweeper) run() {
	for _, queue := range s.queues {
		_, err := queue.RequeueLost()
		if err != nil {
			log.Error(err)
		}
	}
}

func (s *Sweeper) Stop() {
	if atomic.LoadUint32(&s.status) == StatusRunning {
		atomic.StoreUint32(&s.status, StatusStopping)
	}
}
