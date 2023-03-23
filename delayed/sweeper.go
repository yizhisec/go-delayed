package delayed

import (
	"sync/atomic"
	"time"

	"github.com/keakon/golog/log"
)

const defaultSweeperInterval = time.Minute

// Sweeper keeps recovering lost tasks.
type Sweeper struct {
	queues   []*Queue
	interval time.Duration
	status   uint32
}

// NewSweeper creates a new sweeper.
func NewSweeper(queues ...*Queue) *Sweeper {
	return &Sweeper{
		queues:   queues,
		interval: defaultSweeperInterval,
	}
}

// SetInterval sets the interval of the sweeper.
func (s *Sweeper) SetInterval(interval time.Duration) {
	if interval > 0 {
		s.interval = interval
	}
}

// Run starts the sweeper.
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

// Stop stops the sweeper.
func (s *Sweeper) Stop() {
	if atomic.LoadUint32(&s.status) == StatusRunning {
		atomic.StoreUint32(&s.status, StatusStopping)
	}
}
