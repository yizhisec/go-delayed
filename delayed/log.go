package delayed

import (
	"github.com/keakon/golog"
	"github.com/keakon/golog/log"
)

func initLogger(level golog.Level) {
	logger := golog.NewStderrLogger()
	golog.SetInternalLogger(logger)
	log.SetDefaultLogger(logger)
	return
}
