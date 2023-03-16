package delayed

import (
	"crypto/rand"
	"encoding/hex"
	"io"

	"github.com/keakon/golog/log"
)

func Recover() {
	if err := recover(); err != nil {
		log.Errorf("got a panic: %v", err)
	}
}

func RandHexString(size int) string {
	bs := make([]byte, size)
	if _, err := io.ReadFull(rand.Reader, bs); err != nil {
		return ""
	}
	return hex.EncodeToString(bs)
}
