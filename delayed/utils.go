package delayed

import (
	"crypto/rand"
	"encoding/hex"
	"io"

	"github.com/keakon/golog/log"
)

func Recover() {
	if p := recover(); p != nil {
		log.Errorf("got a panic: %v", p)
	}
}

func RandHexString(size int) string {
	bs := make([]byte, size)
	if _, err := io.ReadFull(rand.Reader, bs); err != nil {
		return ""
	}
	return hex.EncodeToString(bs)
}
