package delayed

import (
	"crypto/rand"
	"encoding/hex"
	"io"

	"github.com/keakon/golog/log"
)

// Recover recovers from a panic.
func Recover() {
	if p := recover(); p != nil {
		log.Errorf("Got a panic: %v", p)
	}
}

// RandHexString generates a random hex string.
func RandHexString(size int) string {
	bs := make([]byte, size)
	if _, err := io.ReadFull(rand.Reader, bs); err != nil {
		return ""
	}
	return hex.EncodeToString(bs)
}
