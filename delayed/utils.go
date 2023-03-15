package delayed

import (
	"github.com/keakon/golog/log"
	"golang.org/x/exp/constraints"
)

func InterfaceToInteger[T constraints.Integer](i interface{}) (T, bool) {
	switch i := i.(type) {
	case uint8:
		return T(i), true
	case int8:
		return T(i), true
	case uint16:
		return T(i), true
	case int16:
		return T(i), true
	case uint32:
		return T(i), true
	case int32:
		return T(i), true
	case uint64:
		return T(i), true
	case int64:
		return T(i), true
	}
	return 0, false
}

func InterfaceToSigned[T constraints.Signed](i interface{}) (T, bool) {
	switch i := i.(type) {
	case int8:
		return T(i), true
	case int16:
		return T(i), true
	case int32:
		return T(i), true
	case int64:
		return T(i), true
	}
	return 0, false
}

func InterfaceToUnsigned[T constraints.Unsigned](i interface{}) (T, bool) {
	switch i := i.(type) {
	case uint8:
		return T(i), true
	case uint16:
		return T(i), true
	case uint32:
		return T(i), true
	case uint64:
		return T(i), true
	}
	return 0, false
}

func Recover() {
	if err := recover(); err != nil {
		log.Errorf("got a panic: %v", err)
	}
}
