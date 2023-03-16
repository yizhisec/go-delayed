package delayed

import (
	"reflect"
	"runtime"

	"github.com/keakon/golog/log"
	"github.com/shamaton/msgpack/v2"
)

type Handler struct {
	fn      reflect.Value
	argType reflect.Type
	path    string
}

var handlerCache = map[interface{}]*Handler{}

func parseFunc(f interface{}) (fn reflect.Value, fnType reflect.Type, path string) {
	fn = reflect.ValueOf(f)
	if fn.Kind() != reflect.Func {
		return
	}

	fnType = fn.Type()
	if fnType.NumIn() != 1 {
		return
	}

	path = runtime.FuncForPC(fn.Pointer()).Name()
	if path == "" {
		return
	}

	return
}

func NewHandler(f interface{}) *Handler {
	fn, fnType, path := parseFunc(f)
	if path == "" {
		return nil
	}

	return &Handler{
		fn:      fn,
		argType: fnType.In(0),
		path:    path,
	}
}

func (h *Handler) Call(payload []byte) ([]reflect.Value, error) {
	arg := reflect.New(h.argType)
	argIf := arg.Interface()
	if len(payload) != 0 { // not nil
		err := msgpack.Unmarshal(payload, argIf)
		if err != nil {
			log.Errorf("unmarshal payload error: %v", err)
			return nil, err
		}
	}
	return h.fn.Call([]reflect.Value{arg.Elem()}), nil
}
