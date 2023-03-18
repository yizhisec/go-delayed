package delayed

import (
	"reflect"
	"runtime"
	"strconv"

	"github.com/keakon/golog/log"
	"github.com/shamaton/msgpack/v2"
)

type Handler struct {
	fn       reflect.Value
	path     string
	argCount int
	argType  reflect.Type
	arg      interface{}
	args     []reflect.Value
}

func NewHandler(f interface{}) (h *Handler) {
	fn := reflect.ValueOf(f)
	if fn.Kind() != reflect.Func {
		return nil
	}

	path := runtime.FuncForPC(fn.Pointer()).Name()
	if path == "" {
		return nil
	}

	h = &Handler{
		fn:   fn,
		path: path,
	}

	fnType := fn.Type()
	h.argCount = fnType.NumIn()
	// the rest fields can be reused among tasks, because the worker won't handle tasks concurrently
	if h.argCount == 0 {
		h.args = []reflect.Value{}
	} else if h.argCount == 1 {
		h.argType = fnType.In(0)
		arg := reflect.New(h.argType)
		h.arg = arg.Interface()
		h.args = []reflect.Value{arg.Elem()}
	} else if h.argCount > 1 {
		fields := make([]reflect.StructField, h.argCount)
		for i := 0; i < h.argCount; i++ {
			arg := fnType.In(i)
			fields[i] = reflect.StructField{
				Name: "F" + strconv.Itoa(i),
				Type: arg,
			}
		}
		h.argType = reflect.StructOf(fields)
		arg := reflect.New(h.argType)
		h.arg = arg.Interface()
		h.args = make([]reflect.Value, h.argCount)
		for i := 0; i < h.argCount; i++ {
			h.args[i] = arg.Elem().Field(i)
		}
	}
	return
}

func (h *Handler) Call(payload []byte) (result []reflect.Value, err error) {
	if h.argCount == 0 {
		return h.fn.Call(h.args), nil
	}
	if h.argCount == 1 {
		if len(payload) != 0 { // not nil
			err := msgpack.Unmarshal(payload, h.arg)
			if err != nil {
				log.Errorf("unmarshal payload error: %v", err)
				return nil, err
			}
		}
		return h.fn.Call(h.args), nil
	}
	// h.argCount > 1
	err = msgpack.UnmarshalAsArray(payload, h.arg)
	if err != nil {
		return nil, err
	}
	return h.fn.Call(h.args), nil
}
