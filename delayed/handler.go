package delayed

import (
	"reflect"
	"runtime"
	"strconv"
	"strings"

	"github.com/keakon/golog/log"
	"github.com/shamaton/msgpack/v2"
)

type Handler struct {
	fn         reflect.Value
	path       string
	isVariadic bool
	argCount   int
	arg        interface{}
	args       []reflect.Value
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

	fnType := fn.Type()
	h = &Handler{
		fn:       fn,
		path:     path,
		argCount: fnType.NumIn(),
	}

	// the rest fields can be reused among tasks, because the worker won't handle tasks concurrently
	if h.argCount == 0 {
		h.args = []reflect.Value{}
	} else {
		h.isVariadic = strings.Contains(fnType.String(), "...")
		if h.argCount == 1 {
			argType := fnType.In(0)
			arg := reflect.New(argType)
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
			argType := reflect.StructOf(fields)
			arg := reflect.New(argType)
			h.arg = arg.Interface()
			h.args = make([]reflect.Value, h.argCount)
			for i := 0; i < h.argCount; i++ {
				h.args[i] = arg.Elem().Field(i)
			}
		}
	}
	return
}

func (h *Handler) Call(payload []byte) (result []reflect.Value, err error) {
	if h.argCount > 0 && len(payload) > 0 {
		err := msgpack.UnmarshalAsArray(payload, h.arg)
		if err != nil {
			log.Errorf("unmarshal payload error: %v", err)
			return nil, err
		}
	}
	if h.isVariadic {
		return h.fn.CallSlice(h.args), nil
	}
	return h.fn.Call(h.args), nil
}
