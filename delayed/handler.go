package delayed

import (
	"reflect"
	"runtime"
	"strconv"
	"strings"

	"github.com/keakon/golog/log"
	"github.com/shamaton/msgpack/v2"
)

// A handler stores a function and other information about how to call it.
type Handler struct {
	fn         reflect.Value // the reflected function
	path       string
	argCount   int
	arg        interface{}     // a point to the only argument or to a struct which represents the arguments
	args       []reflect.Value // the prebuilt arguments for fn.Call() or fn.CallSlice(), each element of it references the same one as arg (the only argument) or one field of arg (a struct represents the arguments)
	isVariadic bool
}

// NewHandler creates a handler for a function.
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
		} else {
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
			argElem := arg.Elem()
			h.arg = arg.Interface()
			h.args = make([]reflect.Value, h.argCount)
			for i := 0; i < h.argCount; i++ {
				h.args[i] = argElem.Field(i)
			}
		}
	}
	return
}

// Call executes the function of a handler.
func (h *Handler) Call(payload []byte) (result []reflect.Value, err error) {
	if h.argCount > 0 && len(payload) > 0 {
		err := msgpack.UnmarshalAsArray(payload, h.arg)
		if err != nil {
			log.Errorf("Failed to unmarshal payload: %v", err)
			return nil, err
		}
	}
	if h.isVariadic {
		return h.fn.CallSlice(h.args), nil
	}
	return h.fn.Call(h.args), nil
}
