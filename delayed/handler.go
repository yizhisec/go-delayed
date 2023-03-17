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
	if h.argCount == 1 {
		h.argType = fnType.In(0)
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
	}
	return
}

func (h *Handler) Call(payload []byte) (result []reflect.Value, err error) {
	defer Recover()

	if h.argCount == 0 {
		return h.fn.Call([]reflect.Value{}), nil
	}
	if h.argCount == 1 {
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
	// h.argCount > 1
	argStruct := reflect.New(h.argType)
	argStructIf := argStruct.Interface()
	err = msgpack.UnmarshalAsArray(payload, argStructIf)
	if err != nil {
		return nil, err
	}
	args := make([]reflect.Value, h.argCount)
	for i := 0; i < h.argCount; i++ {
		args[i] = argStruct.Elem().Field(i)
	}
	return h.fn.Call(args), nil
}
