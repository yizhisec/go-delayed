package delayed

import (
	"bytes"
	"reflect"
	"runtime"

	"github.com/keakon/golog/log"
	"github.com/shamaton/msgpack/v2"
)

type Task interface {
	Serialize() ([]byte, error)
	getID() *uint64
	getData() []byte
}

type RawGoTask struct {
	ID       uint64
	FuncPath string
	Payload  []byte // serialized arg
}

type GoTask struct {
	raw  RawGoTask // make it unexported but can be serialized by MessagePack
	arg  interface{}
	data []byte // serialized data
}

func NewGoTask(funcPath string, arg ...interface{}) *GoTask {
	var a interface{}
	if len(arg) == 1 {
		a = arg[0]
	} else {
		a = arg
	}
	return &GoTask{
		raw: RawGoTask{
			FuncPath: funcPath,
		},
		arg: a,
	}
}

func NewGoTaskOfFunc(f interface{}, arg ...interface{}) *GoTask {
	fn := reflect.ValueOf(f)
	if fn.Kind() != reflect.Func {
		return nil
	}

	funcPath := runtime.FuncForPC(fn.Pointer()).Name()
	if funcPath == "" {
		return nil
	}

	var a interface{}
	if len(arg) == 1 {
		a = arg[0]
	} else {
		a = arg
	}
	return &GoTask{
		raw: RawGoTask{
			FuncPath: funcPath,
		},
		arg: a,
	}
}

func (t *GoTask) Equal(task *GoTask) bool {
	// it may return false if one task is not serialized and the other is deserialized
	return t.raw.ID == task.raw.ID && t.raw.FuncPath == task.raw.FuncPath && (bytes.Equal(t.raw.Payload, task.raw.Payload) || reflect.DeepEqual(t.arg, task.arg))
}

func (t *GoTask) Serialize() (data []byte, err error) {
	if t.arg != nil {
		if reflect.TypeOf(t.arg).Kind() == reflect.Slice {
			t.raw.Payload, err = msgpack.MarshalAsArray(t.arg)
		} else {
			t.raw.Payload, err = msgpack.Marshal(t.arg)
		}
		if err != nil {
			log.Errorf("Failed to serialize task.arg: %v", err)
			return
		}
	}

	t.data, err = msgpack.MarshalAsArray(&t.raw)
	if err != nil {
		log.Errorf("Failed to serialize task.data: %v", err)
		return
	}
	return t.data, nil
}

func DeserializeGoTask(data []byte) (task *GoTask, err error) {
	t := &GoTask{
		data: data,
	}
	err = msgpack.UnmarshalAsArray(data, &t.raw)
	if err != nil {
		log.Errorf("Failed to deserialize task: %v", err)
		return
	}
	return t, nil
}

func (t *GoTask) getID() *uint64 {
	return &t.raw.ID
}

func (t *GoTask) getData() []byte {
	return t.data
}

type RawPyTask struct {
	ID       uint64
	FuncPath string
	Args     interface{} // must be slice, array or nil
	KwArgs   interface{} // must be map, struct or nil
}

type PyTask struct {
	raw  RawPyTask // make it unexported but can be serialized by MessagePack
	data []byte    // serialized data
}

func NewPyTask(funcPath string, args, kwArgs interface{}) *PyTask {
	return &PyTask{
		raw: RawPyTask{
			FuncPath: funcPath,
			Args:     args,
			KwArgs:   kwArgs,
		},
	}
}

func (t *PyTask) Serialize() (data []byte, err error) {
	t.data, err = msgpack.MarshalAsArray(&t.raw)
	if err != nil {
		log.Errorf("Failed to serialize task.data: %v", err)
		return
	}
	return t.data, nil
}

func (t *PyTask) getID() *uint64 {
	return &t.raw.ID
}

func (t *PyTask) getData() []byte {
	return t.data
}
