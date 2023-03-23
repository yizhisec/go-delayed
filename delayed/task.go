package delayed

import (
	"bytes"
	"reflect"
	"runtime"

	"github.com/keakon/golog/log"
	"github.com/shamaton/msgpack/v2"
)

// Task is the interface of both GoTask and PyTask.
type Task interface {
	Serialize() ([]byte, error)
	getFuncPath() string
}

// RawGoTask store the fields need to be serialized for a GoTask.
type RawGoTask struct {
	FuncPath string
	Payload  []byte // serialized arg
}

// GoTask store a RawGoTask and the serialized data.
type GoTask struct {
	raw  RawGoTask // make it unexported but can be serialized by MessagePack
	arg  interface{}
	data []byte // serialized data
}

// NewGoTask creates a new GoTask by the function path.
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

// NewGoTaskOfFunc creates a new GoTask by a function.
// It's about 100x slower than NewGoTask.
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

// Equal returns if two tasks are equal.
// It may return false if one task is not serialized and the other is deserialized.
func (t *GoTask) Equal(task *GoTask) bool {
	return t.raw.FuncPath == task.raw.FuncPath && (bytes.Equal(t.raw.Payload, task.raw.Payload) || reflect.DeepEqual(t.arg, task.arg))
}

// Serialize returns the serialized data of the task.
func (t *GoTask) Serialize() (data []byte, err error) {
	if len(t.data) != 0 {
		return t.data, nil
	}

	if t.arg != nil {
		t.raw.Payload, err = msgpack.MarshalAsArray(t.arg)
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

// DeserializeGoTask creates a new GoTask from the serialized data.
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

func (t *GoTask) getFuncPath() string {
	return t.raw.FuncPath
}

// RawPyTask store the fields need to be serialized for a PyTask.
type RawPyTask struct {
	FuncPath string
	Args     interface{} // must be slice, array or nil
	KwArgs   interface{} // must be map, struct or nil
}

// PyTask store a RawPyTask and the serialized data.
type PyTask struct {
	raw  RawPyTask // make it unexported but can be serialized by MessagePack
	data []byte    // serialized data
}

// NewPyTask creates a new PyTask by the function path.
func NewPyTask(funcPath string, args, kwArgs interface{}) *PyTask {
	return &PyTask{
		raw: RawPyTask{
			FuncPath: funcPath,
			Args:     args,
			KwArgs:   kwArgs,
		},
	}
}

// Serialize returns the serialized data of the task.
func (t *PyTask) Serialize() (data []byte, err error) {
	if t.data == nil {
		t.data, err = msgpack.MarshalAsArray(&t.raw)
		if err != nil {
			log.Errorf("Failed to serialize task.data: %v", err)
			return
		}
	}
	return t.data, nil
}

func (t *PyTask) getFuncPath() string {
	return t.raw.FuncPath
}
