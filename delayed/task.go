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
	getID() uint64
	setID(uint64)
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

func NewGoTask(funcPath string, arg interface{}) *GoTask {
	return &GoTask{
		raw: RawGoTask{
			FuncPath: funcPath,
		},
		arg: arg,
	}
}

func NewGoTaskOfFunc(f, arg interface{}) *GoTask {
	fn := reflect.ValueOf(f)
	if fn.Kind() != reflect.Func {
		return nil
	}

	funcPath := runtime.FuncForPC(fn.Pointer()).Name()
	if funcPath == "" {
		return nil
	}

	return &GoTask{
		raw: RawGoTask{
			FuncPath: funcPath,
		},
		arg: arg,
	}
}

func (t *GoTask) Equal(task *GoTask) bool {
	// it may return false if one task is not serialized and the other is deserialized
	return t.raw.ID == task.raw.ID && t.raw.FuncPath == task.raw.FuncPath && (bytes.Equal(t.raw.Payload, task.raw.Payload) || reflect.DeepEqual(t.arg, task.arg))
}

func (t *GoTask) Serialize() (data []byte, err error) {
	if t.arg != nil {
		t.raw.Payload, err = msgpack.Marshal(t.arg)
		if err != nil {
			log.Errorf("serialize task.arg error: %v", err)
			return
		}
	}

	t.data, err = msgpack.MarshalAsArray(&t.raw)
	if err != nil {
		log.Errorf("serialize task.data error: %v", err)
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
		log.Errorf("deserialize task error: %v", err)
		return
	}
	return t, nil
}

func (t *GoTask) getID() uint64 {
	return t.raw.ID
}

func (t *GoTask) setID(id uint64) {
	t.raw.ID = id
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
		log.Errorf("serialize task.data error: %v", err)
		return
	}
	return t.data, nil
}

func (t *PyTask) getID() uint64 {
	return t.raw.ID
}

func (t *PyTask) setID(id uint64) {
	t.raw.ID = id
}

func (t *PyTask) getData() []byte {
	return t.data
}
