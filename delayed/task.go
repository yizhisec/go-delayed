package delayed

import (
	"bytes"
	"reflect"
	"runtime"

	"github.com/keakon/golog/log"
	"github.com/shamaton/msgpack/v2"
)

type RawGoTask struct {
	ID       uint64
	FuncPath string
	Payload  []byte // serialized arg
}

type GoTask struct {
	raw  RawGoTask
	arg  interface{}
	data []byte // serialized data
}

var goTaskExportFieldsCount = reflect.TypeOf(RawGoTask{}).NumField()

func NewTask(id uint64, funcPath string, arg interface{}) *GoTask {
	return &GoTask{
		raw: RawGoTask{
			ID:       id,
			FuncPath: funcPath,
		},
		arg: arg,
	}
}

func NewTaskOfFunc(id uint64, f, arg interface{}) *GoTask {
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
			ID:       id,
			FuncPath: funcPath,
		},
		arg: arg,
	}
}

func (t *GoTask) Equal(task *GoTask) bool {
	// it may return false if one task is not serialized and the other is deserialized
	return t.raw.ID == task.raw.ID && t.raw.FuncPath == task.raw.FuncPath && (bytes.Equal(t.raw.Payload, task.raw.Payload) || reflect.DeepEqual(t.arg, task.arg))
}

func (t *GoTask) Serialize() (err error) {
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
	return
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
