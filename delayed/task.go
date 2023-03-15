package delayed

import (
	"bytes"
	"reflect"

	"github.com/keakon/golog/log"
	"github.com/shamaton/msgpack/v2"
)

type RawGoTask struct {
	ID       uint64
	FuncPath string
	Payload  []byte // serialized arg
	Timeout  uint32 // in ms
	Prior    bool
}

type GoTask struct {
	raw  RawGoTask
	arg  interface{}
	data []byte // serialized data
}

const (
	GoTaskExportFieldsMinCount = 2
	GoTaskExportFieldsCount    = 5
)

func NewTask(id uint64, funcPath string, arg interface{}, timeout uint32, prior bool) *GoTask {
	return &GoTask{
		raw: RawGoTask{
			ID:       id,
			FuncPath: funcPath,
			Timeout:  timeout,
			Prior:    prior,
		},
		arg: arg,
	}
}

func NewTaskOfFunc(id uint64, f, arg interface{}, timeout uint32, prior bool) *GoTask {
	_, _, funcPath := parseFunc(f)
	if funcPath == "" {
		return nil
	}

	return &GoTask{
		raw: RawGoTask{
			ID:       id,
			FuncPath: funcPath,
			Timeout:  timeout,
			Prior:    prior,
		},
		arg: arg,
	}
}

func (t *GoTask) Equal(task *GoTask) bool {
	// it may return false if one task is not serialized and the other is deserialized
	return t.raw.ID == task.raw.ID && t.raw.FuncPath == task.raw.FuncPath && (bytes.Equal(t.raw.Payload, task.raw.Payload) || reflect.DeepEqual(t.arg, task.arg)) && t.raw.Timeout == task.raw.Timeout && t.raw.Prior == task.raw.Prior
}

func (t *GoTask) Serialize() (err error) {
	if t.arg != nil {
		t.raw.Payload, err = msgpack.Marshal(t.arg)
		if err != nil {
			log.Errorf("serialize task.arg error: %v", err)
			return
		}
	}

	raw := t.raw
	fields := []interface{}{raw.ID, raw.FuncPath, raw.Payload, raw.Timeout, raw.Prior} // TODO: use sync.Pool

	if !raw.Prior { // removes default fields, makes it smaller than msgpack.MarshalAsArray(&raw)
		size := GoTaskExportFieldsCount - 1
		if raw.Timeout == 0 {
			size--
			if t.arg == nil {
				size--
			}
		}
		fields = fields[:size]
	}

	t.data, err = msgpack.Marshal(fields)
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

func (t *GoTask) Run(handlers map[string]Handler) {
	h, ok := handlers[t.raw.FuncPath]
	if ok {
		h.Call(t.raw.Payload)
	}
}
