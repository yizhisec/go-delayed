package delayed

import (
	"testing"
)

var (
	tArg = testArg{A: 1, B: "test"}

	taskTestCases = []struct {
		name     string
		id       uint64
		funcPath string
		arg      interface{}
	}{
		{
			name:     "nil arg",
			id:       1,
			funcPath: "test",
			arg:      nil,
		},
		{
			name:     "struct arg",
			id:       2,
			funcPath: "test",
			arg:      tArg,
		},
		{
			name:     "*struct arg",
			id:       3,
			funcPath: "test",
			arg:      &tArg,
		},
		{
			name:     "int arg",
			id:       4,
			funcPath: "test",
			arg:      1,
		},
	}
)

func TestGoTaskSerialize(t *testing.T) {
	task1 := NewGoTask("test", nil)
	task1.setID(1)
	_, err := task1.Serialize()
	if err != nil {
		t.Fatal(err)
	}

	task2 := NewGoTask("test", tArg)
	task2.setID(2)
	_, err = task2.Serialize()
	if err != nil {
		t.Fatal(err)
	}

	if len(task1.data) >= len(task2.data) {
		t.FailNow()
	}

	task3 := NewGoTask("test", &tArg)
	task3.setID(3)
	_, err = task3.Serialize()
	if err != nil {
		t.Fatal(err)
	}

	if len(task2.data) != len(task3.data) {
		t.FailNow()
	}
}

func TestDeserializeGoTask(t *testing.T) {
	for _, tt := range taskTestCases {
		t.Run(tt.name, func(t *testing.T) {
			task1 := NewGoTask(tt.funcPath, tt.arg)
			task1.setID(tt.id)
			task2 := NewGoTask(tt.funcPath, tt.arg)
			task2.setID(tt.id)

			data, err := task1.Serialize()
			if err != nil {
				t.Fatal(err)
			}

			if !task1.Equal(task2) {
				t.FailNow()
			}

			task3, err := DeserializeGoTask(data)
			if err != nil {
				t.Fatal(err)
			}

			if !task1.Equal(task3) {
				t.FailNow()
			}
		})
	}
}

func TestPyTaskSerialize(t *testing.T) {
	tests := []struct {
		name     string
		funcPath string
		args     interface{}
		kwArgs   interface{}
	}{
		{
			name:     "int arg",
			funcPath: "test",
			args:     []int{1},
			kwArgs:   nil,
		},
		{
			name:     "no arg",
			funcPath: "test",
			args:     nil,
			kwArgs:   nil,
		},
		{
			name:     "int + string args",
			funcPath: "test",
			args:     []interface{}{1, "2"},
			kwArgs:   nil,
		},
		{
			name:     "map kwargs",
			funcPath: "test",
			args:     nil,
			kwArgs:   map[string]string{"foo": "bar"},
		},
		{
			name:     "struct kwargs",
			funcPath: "test",
			args:     nil,
			kwArgs:   testArg{A: 1, B: "test"},
		},
		{
			name:     "args + kwargs",
			funcPath: "test",
			args:     []interface{}{1, "2"},
			kwArgs:   testArg{A: 1, B: "test"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if task := NewPyTask(tt.funcPath, tt.args, tt.kwArgs); task == nil {
				t.Errorf("NewPyTask() is nil")
			} else if _, err := task.Serialize(); err != nil {
				t.Errorf("task.Serialize() failed: %v", err)
			}
		})
	}
}
