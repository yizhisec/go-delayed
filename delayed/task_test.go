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
	task1 := NewTask(1, "test", nil)
	err := task1.Serialize()
	if err != nil {
		t.Fatal(err)
	}

	task2 := NewTask(2, "test", tArg)
	err = task2.Serialize()
	if err != nil {
		t.Fatal(err)
	}

	if len(task1.data) >= len(task2.data) {
		t.FailNow()
	}

	task3 := NewTask(3, "test", &tArg)
	err = task3.Serialize()
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
			task1 := NewTask(tt.id, tt.funcPath, tt.arg)
			task2 := NewTask(tt.id, tt.funcPath, tt.arg)

			err := task1.Serialize()
			if err != nil {
				t.Fatal(err)
			}

			if !task1.Equal(task2) {
				t.FailNow()
			}

			task3, err := DeserializeGoTask(task1.data)
			if err != nil {
				t.Fatal(err)
			}

			if !task1.Equal(task3) {
				t.FailNow()
			}
		})
	}
}
