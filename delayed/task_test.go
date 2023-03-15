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
		timeout  uint32
		prior    bool
	}{
		{
			name:     "nil arg",
			id:       1,
			funcPath: "test",
			arg:      nil,
			timeout:  0,
			prior:    false,
		},
		{
			name:     "struct arg",
			id:       2,
			funcPath: "test",
			arg:      tArg,
			timeout:  0,
			prior:    false,
		},
		{
			name:     "*struct arg",
			id:       3,
			funcPath: "test",
			arg:      &tArg,
			timeout:  0,
			prior:    false,
		},
		{
			name:     "int arg",
			id:       4,
			funcPath: "test",
			arg:      1,
			timeout:  0,
			prior:    false,
		},
		{
			name:     "with timeout",
			id:       5,
			funcPath: "test",
			arg:      nil,
			timeout:  1,
			prior:    false,
		},
		{
			name:     "with prior",
			id:       6,
			funcPath: "test",
			arg:      nil,
			timeout:  0,
			prior:    true,
		},
		{
			name:     "with all",
			id:       7,
			funcPath: "test",
			arg:      tArg,
			timeout:  1,
			prior:    true,
		},
	}
)

func TestGoTaskSerialize(t *testing.T) {
	task1 := NewTask(1, "test", nil, 0, false)
	err := task1.Serialize()
	if err != nil {
		t.Fatal(err)
	}

	task2 := NewTask(2, "test", tArg, 0, false)
	err = task2.Serialize()
	if err != nil {
		t.Fatal(err)
	}

	if len(task1.data) >= len(task2.data) {
		t.FailNow()
	}

	task3 := NewTask(3, "test", tArg, 10, false)
	err = task3.Serialize()
	if err != nil {
		t.Fatal(err)
	}

	if len(task2.data) >= len(task3.data) {
		t.FailNow()
	}

	task4 := NewTask(4, "test", nil, 10, false)
	err = task4.Serialize()
	if err != nil {
		t.Fatal(err)
	}

	if len(task1.data) >= len(task4.data) {
		t.FailNow()
	}

	if len(task2.data) <= len(task4.data) {
		t.FailNow()
	}

	if len(task3.data) <= len(task4.data) {
		t.FailNow()
	}

	task5 := NewTask(5, "test", &tArg, 10, true)
	err = task5.Serialize()
	if err != nil {
		t.Fatal(err)
	}

	if len(task3.data) == len(task5.data) {
		t.FailNow()
	}

	task6 := NewTask(6, "test", tArg, 10, true)
	err = task6.Serialize()
	if err != nil {
		t.Fatal(err)
	}

	if len(task3.data) >= len(task6.data) {
		t.FailNow()
	}
}

func TestDeserializeGoTask(t *testing.T) {
	for _, tt := range taskTestCases {
		t.Run(tt.name, func(t *testing.T) {
			task1 := NewTask(tt.id, tt.funcPath, tt.arg, tt.timeout, tt.prior)
			task2 := NewTask(tt.id, tt.funcPath, tt.arg, tt.timeout, tt.prior)

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
