package delayed

import (
	"reflect"
	"testing"

	"github.com/shamaton/msgpack/v2"
)

type testArg struct {
	A int
	B string
}

func f1(a testArg) int {
	return a.A + len(a.B)
}

func f2(a *testArg) int {
	if a == nil {
		return 0
	}
	return a.A + len(a.B)
}

func f3(a int) int {
	return a
}

func f4(a *int) int {
	return *a
}

func TestNewHandler(t *testing.T) {
	tests := []struct {
		name string
		f    interface{}
		want *Handler
	}{
		{
			name: "struct arg",
			f:    f1,
			want: &Handler{
				fn:      reflect.ValueOf(f1),
				argType: reflect.TypeOf(testArg{}),
				path:    "github.com/yizhisec/go-delayed/delayed.f1",
			},
		},
		{
			name: "*struct arg",
			f:    f2,
			want: &Handler{
				fn:      reflect.ValueOf(f2),
				argType: reflect.TypeOf(&testArg{}),
				path:    "github.com/yizhisec/go-delayed/delayed.f2",
			},
		},
		{
			name: "int arg",
			f:    f3,
			want: &Handler{
				fn:      reflect.ValueOf(f3),
				argType: reflect.TypeOf(0),
				path:    "github.com/yizhisec/go-delayed/delayed.f3",
			},
		},
		{
			name: "*int arg",
			f:    f4,
			want: &Handler{
				fn:      reflect.ValueOf(f4),
				argType: reflect.TypeOf(new(int)),
				path:    "github.com/yizhisec/go-delayed/delayed.f4",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewHandler(tt.f); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewHandler() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHandlerCall(t *testing.T) {
	tests := []struct {
		name string
		f    interface{}
		arg  interface{}
		want int
	}{
		{
			name: "struct arg",
			f:    f1,
			arg:  testArg{A: 1, B: "test"},
			want: 5,
		},
		{
			name: "nil struct arg",
			f:    f1,
			arg:  nil,
			want: 0,
		},
		{
			name: "*struct arg",
			f:    f2,
			arg:  &testArg{A: 1, B: "test"},
			want: 5,
		},
		{
			name: "nil *struct arg",
			f:    f2,
			arg:  nil,
			want: 0,
		},
		{
			name: "int arg",
			f:    f3,
			arg:  1,
			want: 1,
		},
		{
			name: "*int arg",
			f:    f4,
			arg:  new(int),
			want: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var (
				h   = NewHandler(tt.f)
				r   []reflect.Value
				p   []byte
				err error
			)

			if tt.arg != nil {
				p, err = msgpack.Marshal(tt.arg)
				if err != nil {
					t.Fatal(err)
				}
			}

			r, err = h.Call(p)
			if err != nil {
				t.Fatal(err)
			}

			got := r[0].Interface().(int)
			if got != tt.want {
				t.Errorf("Handler.Call() = %d, want %d", got, tt.want)
			}
		})
	}
}
