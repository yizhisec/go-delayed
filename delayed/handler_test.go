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

func f5() int {
	return 0
}

func f6(a, b int) int {
	return a + b
}

func f7(a, b testArg) int {
	return a.A + len(a.B) + b.A + len(b.B)
}

func f8(a, b *testArg) int {
	return a.A + len(a.B) + b.A + len(b.B)
}

func f9(a []int, b testArg) int {
	sum := 0
	for _, i := range a {
		sum += i
	}
	return sum + b.A + len(b.B)
}

func f10(a [3]int, b []int) int {
	sum := 0
	for _, i := range a {
		sum += i
	}
	for _, i := range b {
		sum += i
	}
	return sum
}

func f11(a []int) int {
	sum := 0
	for _, i := range a {
		sum += i
	}
	return sum
}

func f12(a ...int) int {
	sum := 0
	for _, i := range a {
		sum += i
	}
	return sum
}

func f13(a []int, b ...int) int {
	sum := 0
	for _, i := range a {
		sum += i
	}
	for _, i := range b {
		sum += i
	}
	return sum
}

func f14(a testArg, b []int, c ...int) int {
	sum := a.A + len(a.B)
	for _, i := range b {
		sum += i
	}
	for _, i := range c {
		sum += i
	}
	return sum
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
				fn:       reflect.ValueOf(f1),
				argCount: 1,
				path:     "github.com/yizhisec/go-delayed/delayed.f1",
			},
		},
		{
			name: "*struct arg",
			f:    f2,
			want: &Handler{
				fn:       reflect.ValueOf(f2),
				argCount: 1,
				path:     "github.com/yizhisec/go-delayed/delayed.f2",
			},
		},
		{
			name: "int arg",
			f:    f3,
			want: &Handler{
				fn:       reflect.ValueOf(f3),
				argCount: 1,
				path:     "github.com/yizhisec/go-delayed/delayed.f3",
			},
		},
		{
			name: "*int arg",
			f:    f4,
			want: &Handler{
				fn:       reflect.ValueOf(f4),
				argCount: 1,
				path:     "github.com/yizhisec/go-delayed/delayed.f4",
			},
		},
		{
			name: "no arg",
			f:    f5,
			want: &Handler{
				fn:       reflect.ValueOf(f5),
				argCount: 0,
				path:     "github.com/yizhisec/go-delayed/delayed.f5",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NewHandler(tt.f)
			if got.fn != tt.want.fn {
				t.Errorf("NewHandler().fn = %v, want %v", got.fn, tt.want.fn)
			}
			if got.argCount != tt.want.argCount {
				t.Errorf("NewHandler().argCount = %v, want %v", got.argCount, tt.want.argCount)
			}
			if got.path != tt.want.path {
				t.Errorf("NewHandler().path = %v, want %v", got.path, tt.want.path)
			}
		})
	}

	tests2 := []struct {
		name         string
		f            interface{}
		wantFn       reflect.Value
		wantArgCount int
		wantPath     string
	}{
		{
			name:         "int args",
			f:            f6,
			wantFn:       reflect.ValueOf(f6),
			wantArgCount: 2,
			wantPath:     "github.com/yizhisec/go-delayed/delayed.f6",
		},
		{
			name:         "struct args",
			f:            f7,
			wantFn:       reflect.ValueOf(f7),
			wantArgCount: 2,
			wantPath:     "github.com/yizhisec/go-delayed/delayed.f7",
		},
		{
			name:         "*struct args",
			f:            f8,
			wantFn:       reflect.ValueOf(f8),
			wantArgCount: 2,
			wantPath:     "github.com/yizhisec/go-delayed/delayed.f8",
		},
		{
			name:         "[]int + struct args",
			f:            f9,
			wantFn:       reflect.ValueOf(f9),
			wantArgCount: 2,
			wantPath:     "github.com/yizhisec/go-delayed/delayed.f9",
		},
		{
			name:         "[3]int + []int args",
			f:            f10,
			wantFn:       reflect.ValueOf(f10),
			wantArgCount: 2,
			wantPath:     "github.com/yizhisec/go-delayed/delayed.f10",
		},
		{
			name:         "[]int arg",
			f:            f11,
			wantFn:       reflect.ValueOf(f11),
			wantArgCount: 1,
			wantPath:     "github.com/yizhisec/go-delayed/delayed.f11",
		},
		{
			name:         "...int arg",
			f:            f12,
			wantFn:       reflect.ValueOf(f12),
			wantArgCount: 1,
			wantPath:     "github.com/yizhisec/go-delayed/delayed.f12",
		},
		{
			name:         "[]int + ...int args",
			f:            f13,
			wantFn:       reflect.ValueOf(f13),
			wantArgCount: 2,
			wantPath:     "github.com/yizhisec/go-delayed/delayed.f13",
		},
		{
			name:         "struct + []int + ...int args",
			f:            f14,
			wantFn:       reflect.ValueOf(f14),
			wantArgCount: 3,
			wantPath:     "github.com/yizhisec/go-delayed/delayed.f14",
		},
	}

	for _, tt := range tests2 {
		t.Run(tt.name, func(t *testing.T) {
			got := NewHandler(tt.f)
			if got == nil {
				t.Fatal("NewHandler() is nil")
			}
			if got.fn != tt.wantFn {
				t.Errorf("NewHandler().fn = %v, want %v", got.fn, tt.wantFn)
			}
			if got.argCount != tt.wantArgCount {
				t.Errorf("NewHandler().argCount = %v, want %v", got.fn, tt.wantArgCount)
			}
			if got.path != tt.wantPath {
				t.Errorf("NewHandler().path = %v, want %v", got.fn, tt.wantPath)
			}
		})
	}
}

func TestHandlerCall(t *testing.T) {
	tests := []struct {
		name string
		f    interface{}
		args interface{}
		want int
	}{
		{
			name: "struct arg",
			f:    f1,
			args: tArg,
			want: 5,
		},
		{
			name: "struct as array arg",
			f:    f1,
			args: []interface{}{1, "test"},
			want: 5,
		},
		{
			name: "nil struct arg",
			f:    f1,
			args: nil,
			want: 0,
		},
		{
			name: "*struct arg",
			f:    f2,
			args: &tArg,
			want: 5,
		},
		{
			name: "nil *struct arg",
			f:    f2,
			args: nil,
			want: 0,
		},
		{
			name: "int arg",
			f:    f3,
			args: 1,
			want: 1,
		},
		{
			name: "*int arg",
			f:    f4,
			args: new(int),
			want: 0,
		},
		{
			name: "no arg",
			f:    f5,
			args: nil,
			want: 0,
		},
		{
			name: "int args",
			f:    f6,
			args: []int{2, 3},
			want: 5,
		},
		{
			name: "struct args",
			f:    f7,
			args: []testArg{tArg, tArg},
			want: 10,
		},
		{
			name: "*struct args",
			f:    f8,
			args: []*testArg{&tArg, &tArg},
			want: 10,
		},
		{
			name: "[]int + struct args",
			f:    f9,
			args: []interface{}{[]int{1, 2, 3}, tArg},
			want: 11,
		},
		{
			name: "[3]int + []int args",
			f:    f10,
			args: []interface{}{[3]int{1, 2, 3}, []int{4, 5}},
			want: 15,
		},
		{
			name: "[]int arg",
			f:    f11,
			args: []int{1, 2, 3},
			want: 6,
		},
		{
			name: "...int arg",
			f:    f12,
			args: []int{1, 2, 3},
			want: 6,
		},
		{
			name: "[]int + ...int arg",
			f:    f13,
			args: []interface{}{[]int{1, 2, 3}, []int{4, 5, 6}},
			want: 21,
		},
		{
			name: "struct + []int + ...int arg",
			f:    f14,
			args: []interface{}{tArg, []int{1, 2, 3}, []int{4, 5, 6}},
			want: 26,
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

			if tt.args != nil {
				p, err = msgpack.MarshalAsArray(tt.args)
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
