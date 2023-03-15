package delayed

import "testing"

func TestWorkerRegisterHandlers(t *testing.T) {
	w := NewWorker(nil)
	w.RegisterHandlers(f1, f2, f3)
	if len(w.handlers) != 3 {
		t.FailNow()
	}
	w.RegisterHandlers(f4)
	if len(w.handlers) != 4 {
		t.FailNow()
	}
}
