package input

import (
	"testing"

	"codefloe.com/pat-s/zendrite/setup/process"
)

func TestWorkerRejectedAfterShutdown(t *testing.T) {
	p := process.NewProcessContextWithParent(t.Context())
	p.ShutdownZendrite()
	w := &worker{r: &Inputer{ProcessContext: p}}
	w.Lock()
	w.start()
	w.Unlock()
	if w.ctx != nil || w.cancel != nil {
		t.Fatal("rejected worker is marked running")
	}
	p.WaitForComponentsToFinish()
}
