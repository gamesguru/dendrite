package internal

import (
	"testing"
	"time"

	"codefloe.com/pat-s/zendrite/setup/process"
)

func TestIdleMetadataWorkerStops(t *testing.T) {
	p := process.NewProcessContextWithParent(t.Context())
	w := &SlidingSyncMetadataWorker{process: p, workerCh: make(chan string)}
	p.Go(func() { w.worker(p.Context(), 0) })
	p.ShutdownZendrite()
	done := make(chan struct{})
	go func() { p.WaitForComponentsToFinish(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("idle metadata worker ignored cancellation")
	}
}
