package internal

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	roomserverAPI "codefloe.com/pat-s/zendrite/roomserver/api"
	"codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/setup/process"
)

func TestIdlePartialStateWorkerStops(t *testing.T) {
	p := process.NewProcessContextWithParent(t.Context())
	w := &PartialStateWorker{process: p, workerCh: make(chan types.RoomNID)}
	p.Go(func() { w.worker(0) })
	p.ShutdownZendrite()
	done := make(chan struct{})
	go func() { p.WaitForComponentsToFinish(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("idle partial-state worker ignored cancellation")
	}
}

type partialStateStartupAPI struct {
	roomserverAPI.FederationRoomserverAPI
}

func (partialStateStartupAPI) GetAllPartialStateRooms(context.Context) ([]types.RoomNID, error) {
	return []types.RoomNID{1, 2, 3}, nil
}

func TestPartialStateStartupStaggerStops(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := process.NewProcessContextWithParent(t.Context())
		w := NewPartialStateWorker(p, partialStateStartupAPI{}, nil)
		if err := w.Start(); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		p.ShutdownZendrite()
		p.WaitForComponentsToFinish()
		time.Sleep(2 * time.Minute)
		synctest.Wait()
		if len(w.workerCh) != 0 || len(w.retryMap) != 0 {
			t.Fatal("startup stagger queued work after shutdown")
		}
	})
}
