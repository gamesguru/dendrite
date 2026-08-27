package test_test

import (
	"sync"
	"testing"

	"codefloe.com/pat-s/zendrite/test"
)

// TestRoomEventsAreSafeForParallelSubtests mirrors what WithAllDatabases does:
// a room is built once and its events are then read from several goroutines at
// the same time. Under -race this fails if the events still carry a lazily
// generated event ID, because the first EventID() call writes to the event.
func TestRoomEventsAreSafeForParallelSubtests(t *testing.T) {
	alice := test.NewUser(t)
	room := test.NewRoom(t, alice)
	extra := room.CreateEvent(t, alice, "m.room.name", map[string]string{"name": "testing"}, test.WithStateKey(""))

	start := make(chan struct{})
	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for _, ev := range room.Events() {
				_ = ev.EventID()
			}
			_ = extra.EventID()
		}()
	}
	close(start)
	wg.Wait()
}
