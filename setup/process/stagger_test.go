package process

import (
	"testing"
	"testing/synctest"
	"time"
)

func TestStagger(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := NewProcessContextWithParent(t.Context())
		defer p.ShutdownZendrite()
		start := time.Now()
		var got []time.Duration
		Stagger(p.Context(), []int{0, 1, 2}, 10*time.Second, time.Second, func(i int) {
			if i != len(got) {
				t.Errorf("dispatch order: got %d, want %d", i, len(got))
			}
			got = append(got, time.Since(start))
		})
		time.Sleep(13 * time.Second)
		synctest.Wait()
		if len(got) != 3 {
			t.Fatalf("dispatches: %v", got)
		}
		for i, delay := range got {
			if want := time.Duration(10+i) * time.Second; delay != want {
				t.Errorf("dispatch %d at %v, want %v", i, delay, want)
			}
		}
	})
}

func TestStaggerCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := NewProcessContextWithParent(t.Context())
		calls := 0
		Stagger(p.Context(), make([]int, 10_000), time.Second, time.Second, func(int) { calls++ })
		time.Sleep(1500 * time.Millisecond)
		synctest.Wait()
		p.ShutdownZendrite()
		p.WaitForComponentsToFinish()
		time.Sleep(time.Hour)
		if calls != 1 {
			t.Fatalf("dispatches after cancellation: %d", calls)
		}
	})
}
