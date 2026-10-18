//go:build linux

package hachaos

import (
	"context"
	"sync"
	"time"
)

// observer samples every candidate's status endpoint and records phase
// transitions. It also records any moment at which two candidates report
// readiness together, which the router would treat as two active backends.
type observer struct {
	slots    []*slot
	timeline *timeline
	stopped  chan struct{}
	done     chan struct{}

	mu       sync.Mutex
	last     map[string]string
	overlaps []time.Time
	activeAt map[string][]time.Time
}

const observeInterval = 250 * time.Millisecond

func startObserver(ctx context.Context, slots []*slot, tl *timeline) *observer {
	o := &observer{slots: slots, timeline: tl, stopped: make(chan struct{}), done: make(chan struct{}), last: map[string]string{}, activeAt: map[string][]time.Time{}}
	go o.run(context.WithoutCancel(ctx))
	return o
}

func (o *observer) run(ctx context.Context) {
	defer close(o.done)
	ticker := time.NewTicker(observeInterval)
	defer ticker.Stop()
	for {
		select {
		case <-o.stopped:
			return
		case <-ticker.C:
		}
		ready := 0
		var wg sync.WaitGroup
		results := make([]string, len(o.slots))
		readies := make([]bool, len(o.slots))
		for i, sl := range o.slots {
			wg.Add(1)
			go func() {
				defer wg.Done()
				ctx, cancel := context.WithTimeout(ctx, observeInterval)
				defer cancel()
				st, err := sl.status(ctx)
				switch {
				case err != nil:
					results[i] = "unreachable"
				case st.Boundary != "":
					results[i] = st.Phase + " blocked at " + st.Boundary
				default:
					results[i] = st.Phase
				}
				readies[i] = err == nil && st.Ready
			}()
		}
		wg.Wait()
		now := time.Now()
		o.mu.Lock()
		for i, sl := range o.slots {
			if readies[i] {
				ready++
			}
			state := results[i]
			if readies[i] {
				state += " (ready)"
			}
			if o.last[sl.name] != state {
				o.last[sl.name] = state
				o.timeline.add(eventPhase, sl.name, "%s", state)
				if readies[i] {
					o.activeAt[sl.name] = append(o.activeAt[sl.name], now)
				}
			}
		}
		if ready > 1 {
			o.overlaps = append(o.overlaps, now)
		}
		o.mu.Unlock()
	}
}

func (o *observer) readyOverlaps() []time.Time {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]time.Time(nil), o.overlaps...)
}

func (o *observer) stop() {
	select {
	case <-o.stopped:
	default:
		close(o.stopped)
	}
	<-o.done
}
