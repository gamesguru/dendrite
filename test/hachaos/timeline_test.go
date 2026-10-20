//go:build linux

package hachaos

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"
)

// timeline records what the harness did and observed, in order, so a failed
// run can be read back without rerunning it.
type timeline struct {
	mu     sync.Mutex
	events []event
	file   *os.File
}

type event struct {
	At      time.Time `json:"at"`
	Kind    string    `json:"kind"`
	Subject string    `json:"subject,omitempty"`
	Detail  string    `json:"detail,omitempty"`
}

const (
	eventFault  = "fault"
	eventPhase  = "phase"
	eventRouter = "router"
	eventNote   = "note"
	eventProc   = "process"
)

func newTimeline(path string) (*timeline, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	return &timeline{file: f}, nil
}

func (t *timeline) add(kind, subject, format string, args ...any) event {
	e := event{At: time.Now(), Kind: kind, Subject: subject, Detail: fmt.Sprintf(format, args...)}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.events = append(t.events, e)
	if t.file != nil {
		if line, err := json.Marshal(e); err == nil {
			_, _ = t.file.Write(append(line, '\n'))
		}
	}
	return e
}

func (t *timeline) close() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.file != nil {
		_ = t.file.Close()
		t.file = nil
	}
}
