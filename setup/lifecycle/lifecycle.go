// Package lifecycle controls one homeserver activation per operating-system process.
package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	"github.com/sirupsen/logrus"

	"codefloe.com/pat-s/zendrite/setup/process"
)

// State describes application activation, independently of election ownership.
type State string

const (
	Standby    State = "standby"
	Recovering State = "recovering"
	Active     State = "active"
	// HandingOver still serves, but has asked routers to send new requests
	// elsewhere and is waiting for the ones in flight.
	HandingOver State = "handing-over"
	Draining    State = "draining"
	Stopped     State = "stopped"
)

// ErrAlreadyStarted prevents reuse of stopped application singletons.
var ErrAlreadyStarted = errors.New("homeserver activation already attempted")

// ErrShutdownTimeout requires the caller to terminate the operating-system process.
var ErrShutdownTimeout = errors.New("homeserver shutdown deadline exceeded; process must exit")

// Homeserver defers all application effects until Run is explicitly authorized.
// It does not perform election or fencing: its caller must establish safe ownership.
type Homeserver struct {
	mu              sync.RWMutex
	state           State
	started         bool
	process         *process.ProcessContext
	initialize      func(*process.ProcessContext) error
	shutdownTimeout time.Duration
}

// New constructs an inert homeserver. The initializer must register workers and cleanup
// with the supplied ProcessContext and return only after recovery is complete.
func New(initialize func(*process.ProcessContext) error, shutdownTimeout time.Duration) *Homeserver {
	return &Homeserver{state: Standby, initialize: initialize, shutdownTimeout: shutdownTimeout}
}

// State reports recovery separately from readiness and observes cancellation immediately.
func (h *Homeserver) State() State {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if h.process != nil && h.process.Context().Err() != nil && h.state != Stopped {
		return Draining
	}
	return h.state
}

// Ready reports whether initialization completed and ownership has not been
// canceled. A homeserver that is handing over is still ready: it admits the
// requests a router sent before it noticed the handover.
func (h *Homeserver) Ready() bool {
	state := h.State()
	return state == Active || state == HandingOver
}

// handoverPoll is how often a handover checks for requests still in flight.
const handoverPoll = 50 * time.Millisecond

// HandOver lets an active homeserver stop without failing requests.
//
// It announces the handover, which routers see as a failed active-only check
// that must not cut established requests, and wakes long-polling requests so
// that they answer now. It keeps admitting requests for grace, so a request a
// router sent before its next health check still succeeds, and then waits for
// every request in flight to finish, for at most timeout in total.
//
// It does not stop the homeserver or release ownership: the caller stops it
// afterwards, as for any shutdown. A homeserver that is not active, or that
// loses ownership meanwhile, returns at once.
func (h *Homeserver) HandOver(grace, timeout time.Duration) {
	h.mu.Lock()
	p := h.process
	if h.state != Active || p == nil || p.Context().Err() != nil {
		h.mu.Unlock()
		return
	}
	h.state = HandingOver
	h.mu.Unlock()
	started := time.Now()
	logrus.WithFields(logrus.Fields{"grace": grace, "timeout": timeout}).Info("Handing over: routers are told to send new requests elsewhere")
	p.Quiesce()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	if !process.Sleep(p.Context(), grace) {
		return
	}
	ticker := time.NewTicker(handoverPoll)
	defer ticker.Stop()
	for p.InFlight() > 0 {
		select {
		case <-p.Context().Done():
			return
		case <-deadline.C:
			logrus.WithFields(logrus.Fields{"duration": time.Since(started).Round(time.Millisecond), "in_flight": p.InFlight()}).Warn("Handover timed out; requests still in flight will be cut")
			return
		case <-ticker.C:
		}
	}
	logrus.WithField("duration", time.Since(started).Round(time.Millisecond)).Info("Handover drained every request in flight")
}

// Stop revokes local admission and cancels application work. Run joins the workers.
// Stop is terminal, including before Run; it is not a fencing mechanism.
func (h *Homeserver) Stop() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.started = true
	if h.process == nil {
		h.state = Stopped
		return
	}
	h.process.ShutdownZendrite()
	if h.state != Stopped {
		h.state = Draining
	}
}

// Run activates once after the caller authorizes ownership and waits for shutdown.
// The ownership context must be canceled on ownership loss, including during startup.
// ErrShutdownTimeout requires immediate process exit, never standby or reacquisition.
func (h *Homeserver) Run(ownership context.Context) error {
	h.mu.Lock()
	if h.started {
		h.mu.Unlock()
		return ErrAlreadyStarted
	}
	h.started = true
	if h.shutdownTimeout <= 0 || h.initialize == nil {
		h.state = Stopped
		h.mu.Unlock()
		return errors.New("initializer and positive shutdown timeout are required")
	}
	p := process.NewProcessContextWithParent(ownership)
	h.process = p
	h.state = Recovering
	h.mu.Unlock()

	initialized := make(chan error, 1)
	p.Go(func() {
		var err error
		defer func() {
			if failure := recover(); failure != nil {
				switch value := failure.(type) {
				case error:
					err = fmt.Errorf("startup panic: %w", value)
				case *logrus.Entry:
					if cause, ok := value.Data[logrus.ErrorKey].(error); ok {
						err = fmt.Errorf("startup panic: %s: %w", value.Message, cause)
					} else {
						err = fmt.Errorf("startup panic: %s", value.Message)
					}
				default:
					err = fmt.Errorf("startup panic: %v", failure)
				}
				err = fmt.Errorf("%w\n%s", err, debug.Stack())
			}
			// Cancellation during recovery is an ordinary stop, after cleanup joins.
			if p.Context().Err() != nil && errors.Is(err, p.Context().Err()) {
				err = nil
			}
			initialized <- err
		}()
		err = h.initialize(p)
	})
	var result error
	select {
	case result = <-initialized:
		if result == nil {
			h.mu.Lock()
			if p.Context().Err() == nil {
				h.state = Active
			}
			h.mu.Unlock()
			<-p.WaitForShutdown()
		}
	case <-p.WaitForShutdown():
	}
	h.Stop()
	finished := make(chan struct{})
	go func() { p.WaitForComponentsToFinish(); close(finished) }()
	timer := time.NewTimer(h.shutdownTimeout)
	defer timer.Stop()
	select {
	case <-finished:
		select {
		case startupErr := <-initialized:
			result = errors.Join(result, startupErr)
		default:
		}
		h.mu.Lock()
		h.state = Stopped
		h.mu.Unlock()
		if result != nil {
			return fmt.Errorf("initialize homeserver: %w", result)
		}
		return nil
	case <-timer.C:
		return errors.Join(result, ErrShutdownTimeout)
	}
}
