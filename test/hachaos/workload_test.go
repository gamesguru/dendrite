//go:build linux

package hachaos

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"
)

// workload is the traffic a scenario runs through. It behaves like a careful
// client: a failed write is retried with the same transaction ID until it is
// acknowledged, a failed sync is retried with the same since token, and every
// outcome is recorded so the verifier can compare what clients were told with
// what the servers stored.
type workload struct {
	e    *environment
	stop context.CancelCauseFunc
	ctx  context.Context
	wg   sync.WaitGroup

	alice  *mxClient
	bob    *mxClient
	remote *mxClient
	room   string

	mu           sync.Mutex
	sends        []*sendRecord
	remoteSends  []*sendRecord
	toDevice     []*sendRecord
	receipts     []*sendRecord
	attempts     []attempt
	bobSeen      map[string]time.Time
	remoteSeen   map[string]time.Time
	toDeviceSeen map[string]time.Time
	syncFailures []string
	lastBobEvent string
	serial       int
	// since is each syncing client's last token once its syncer stops.
	since map[string]string
	// gaps are limited timelines still to be filled from prev_batch.
	gaps     []gap
	gapFills int
}

// gap is a limited sync timeline, which a client fills by paging back from
// prev_batch until it reaches events it has already seen.
type gap struct {
	client *mxClient
	from   string
	seen   map[string]time.Time
}

// sendRecord is one logical client operation and how it ended.
type sendRecord struct {
	Txn      string    `json:"txn"`
	Nonce    string    `json:"nonce"`
	First    time.Time `json:"first_attempt"`
	Acked    bool      `json:"acked"`
	AckedAt  time.Time `json:"acked_at,omitzero"`
	EventID  string    `json:"event_id,omitempty"`
	Attempts int       `json:"attempts"`
	LastErr  string    `json:"last_error,omitempty"`
}

// attempt is one HTTP request made on behalf of a logical operation.
type attempt struct {
	Kind  string
	Start time.Time
	End   time.Time
	OK    bool
}

const (
	kindWrite       = "client-write"
	kindRemoteWrite = "remote-write"
	kindSync        = "sync"
)

func nonce() string {
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// startWorkload creates the users and room, joins a remote user over
// federation, and starts writers and syncers for both servers.
func startWorkload(ctx context.Context, e *environment) (*workload, error) {
	w := &workload{e: e, bobSeen: map[string]time.Time{}, remoteSeen: map[string]time.Time{}, toDeviceSeen: map[string]time.Time{}, since: map[string]string{}}
	var err error
	if w.alice, err = registerRetrying(ctx, e.router.url(), "alice"); err != nil {
		return nil, err
	}
	if w.bob, err = registerRetrying(ctx, e.router.url(), "bob"); err != nil {
		return nil, err
	}
	if w.remote, err = registerRetrying(ctx, e.remote.httpURL(), "rita"); err != nil {
		return nil, err
	}
	// A retried createRoom may create a second room; the workload simply
	// uses whichever attempt succeeded.
	if err = retrying(ctx, func() error {
		var err error
		w.room, err = w.alice.createRoom(ctx, "hachaos")
		return err
	}); err != nil {
		return nil, err
	}
	if err = retrying(ctx, func() error { return w.bob.join(ctx, w.room, "") }); err != nil {
		return nil, err
	}
	if err = waitFor(ctx, 2*time.Minute, func() error { return w.remote.join(ctx, w.room, haServerName) }); err != nil {
		return nil, fmt.Errorf("remote join: %w", err)
	}
	bobSince, err := w.initialSync(ctx, w.bob)
	if err != nil {
		return nil, err
	}
	remoteSince, err := w.initialSync(ctx, w.remote)
	if err != nil {
		return nil, err
	}
	// Traffic outlives the setup context's deadline but stops on halt.
	running, stop := context.WithCancelCause(context.WithoutCancel(ctx))
	w.ctx, w.stop = running, stop
	w.run(func() { w.writer(running, w.alice, e.settings.rate, kindWrite, &w.sends) })
	w.run(func() { w.writer(running, w.remote, 1, kindRemoteWrite, &w.remoteSends) })
	w.run(func() { w.syncer(running, w.bob, bobSince, w.bobSeen, true) })
	w.run(func() { w.syncer(running, w.remote, remoteSince, w.remoteSeen, false) })
	w.run(func() { w.toDeviceSender(running) })
	w.run(func() { w.receiptSender(running) })
	return w, nil
}

// retrying repeats a setup request through transient failures, as a client
// would, so a routing hiccup during setup does not end the scenario.
func retrying(ctx context.Context, op func() error) error {
	return waitFor(ctx, time.Minute, func() error {
		err := op()
		if err != nil && !retryable(err) {
			return errPermanent(err)
		}
		return err
	})
}

// registerRetrying registers a user, choosing a fresh name after a failed
// attempt in case that attempt was committed without an answer.
func registerRetrying(ctx context.Context, base, name string) (*mxClient, error) {
	var c *mxClient
	attempt := 0
	err := retrying(ctx, func() error {
		username := name
		if attempt > 0 {
			username = fmt.Sprintf("%s%d", name, attempt)
		}
		attempt++
		var err error
		c, err = register(ctx, base, username)
		return err
	})
	return c, err
}

func (w *workload) run(fn func()) {
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		fn()
	}()
}

func (w *workload) initialSync(ctx context.Context, c *mxClient) (string, error) {
	var since string
	err := waitFor(ctx, time.Minute, func() error {
		resp, err := c.sync(ctx, "", 0)
		if err != nil {
			return err
		}
		since = resp.NextBatch
		return nil
	})
	return since, err
}

func (w *workload) record(kind string, start time.Time, ok bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.attempts = append(w.attempts, attempt{Kind: kind, Start: start, End: time.Now(), OK: ok})
}

func (w *workload) nextTxn(prefix string) string {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.serial++
	return fmt.Sprintf("%s-%d-%s", prefix, w.serial, nonce())
}

// writer sends messages at rate per second. Each message is retried with its
// original transaction ID until it is acknowledged or the workload stops.
func (w *workload) writer(ctx context.Context, c *mxClient, rate float64, kind string, records *[]*sendRecord) {
	interval := time.Duration(float64(time.Second) / rate)
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
		}
		rec := &sendRecord{Txn: w.nextTxn(kind), Nonce: "hachaos" + nonce(), First: time.Now()}
		w.mu.Lock()
		*records = append(*records, rec)
		w.mu.Unlock()
		w.retry(ctx, kind, rec, func(ctx context.Context) error {
			eventID, err := c.send(ctx, w.room, rec.Txn, rec.Nonce)
			if err == nil {
				w.mu.Lock()
				rec.EventID = eventID
				w.mu.Unlock()
			}
			return err
		})
	}
}

// retry repeats op until it succeeds, fails permanently, or the workload
// stops. Each attempt is bounded, as a client's request timeout would be.
func (w *workload) retry(ctx context.Context, kind string, rec *sendRecord, op func(ctx context.Context) error) {
	for ctx.Err() == nil {
		start := time.Now()
		attemptCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		err := op(attemptCtx)
		cancel()
		w.record(kind, start, err == nil)
		w.mu.Lock()
		rec.Attempts++
		if err == nil {
			rec.Acked, rec.AckedAt, rec.LastErr = true, time.Now(), ""
			w.mu.Unlock()
			return
		}
		rec.LastErr = err.Error()
		w.mu.Unlock()
		if !retryable(err) {
			return
		}
		select {
		case <-ctx.Done():
		case <-time.After(250 * time.Millisecond):
		}
	}
}

// syncer long-polls with the since token it last received. A failed request
// is retried with the same token, so a token issued by a former owner must
// keep working after takeover.
func (w *workload) syncer(ctx context.Context, c *mxClient, since string, seen map[string]time.Time, local bool) {
	for ctx.Err() == nil {
		next, err := w.syncOnce(ctx, c, since, seen, local, 10*time.Second)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			time.Sleep(500 * time.Millisecond)
			continue
		}
		since = next
	}
	w.mu.Lock()
	w.since[c.user] = since
	w.mu.Unlock()
}

// catchUp syncs every client once more from the token its syncer stopped at,
// so writes acknowledged just before the workload stopped are still observed.
func (w *workload) catchUp(ctx context.Context) {
	for _, target := range []struct {
		c     *mxClient
		seen  map[string]time.Time
		local bool
	}{{w.bob, w.bobSeen, true}, {w.remote, w.remoteSeen, false}} {
		w.mu.Lock()
		since := w.since[target.c.user]
		w.mu.Unlock()
		if next, err := w.syncOnce(ctx, target.c, since, target.seen, target.local, 0); err == nil {
			w.mu.Lock()
			w.since[target.c.user] = next
			w.mu.Unlock()
		}
	}
}

// syncOnce performs one sync from since, records what it delivered, and
// returns the next token.
func (w *workload) syncOnce(ctx context.Context, c *mxClient, since string, seen map[string]time.Time, local bool, timeout time.Duration) (string, error) {
	start := time.Now()
	requestCtx, cancel := context.WithTimeout(ctx, 40*time.Second)
	resp, err := c.sync(requestCtx, since, timeout)
	cancel()
	if local && timeout > 0 {
		w.record(kindSync, start, err == nil)
	}
	if err != nil {
		if ctx.Err() == nil && !retryable(err) {
			w.mu.Lock()
			w.syncFailures = append(w.syncFailures, fmt.Sprintf("%s: since %s: %v", c.user, since, err))
			w.mu.Unlock()
		}
		return since, err
	}
	w.observe(resp, since, c, seen, local)
	w.fillGaps(ctx)
	return resp.NextBatch, nil
}

// fillGaps pages back from each limited timeline's prev_batch until it
// reaches an event the client had already seen, as a client does.
func (w *workload) fillGaps(ctx context.Context) {
	w.mu.Lock()
	gaps := w.gaps
	w.gaps = nil
	w.mu.Unlock()
	for _, g := range gaps {
		from := g.from
		for range 50 {
			events, end, err := g.client.page(ctx, w.room, from)
			if err != nil {
				w.mu.Lock()
				w.syncFailures = append(w.syncFailures, fmt.Sprintf("%s: filling a limited timeline from %s: %v", g.client.user, g.from, err))
				w.mu.Unlock()
				break
			}
			reached := false
			now := time.Now()
			w.mu.Lock()
			for _, ev := range events {
				if ev.Type != "m.room.message" {
					continue
				}
				if _, ok := g.seen[ev.Content.Body]; ok {
					reached = true
				} else {
					g.seen[ev.Content.Body] = now
				}
			}
			w.gapFills++
			w.mu.Unlock()
			if reached || end == "" || len(events) == 0 {
				break
			}
			from = end
		}
	}
}

// observe records what one sync response delivered.
func (w *workload) observe(resp *syncResponse, since string, c *mxClient, seen map[string]time.Time, local bool) {
	now := time.Now()
	w.mu.Lock()
	for room, joined := range resp.Rooms.Join {
		if room != w.room {
			continue
		}
		if joined.Timeline.Limited && since != "" {
			// A limited timeline has a gap; a client fills it from prev_batch.
			w.gaps = append(w.gaps, gap{client: c, from: joined.Timeline.PrevBatch, seen: seen})
		}
		for _, ev := range joined.Timeline.Events {
			if ev.Type != "m.room.message" {
				continue
			}
			if _, ok := seen[ev.Content.Body]; !ok {
				seen[ev.Content.Body] = now
			}
			if local {
				w.lastBobEvent = ev.EventID
			}
		}
	}
	if local {
		for _, ev := range resp.ToDevice.Events {
			if _, ok := w.toDeviceSeen[ev.Content.Nonce]; !ok {
				w.toDeviceSeen[ev.Content.Nonce] = now
			}
		}
	}
	w.mu.Unlock()
}

func (w *workload) toDeviceSender(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Second):
		}
		rec := &sendRecord{Txn: w.nextTxn("td"), Nonce: nonce(), First: time.Now()}
		w.mu.Lock()
		w.toDevice = append(w.toDevice, rec)
		w.mu.Unlock()
		w.retry(ctx, "to-device", rec, func(ctx context.Context) error {
			return w.alice.sendToDevice(ctx, rec.Txn, w.bob.user, w.bob.device, rec.Nonce)
		})
	}
}

// receiptSender marks the newest message Bob has seen as read.
func (w *workload) receiptSender(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(3 * time.Second):
		}
		w.mu.Lock()
		eventID := w.lastBobEvent
		w.mu.Unlock()
		if eventID == "" {
			continue
		}
		rec := &sendRecord{Txn: eventID, Nonce: eventID, First: time.Now()}
		w.mu.Lock()
		w.receipts = append(w.receipts, rec)
		w.mu.Unlock()
		w.retry(ctx, "receipt", rec, func(ctx context.Context) error { return w.bob.receipt(ctx, w.room, eventID) })
	}
}

// halt stops generating traffic and waits for in-flight operations.
func (w *workload) halt() {
	w.stop(errors.New("workload stopped"))
	w.wg.Wait()
}

// outcome is one request's result, from the client's side.
type outcome struct {
	Start, End time.Time
	OK         bool
}

// recoveredAt reports when service recovered from a fault at t0: the end of
// the first success after the first failure that followed the fault. A fault
// that caused no failure recovers with the first success after it. An old
// owner that keeps serving until it steps down therefore does not count as
// recovery.
func recoveredAt(samples []outcome, t0 time.Time) (time.Time, bool) {
	var failed *outcome
	for i := range samples {
		s := &samples[i]
		if !s.OK && !s.End.Before(t0) && (failed == nil || s.Start.Before(failed.Start)) {
			failed = s
		}
	}
	var best time.Time
	for _, s := range samples {
		if !s.OK || s.Start.Before(t0) {
			continue
		}
		if failed != nil && (s.Start.Before(failed.Start) || s.End.Before(failed.End)) {
			continue
		}
		if best.IsZero() || s.End.Before(best) {
			best = s.End
		}
	}
	return best, !best.IsZero()
}

// outcomes lists every attempt of a kind.
func (w *workload) outcomes(kind string) []outcome {
	w.mu.Lock()
	defer w.mu.Unlock()
	var out []outcome
	for _, a := range w.attempts {
		if a.Kind == kind {
			out = append(out, outcome{Start: a.Start, End: a.End, OK: a.OK})
		}
	}
	return out
}

// lastFailureAfter reports whether any attempt of a kind failed after t, and
// the end of the last one. It separates "never disrupted" from "recovered".
func (w *workload) lastFailureAfter(kind string, t time.Time) (time.Time, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	var last time.Time
	for _, a := range w.attempts {
		if a.Kind == kind && !a.OK && a.End.After(t) && a.End.After(last) {
			last = a.End
		}
	}
	return last, !last.IsZero()
}

func (w *workload) sendsSnapshot() []sendRecord {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make([]sendRecord, 0, len(w.sends))
	for _, r := range w.sends {
		out = append(out, *r)
	}
	return out
}

func (w *workload) snapshot() (sends, remoteSends, toDevice, receipts []sendRecord) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, r := range w.sends {
		sends = append(sends, *r)
	}
	for _, r := range w.remoteSends {
		remoteSends = append(remoteSends, *r)
	}
	for _, r := range w.toDevice {
		toDevice = append(toDevice, *r)
	}
	for _, r := range w.receipts {
		receipts = append(receipts, *r)
	}
	return sends, remoteSends, toDevice, receipts
}

// mediaProbe uploads a blob and remembers it, so the verifier can prove it
// survived takeover byte for byte.
type mediaProbe struct {
	URI     string
	Content []byte
}

func (w *workload) uploadProbe(ctx context.Context) (mediaProbe, error) {
	content := []byte("hachaos media " + nonce() + " " + time.Now().UTC().Format(time.RFC3339Nano))
	var uri string
	err := waitFor(ctx, 2*time.Minute, func() error {
		var err error
		uri, err = w.alice.upload(ctx, content)
		return err
	})
	return mediaProbe{URI: uri, Content: content}, err
}

func (w *workload) checkProbe(ctx context.Context, p mediaProbe) error {
	return waitFor(ctx, time.Minute, func() error {
		data, err := w.bob.download(ctx, p.URI)
		if err != nil {
			return err
		}
		if !bytes.Equal(data, p.Content) {
			return errPermanent(fmt.Errorf("%s: downloaded %d bytes that differ from the upload", p.URI, len(data)))
		}
		return nil
	})
}
