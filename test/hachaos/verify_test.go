//go:build linux

package hachaos

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

// result is everything a scenario proved, measured, or failed to prove.
type result struct {
	Name        string   `json:"name"`
	Description string   `json:"description"`
	Tier        string   `json:"tier"`
	Timings     timings  `json:"timings"`
	Passed      bool     `json:"passed"`
	Failures    []string `json:"failures,omitempty"`
	Notes       []string `json:"notes,omitempty"`

	FaultAt  time.Time `json:"fault_at,omitzero"`
	Recovery recovery  `json:"recovery"`

	Generations []ownershipRecord `json:"generations,omitempty"`
	Counts      counts            `json:"counts"`
	Duration    time.Duration     `json:"duration"`
}

// recovery is measured from fault injection to the first success. A nil
// value means the scenario never observed that kind of success after the
// fault; Disrupted reports whether any failure happened at all.
type recovery struct {
	Target             time.Duration  `json:"target"`
	ClientWrite        *time.Duration `json:"client_write,omitempty"`
	Sync               *time.Duration `json:"sync,omitempty"`
	FederationInbound  *time.Duration `json:"federation_inbound,omitempty"`
	FederationOutbound *time.Duration `json:"federation_outbound,omitempty"`
	Disrupted          bool           `json:"disrupted"`
	// SuspendsObjective marks scenarios where the design deliberately stops
	// serving until a dependency returns, such as a control-plane outage.
	SuspendsObjective bool `json:"suspends_objective,omitempty"`
}

type counts struct {
	Acked      int `json:"acked_writes"`
	Unacked    int `json:"unacknowledged_writes"`
	Retried    int `json:"writes_retried"`
	Lost       int `json:"lost_writes"`
	Unserved   int `json:"stored_but_missing_from_messages"`
	Duplicated int `json:"duplicated_writes"`
	// DuplicatedAcrossTakeover counts retries whose first attempt reached the
	// retired owner, which the process-local transaction cache cannot catch.
	DuplicatedAcrossTakeover int `json:"duplicated_across_takeover"`
	UnackedCommitted         int `json:"unacknowledged_but_committed"`
	RemoteAcked              int `json:"remote_acked_writes"`
	ToDeviceAcked            int `json:"to_device_acked"`
	AuditedCommits           int `json:"audited_commits"`
	BrokerMessages           int `json:"broker_messages"`
	LateBroker               int `json:"retired_generation_publications"`
	MediaWrites              int `json:"media_writes"`
	FederationSends          int `json:"federation_requests"`
}

func (r *result) failf(format string, args ...any) {
	r.Failures = append(r.Failures, fmt.Sprintf(format, args...))
}

func (r *result) notef(format string, args ...any) {
	r.Notes = append(r.Notes, fmt.Sprintf(format, args...))
}

func durationPtr(d time.Duration) *time.Duration { return &d }

// measureRecovery fills in recovery times relative to the fault.
func measureRecovery(r *result, w *workload, e *environment) {
	t0 := r.FaultAt
	if _, ok := w.lastFailureAfter(kindWrite, t0); ok {
		r.Recovery.Disrupted = true
	}
	since := func(at time.Time, ok bool) *time.Duration {
		if !ok {
			return nil
		}
		return durationPtr(at.Sub(t0))
	}
	r.Recovery.ClientWrite = since(recoveredAt(w.outcomes(kindWrite), t0))
	r.Recovery.Sync = since(recoveredAt(w.outcomes(kindSync), t0))
	r.Recovery.FederationInbound = since(recoveredAt(e.federationOutcomes(true), t0))
	r.Recovery.FederationOutbound = since(recoveredAt(e.federationOutcomes(false), t0))
	if !r.Recovery.Disrupted || r.Recovery.SuspendsObjective {
		return
	}
	target := r.Recovery.Target
	if r.Recovery.ClientWrite == nil {
		r.failf("client writes never recovered after the fault")
	} else if *r.Recovery.ClientWrite > target {
		r.failf("client writes recovered after %s, beyond the %s objective", r.Recovery.ClientWrite.Round(time.Millisecond), target)
	}
	if r.Recovery.FederationInbound == nil {
		r.failf("no federation request from the remote server succeeded after the fault")
	} else if *r.Recovery.FederationInbound > target {
		r.notef("inbound federation recovered after %s, beyond the %s objective; the remote server's own retry backoff governs this", r.Recovery.FederationInbound.Round(time.Millisecond), target)
	}
	if r.Recovery.FederationOutbound == nil {
		r.failf("no federation request to the remote server succeeded after the fault")
	} else if *r.Recovery.FederationOutbound > target {
		r.failf("outbound federation recovered after %s, beyond the %s objective", r.Recovery.FederationOutbound.Round(time.Millisecond), target)
	}
}

// verifyClientState checks what clients were told against what the servers
// hold: every acknowledged write exists exactly once, was delivered to
// syncing clients on both servers, and every to-device message arrived.
func verifyClientState(ctx context.Context, r *result, w *workload, e *environment, settle time.Duration) {
	sends, remoteSends, toDevice, receipts := w.snapshot()
	// The sync API applies room events asynchronously and can lag behind a
	// takeover, so wait for every acknowledged write before judging.
	var history []roomEvent
	var occurrences map[string]int
	err := waitFor(ctx, settle, func() error {
		var err error
		if history, err = newClient(e.router.url()).withToken(w.alice).messages(ctx, w.room); err != nil {
			return err
		}
		occurrences = map[string]int{}
		for _, ev := range history {
			if ev.Type == "m.room.message" {
				occurrences[ev.Content.Body]++
			}
		}
		for _, s := range sends {
			if s.Acked && occurrences[s.Nonce] == 0 {
				return fmt.Errorf("%s is not in /messages yet", s.Txn)
			}
		}
		return nil
	})
	if history == nil {
		r.failf("read room history after recovery: %v", err)
		return
	}
	var acquisitions []time.Time
	if ownership, err := e.pg.ownershipHistory(ctx); err == nil {
		for _, h := range ownership[min(1, len(ownership)):] {
			acquisitions = append(acquisitions, h.AcquiredAt)
		}
	}
	for _, s := range sends {
		if s.Attempts > 1 {
			r.Counts.Retried++
		}
		n := occurrences[s.Nonce]
		switch {
		case s.Acked:
			r.Counts.Acked++
			if n == 0 {
				stored := e.pg.whereIs(ctx, s.EventID)
				if stored == "in no table" {
					r.Counts.Lost++
				} else {
					r.Counts.Unserved++
				}
				r.failf("acknowledged write %s (event %s, acked %s) is missing from /messages; database: %s", s.Txn, s.EventID, s.AckedAt.Format(time.RFC3339Nano), stored)
				diagnosis := e.pg.diagnoseEvent(ctx, s.EventID) + "\n/messages returned, newest first:\n"
				for _, ev := range history {
					diagnosis += fmt.Sprintf("  %s %s %s\n", ev.EventID, ev.Type, ev.Content.Body)
				}
				_ = os.WriteFile(filepath.Join(e.logs, "missing-"+s.Txn+".txt"), []byte(diagnosis), 0o644)
			}
		default:
			r.Counts.Unacked++
			if n == 1 {
				// The request timed out or failed after the server committed
				// it: an allowed transport outcome, not a duplicate.
				r.Counts.UnackedCommitted++
			}
		}
		if n > 1 {
			// Record which events carry the body, so a duplicate can be told
			// apart from one event that /messages returned twice.
			var ids []string
			for _, ev := range history {
				if ev.Content.Body == s.Nonce {
					ids = append(ids, ev.EventID)
				}
			}
			diagnosis := fmt.Sprintf("transaction %s, acknowledged event %s, attempts %d, first %s, acked %s\nevents with its body in /messages: %v\n\n",
				s.Txn, s.EventID, s.Attempts, s.First.Format(time.RFC3339Nano), s.AckedAt.Format(time.RFC3339Nano), ids)
			for _, id := range slices.Compact(slices.Sorted(slices.Values(ids))) {
				diagnosis += e.pg.diagnoseEvent(ctx, id) + "\n"
			}
			_ = os.WriteFile(filepath.Join(e.logs, "duplicate-"+s.Txn+".txt"), []byte(diagnosis), 0o644)
			// Transaction IDs are deduplicated in a process-local cache, as
			// the HA documentation states. A request the old owner accepted
			// into the room input stream, but could not answer before it
			// stopped, is applied by the successor; the client's retry then
			// creates a second event. That is the documented limit, so it is
			// counted separately; any other duplicate is a failure.
			if spansTakeover(s, acquisitions) {
				r.Counts.DuplicatedAcrossTakeover++
				r.notef("logical transaction %s produced %d events: its first attempt reached the retired owner and its retry the successor, beyond the process-local transaction cache", s.Txn, n)
			} else {
				r.Counts.Duplicated++
				r.failf("logical transaction %s produced %d events", s.Txn, n)
			}
		}
	}

	// Remote writes arrive over federation once the remote retries.
	for _, s := range remoteSends {
		if !s.Acked {
			continue
		}
		r.Counts.RemoteAcked++
	}
	awaitAll(ctx, r, settle, "remote write visible on the HA server", func() []string {
		history, err := newClient(e.router.url()).withToken(w.alice).messages(ctx, w.room)
		if err != nil {
			return []string{err.Error()}
		}
		seen := map[string]int{}
		for _, ev := range history {
			seen[ev.Content.Body]++
		}
		var missing []string
		for _, s := range remoteSends {
			if s.Acked && seen[s.Nonce] == 0 {
				missing = append(missing, s.Txn)
			}
			if seen[s.Nonce] > 1 {
				missing = append(missing, s.Txn+" (duplicated)")
			}
		}
		return missing
	})

	// Every acknowledged local write reaches a syncing client that kept its
	// token across takeover, and the remote server's syncing client.
	missingSync := awaitAll(ctx, r, settle, "acknowledged write delivered to /sync with an existing token", func() []string {
		w.catchUp(ctx)
		w.mu.Lock()
		defer w.mu.Unlock()
		var missing []string
		for _, s := range sends {
			if s.Acked {
				if _, ok := w.bobSeen[s.Nonce]; !ok {
					missing = append(missing, s.Txn)
				}
			}
		}
		return missing
	})
	for _, txn := range missingSync[:min(len(missingSync), 3)] {
		for _, s := range sends {
			if s.Txn == txn {
				_ = os.WriteFile(filepath.Join(e.logs, "missing-sync-"+txn+".txt"), []byte(diagnoseSync(ctx, w, e, s)), 0o644)
			}
		}
	}
	awaitAll(ctx, r, settle, "acknowledged write delivered over federation to the remote server", func() []string {
		w.catchUp(ctx)
		w.mu.Lock()
		defer w.mu.Unlock()
		var missing []string
		for _, s := range sends {
			if s.Acked {
				if _, ok := w.remoteSeen[s.Nonce]; !ok {
					missing = append(missing, s.Txn)
				}
			}
		}
		return missing
	})
	awaitAll(ctx, r, settle, "acknowledged to-device message delivered", func() []string {
		w.catchUp(ctx)
		w.mu.Lock()
		defer w.mu.Unlock()
		var missing []string
		for _, s := range toDevice {
			if s.Acked {
				if _, ok := w.toDeviceSeen[s.Nonce]; !ok {
					missing = append(missing, s.Txn)
				}
			}
		}
		return missing
	})
	for _, s := range toDevice {
		if s.Acked {
			r.Counts.ToDeviceAcked++
		}
	}
	w.mu.Lock()
	failures := append([]string(nil), w.syncFailures...)
	gapFills := w.gapFills
	w.mu.Unlock()
	if gapFills > 0 {
		r.notef("syncing clients filled %d page(s) of limited timelines from prev_batch", gapFills)
	}
	for _, f := range failures {
		r.failf("sync: %s", f)
	}
	verifyReceipt(ctx, r, w, e, receipts, settle)
}

// spansTakeover reports whether a write's first attempt preceded a successor
// acquiring ownership and its acknowledgement followed it.
func spansTakeover(s sendRecord, acquisitions []time.Time) bool {
	for _, at := range acquisitions {
		if s.First.Before(at) && s.AckedAt.After(at) {
			return true
		}
	}
	return false
}

// awaitAll waits until check reports nothing missing, and fails with a
// sample of what is still missing otherwise.
func awaitAll(ctx context.Context, r *result, timeout time.Duration, what string, check func() []string) []string {
	var missing []string
	_ = waitFor(ctx, timeout, func() error {
		missing = check()
		if len(missing) > 0 {
			return fmt.Errorf("%d missing", len(missing))
		}
		return nil
	})
	if len(missing) > 0 {
		sample := missing
		if len(sample) > 5 {
			sample = sample[:5]
		}
		r.failf("%d not %s within %s, e.g. %s", len(missing), what, timeout, strings.Join(sample, ", "))
	}
	return missing
}

// diagnoseSync shows where the sync API stored a write that a syncing client
// never received, and the client's /sync requests around it. The request
// whose token range spans the event's position, without returning it, is the
// one that skipped it.
func diagnoseSync(ctx context.Context, w *workload, e *environment, s sendRecord) string {
	var b strings.Builder
	fmt.Fprintf(&b, "write %s, event %s, first attempt %s, acknowledged %s\n", s.Txn, s.EventID,
		s.First.Format(time.RFC3339Nano), s.AckedAt.Format(time.RFC3339Nano))
	pos, committed, err := e.pg.syncPosition(ctx, s.EventID)
	switch {
	case err != nil:
		fmt.Fprintf(&b, "sync API position: unknown: %v\n", err)
	case pos < 0:
		fmt.Fprintf(&b, "sync API position: not stored; database: %s\n", e.pg.whereIs(ctx, s.EventID))
	default:
		fmt.Fprintf(&b, "sync API position %d, committed %v\n", pos, fmtTime(committed))
	}
	b.WriteString("\n/sync requests by " + w.bob.user + " (start, end, since PDU, next_batch PDU, limited, events; > spans the position, * returned the event):\n")
	for _, t := range w.syncTraces() {
		if t.User != w.bob.user {
			continue
		}
		from, to := pduPosition(t.Since), pduPosition(t.NextBatch)
		spans := pos >= 0 && from < pos && pos <= to
		near := t.End.After(s.First.Add(-30*time.Second)) && t.Start.Before(s.AckedAt.Add(30*time.Second))
		if !spans && !near {
			continue
		}
		marker := " "
		if spans {
			marker = ">"
		}
		if slices.Contains(t.Events, s.EventID) {
			marker += "*"
		}
		fmt.Fprintf(&b, "%-2s %s %s %d %d %t %d\n", marker, t.Start.Format("15:04:05.000"), t.End.Format("15:04:05.000"), from, to, t.Limited, len(t.Events))
	}
	return b.String()
}

// pduPosition reads the PDU stream position from a sync token, "s<pdu>_...".
func pduPosition(token string) int64 {
	first, _, _ := strings.Cut(strings.TrimPrefix(token, "s"), "_")
	pos, err := strconv.ParseInt(first, 10, 64)
	if err != nil {
		return 0
	}
	return pos
}

func fmtTime(t *time.Time) string {
	if t == nil {
		return "unknown"
	}
	return t.Format(time.RFC3339Nano)
}

// verifyReceipt checks that the newest acknowledged read receipt, or one sent
// after it, is visible to another member after takeover.
func verifyReceipt(ctx context.Context, r *result, w *workload, e *environment, receipts []sendRecord, settle time.Duration) {
	accepted := newestReceipts(receipts)
	if len(accepted) == 0 {
		r.notef("no read receipt was acknowledged during the scenario")
		return
	}
	last := accepted[0]
	alice := newClient(e.router.url()).withToken(w.alice)
	var shown string
	err := waitFor(ctx, settle, func() error {
		resp, err := alice.sync(ctx, "", 0)
		if err != nil {
			return err
		}
		shown = ""
		for _, ev := range resp.Rooms.Join[w.room].Ephemeral.Events {
			if ev.Type != "m.receipt" {
				continue
			}
			for eventID, byType := range ev.Content {
				if _, ok := byType["m.read"][w.bob.user]; ok {
					shown = eventID
				}
			}
		}
		if slices.Contains(accepted, shown) {
			return nil
		}
		return fmt.Errorf("receipt for %s not visible; the server shows %q", last, shown)
	})
	switch {
	case err != nil:
		r.failf("acknowledged read receipt: %v", err)
		_ = os.WriteFile(filepath.Join(e.logs, "missing-receipt.txt"), []byte(diagnoseReceipt(ctx, w, e, receipts)), 0o644)
	case shown != last:
		r.notef("the visible read receipt, for %s, was sent after the newest acknowledged one and never acknowledged: the workload halted with it in flight after the server had stored it", shown)
	}
}

// newestReceipts lists the receipts a server may show once the workload stops:
// the newest acknowledged one first, then every receipt sent after it. A
// receipt in flight when the workload halts is canceled on the client, but
// the server may already have published it, so, like an unacknowledged
// message that was committed anyway, it can replace the acknowledged one.
func newestReceipts(receipts []sendRecord) []string {
	for i := len(receipts) - 1; i >= 0; i-- {
		if !receipts[i].Acked {
			continue
		}
		var out []string
		for _, rec := range receipts[i:] {
			out = append(out, rec.Nonce)
		}
		return out
	}
	return nil
}

// diagnoseReceipt separates a receipt that never reached the broker, one the
// sync API never consumed, and one a later write replaced.
func diagnoseReceipt(ctx context.Context, w *workload, e *environment, receipts []sendRecord) string {
	var b strings.Builder
	const recent = 12
	b.WriteString("receipts sent, oldest first (event, first attempt, acked at, attempts, last error):\n")
	var events []string
	for _, rec := range receipts[max(0, len(receipts)-recent):] {
		fmt.Fprintf(&b, "  %s %s %s %d %s\n", rec.Nonce, rec.First.Format(time.RFC3339Nano), rec.AckedAt.Format(time.RFC3339Nano), rec.Attempts, rec.LastErr)
		if !slices.Contains(events, rec.Nonce) {
			events = append(events, rec.Nonce)
		}
	}
	b.WriteString("\nreceipt publications, oldest first (at, user, event, generation):\n")
	pubs := e.nats.receiptPublications()
	for _, p := range pubs[max(0, len(pubs)-recent):] {
		fmt.Fprintf(&b, "  %s %s %s %s\n", p.At.Format(time.RFC3339Nano), p.User, p.EventID, p.Generation)
	}
	b.WriteString("\n" + e.nats.consumerState("OutputReceiptEvent", "SyncAPIReceiptConsumerPull") + "\n\n")
	b.WriteString(e.pg.receiptState(ctx, w.room, w.bob.user, events...))
	return b.String()
}

// verifyMedia proves that media uploaded before the fault survives and that
// uploads work after recovery.
func verifyMedia(ctx context.Context, r *result, w *workload, before mediaProbe) {
	if err := w.checkProbe(ctx, before); err != nil {
		r.failf("media uploaded before the fault: %v", err)
	}
	after, err := w.uploadProbe(ctx)
	if err != nil {
		r.failf("upload after recovery: %v", err)
		return
	}
	if err = w.checkProbe(ctx, after); err != nil {
		r.failf("media uploaded after recovery: %v", err)
	}
}

// verifySearch proves the successor's rebuilt index covers messages from
// before and after the fault.
func verifySearch(ctx context.Context, r *result, w *workload, settle time.Duration) {
	sends := w.sendsSnapshot()
	var before, after *sendRecord
	for i := range sends {
		s := &sends[i]
		if !s.Acked {
			continue
		}
		if s.AckedAt.Before(r.FaultAt) {
			before = s
		} else if after == nil {
			after = s
		}
	}
	for _, s := range []*sendRecord{before, after} {
		if s == nil {
			continue
		}
		err := waitFor(ctx, settle, func() error {
			ids, err := w.alice.search(ctx, s.Nonce)
			if err != nil {
				return err
			}
			for _, id := range ids {
				if id == s.EventID {
					return nil
				}
			}
			return fmt.Errorf("search for %s returned %v", s.Nonce, ids)
		})
		if err != nil {
			r.failf("search after recovery: %v", err)
		}
	}
}

// verifyOwnership checks the database, broker, media and federation
// boundaries for overlapping owners.
//
// The database is strict: every commit of a generation must precede every
// commit of any later generation, because promotion terminates the retired
// generation's backends before the successor starts. Boundaries the database
// cannot reach are protected by the ownership freshness window instead, so a
// retired generation may act for a bounded time after its successor acquired
// ownership, and never after that.
func verifyOwnership(ctx context.Context, r *result, e *environment, pauses map[string][]time.Time) {
	history, err := e.pg.ownershipHistory(ctx)
	if err != nil {
		r.failf("read ownership history: %v", err)
		return
	}
	r.Generations = history
	acquired := map[int64]time.Time{}
	owners := map[int64]string{}
	for i, h := range history {
		acquired[h.Generation] = h.AcquiredAt
		owners[h.Generation] = h.Owner
		if i > 0 && h.Generation != history[i-1].Generation+1 {
			r.failf("ownership generations are not contiguous: %d follows %d", h.Generation, history[i-1].Generation)
		}
	}
	commits, err := e.pg.auditedCommits(ctx)
	if err != nil {
		r.failf("read audit: %v", err)
		return
	}
	r.Counts.AuditedCommits = len(commits)
	first := map[int64]time.Time{}
	last := map[int64]time.Time{}
	for _, c := range commits {
		if c.Generation == 0 {
			if c.App != harnessApp {
				r.failf("table %s was written by %q, which carries no activation marker", c.Table, c.App)
			}
			continue
		}
		if f, ok := first[c.Generation]; !ok || c.Committed.Before(f) {
			first[c.Generation] = c.Committed
		}
		if l, ok := last[c.Generation]; !ok || c.Committed.After(l) {
			last[c.Generation] = c.Committed
		}
	}
	var generations []int64
	for g := range first {
		generations = append(generations, g)
	}
	sort.Slice(generations, func(i, j int) bool { return generations[i] < generations[j] })
	for i, g := range generations {
		for _, later := range generations[i+1:] {
			if !last[g].Before(first[later]) {
				r.failf("split brain at the database: generation %d committed at %s, after generation %d first committed at %s",
					g, last[g].Format(time.RFC3339Nano), later, first[later].Format(time.RFC3339Nano))
			}
		}
	}

	// A retired generation may act until it notices its successor, which
	// takes at most one heartbeat interval, plus scheduling slack.
	bound := e.timings.Retry + 2*time.Second
	retiredAt := func(g int64) (time.Time, bool) {
		next, ok := acquired[g+1]
		return next, ok
	}
	inFlight := func(owner string, at time.Time) bool {
		for _, resumed := range pauses[owner] {
			if !at.Before(resumed) && at.Sub(resumed) < 2*time.Second {
				return true
			}
		}
		return false
	}
	pubs := e.nats.publications()
	r.Counts.BrokerMessages = len(pubs)
	for _, p := range pubs {
		next, ok := retiredAt(p.Generation)
		if !ok || p.At.Before(next) {
			continue
		}
		r.Counts.LateBroker++
		if p.At.Sub(next) > bound {
			r.failf("generation %d published %s at %s, %s after generation %d acquired ownership", p.Generation, p.Subject, p.At.Format(time.RFC3339Nano), p.At.Sub(next).Round(time.Millisecond), p.Generation+1)
		}
	}
	if r.Counts.LateBroker > 0 {
		r.notef("%d publication(s) by a retired generation within the %s freshness bound after its successor acquired ownership", r.Counts.LateBroker, bound)
	}

	generationOf := ownerGenerations(history)
	for _, m := range e.s3.mutations() {
		if !strings.HasPrefix(m.Owner, "cand-") || m.Status/100 != 2 {
			continue
		}
		r.Counts.MediaWrites++
		g := generationOf(m.Owner, m.At)
		next, ok := retiredAt(g)
		if g == 0 || !ok || m.At.Before(next) || inFlight(m.Owner, m.At) {
			continue
		}
		if m.At.Sub(next) > bound {
			r.failf("%s (generation %d) wrote media %s %s at %s, after generation %d acquired ownership", m.Owner, g, m.Method, m.Path, m.At.Format(time.RFC3339Nano), g+1)
		}
	}
	for _, f := range e.fed.requests() {
		// Only a request the remote server accepted is an effect; one that
		// never reached it, such as a dead process's request released by a
		// healed partition, is not.
		if !strings.HasPrefix(f.Owner, "cand-") || f.Host != remoteServerName || f.Status/100 != 2 {
			continue
		}
		r.Counts.FederationSends++
		g := generationOf(f.Owner, f.At)
		next, ok := retiredAt(g)
		if g == 0 || !ok || f.At.Before(next) || inFlight(f.Owner, f.At) {
			continue
		}
		if f.At.Sub(next) > bound {
			r.failf("%s (generation %d) sent federation %s %s at %s, after generation %d acquired ownership", f.Owner, g, f.Method, f.Path, f.At.Format(time.RFC3339Nano), g+1)
		}
	}

	if overlaps := e.router.routingOverlaps(); len(overlaps) > 0 {
		r.failf("the router saw %d check round(s) with more than one active backend, first at %s", len(overlaps), overlaps[0].Format(time.RFC3339Nano))
	}
	if overlaps := e.observer.readyOverlaps(); len(overlaps) > 0 {
		r.failf("%d status sample(s) found more than one candidate ready, first at %s", len(overlaps), overlaps[0].Format(time.RFC3339Nano))
	}
}

// ownerGenerations maps a candidate and a time to the generation it held
// then. Candidate identities are the slot name plus a per-process suffix, so
// the most recent generation acquired by that slot before t is the one its
// process was acting under.
func ownerGenerations(history []ownershipRecord) func(owner string, at time.Time) int64 {
	return func(owner string, at time.Time) int64 {
		var g int64
		for _, h := range history {
			if strings.HasPrefix(h.Owner, owner+"_") && !h.AcquiredAt.After(at) {
				g = h.Generation
			}
		}
		return g
	}
}

func TestNewestReceipts(t *testing.T) {
	acked := func(event string) sendRecord { return sendRecord{Nonce: event, Acked: true} }
	unacked := func(event string) sendRecord { return sendRecord{Nonce: event} }
	for _, tc := range []struct {
		name     string
		receipts []sendRecord
		want     []string
	}{
		{"none", nil, nil},
		{"none acknowledged", []sendRecord{unacked("$a")}, nil},
		{"newest acknowledged", []sendRecord{acked("$a"), acked("$b")}, []string{"$b"}},
		{"in flight at halt", []sendRecord{unacked("$a"), acked("$b"), unacked("$c")}, []string{"$b", "$c"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := newestReceipts(tc.receipts); !slices.Equal(got, tc.want) {
				t.Fatalf("newestReceipts() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestPDUPosition(t *testing.T) {
	for token, want := range map[string]int64{
		"s37_0_5_4_0_2_0_74_0":     37,
		"s125_0_45_42_0_2_0_173_0": 125,
		"":                         0,
		"t12-34":                   0,
	} {
		if got := pduPosition(token); got != want {
			t.Errorf("pduPosition(%q) = %d, want %d", token, got, want)
		}
	}
}
