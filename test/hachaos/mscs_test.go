//go:build linux

package hachaos

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

// haMSCs are the experimental MSCs qualified for HA. Every scenario runs with
// them enabled on the candidates and the remote server, so every fault is
// also a fault for their consumers, tables and federation traffic.
var haMSCs = []string{"msc2444", "msc2753", "msc2836"}

const (
	kindMSCReply       = "msc-reply"
	kindMSCRemoteWrite = "msc-remote-write"
	// MscInterval paces each MSC writer. It is slower than the main writer:
	// the probe checks delivery paths, not throughput.
	mscInterval = 2 * time.Second
)

// mscProbe is MSC traffic that runs alongside the workload:
//   - replies in two world-readable HA rooms, room versions 11 and 12, each
//     naming a thread root with an MSC2836 m.relationship;
//   - a local user peeking into both rooms via /sync (MSC2753);
//   - a remote user peeking into both rooms over federation (MSC2444, inbound);
//   - the local user peeking into a room on the remote server (MSC2444,
//     outbound), in which the remote server writes.
//
// Writes are retried with their transaction IDs like the workload's, and the
// peekers keep their sync tokens across takeover.
type mscProbe struct {
	w *workload
	e *environment

	rooms      []string
	parents    map[string]string
	remoteRoom string
	peeker     *mxClient
	remote     *mxClient

	mu          sync.Mutex
	replies     []*mscReply
	remoteSends []*sendRecord
	peekerSeen  map[string]time.Time
	remoteSeen  map[string]time.Time
	since       map[string]string
	failures    []string
}

// mscReply is a reply sent into one of the HA rooms.
type mscReply struct {
	room string
	rec  *sendRecord
}

// peekSyncResponse is the part of /sync that carries peeked rooms.
type peekSyncResponse struct {
	NextBatch string `json:"next_batch"`
	Rooms     struct {
		Peek map[string]struct {
			Timeline struct {
				Events    []roomEvent `json:"events"`
				Limited   bool        `json:"limited"`
				PrevBatch string      `json:"prev_batch"`
			} `json:"timeline"`
		} `json:"peek"`
	} `json:"rooms"`
}

func (c *mxClient) peekSync(ctx context.Context, since string, timeout time.Duration) (*peekSyncResponse, error) {
	q := url.Values{"timeout": {fmt.Sprint(timeout.Milliseconds())}, "filter": {timelineFilter}}
	if since != "" {
		q.Set("since", since)
	}
	var out peekSyncResponse
	err := c.do(ctx, http.MethodGet, "/_matrix/client/v3/sync?"+q.Encode(), nil, &out)
	return &out, err
}

// createPeekableRoom creates a public, world-readable room of a room version.
func (c *mxClient) createPeekableRoom(ctx context.Context, name, version string) (string, error) {
	var out struct {
		RoomID string `json:"room_id"`
	}
	err := c.do(ctx, http.MethodPost, "/_matrix/client/v3/createRoom", map[string]any{
		"preset": "public_chat", "name": name, "visibility": "private", "room_version": version,
		"initial_state": []any{map[string]any{
			"type": "m.room.history_visibility", "state_key": "", "content": map[string]any{"history_visibility": "world_readable"},
		}},
	}, &out)
	return out.RoomID, err
}

func (c *mxClient) peek(ctx context.Context, room, via string) error {
	path := "/_matrix/client/v3/peek/" + url.PathEscape(room)
	if via != "" {
		path += "?server_name=" + url.QueryEscape(via)
	}
	return c.do(ctx, http.MethodPost, path, map[string]any{}, nil)
}

func (c *mxClient) sendReply(ctx context.Context, room, txn, body, parent string) (string, error) {
	var out struct {
		EventID string `json:"event_id"`
	}
	path := fmt.Sprintf("/_matrix/client/v3/rooms/%s/send/m.room.message/%s", url.PathEscape(room), url.PathEscape(txn))
	err := c.do(ctx, http.MethodPut, path, map[string]any{
		"msgtype": "m.text", "body": body,
		"m.relationship": map[string]any{"rel_type": "m.reference", "event_id": parent},
	}, &out)
	return out.EventID, err
}

// children returns the thread replies /event_relationships knows for parent.
func (c *mxClient) children(ctx context.Context, room, parent string) (map[string]bool, error) {
	var out struct {
		Events []roomEvent `json:"events"`
	}
	err := c.do(ctx, http.MethodPost, "/_matrix/client/unstable/event_relationships", map[string]any{
		"event_id": parent, "room_id": room, "include_children": true, "limit": 100000, "max_depth": 1, "max_breadth": 100000,
	}, &out)
	children := map[string]bool{}
	for _, ev := range out.Events {
		children[ev.EventID] = true
	}
	return children, err
}

// startMSCProbe sets up the MSC rooms and peeks, then starts the probe's
// traffic on the workload so it stops with it.
//
//nolint:contextcheck // Traffic runs on the workload's context, which outlives setup's deadline.
func startMSCProbe(ctx context.Context, w *workload) (*mscProbe, error) {
	e := w.e
	p := &mscProbe{w: w, e: e, parents: map[string]string{}, peekerSeen: map[string]time.Time{}, remoteSeen: map[string]time.Time{}, since: map[string]string{}}
	var err error
	if p.peeker, err = registerRetrying(ctx, e.router.url(), "petra"); err != nil {
		return nil, err
	}
	if p.remote, err = registerRetrying(ctx, e.remote.httpURL(), "remy"); err != nil {
		return nil, err
	}
	for _, version := range []string{"11", "12"} {
		var room string
		started := time.Now()
		if err = retryingSetup(ctx, func() error {
			defer e.watchSetup(ctx, "create-room-v"+version)()
			room, err = w.alice.patient().createPeekableRoom(ctx, "msc-v"+version, version)
			return err
		}); err != nil {
			return nil, fmt.Errorf("create a version %s room: %w", version, err)
		}
		w.noteSlowSetup("creating the version "+version+" MSC room", started)
		txn := "msc-root-" + nonce()
		var parent string
		if err = retrying(ctx, func() error {
			parent, err = w.alice.send(ctx, room, txn, "msc thread root")
			return err
		}); err != nil {
			return nil, fmt.Errorf("send a thread root: %w", err)
		}
		p.rooms = append(p.rooms, room)
		p.parents[room] = parent
	}
	if err = retrying(ctx, func() error {
		p.remoteRoom, err = p.remote.createPeekableRoom(ctx, "msc-remote", "12")
		return err
	}); err != nil {
		return nil, fmt.Errorf("create the remote room: %w", err)
	}
	for _, room := range p.rooms {
		if err = retrying(ctx, func() error { return p.peeker.peek(ctx, room, "") }); err != nil {
			return nil, fmt.Errorf("local peek into %s: %w", room, err)
		}
		if err = waitFor(ctx, 2*time.Minute, func() error { return p.remote.peek(ctx, room, haServerName) }); err != nil {
			return nil, fmt.Errorf("remote peek into %s: %w", room, err)
		}
	}
	if err = waitFor(ctx, 2*time.Minute, func() error { return p.peeker.peek(ctx, p.remoteRoom, remoteServerName) }); err != nil {
		return nil, fmt.Errorf("peek into the remote room: %w", err)
	}
	for _, c := range []*mxClient{p.peeker, p.remote} {
		if err = waitFor(ctx, time.Minute, func() error {
			resp, err := c.peekSync(ctx, "", 0)
			if err == nil {
				p.since[c.user] = resp.NextBatch
			}
			return err
		}); err != nil {
			return nil, fmt.Errorf("initial peek sync for %s: %w", c.user, err)
		}
	}
	running := w.ctx
	w.run(func() { p.replyWriter(running) })
	w.run(func() { p.remoteWriter(running) })
	w.run(func() { p.syncer(running, p.peeker, p.peekerSeen) })
	w.run(func() { p.syncer(running, p.remote, p.remoteSeen) })
	return p, nil
}

// replyWriter sends thread replies, alternating between the HA rooms.
func (p *mscProbe) replyWriter(ctx context.Context) {
	for i := 0; ; i++ {
		select {
		case <-ctx.Done():
			return
		case <-time.After(mscInterval):
		}
		room := p.rooms[i%len(p.rooms)]
		rec := &sendRecord{Txn: p.w.nextTxn(kindMSCReply), Nonce: "msc" + nonce(), First: time.Now()}
		p.mu.Lock()
		p.replies = append(p.replies, &mscReply{room: room, rec: rec})
		p.mu.Unlock()
		p.w.retry(ctx, kindMSCReply, rec, func(ctx context.Context) error {
			eventID, err := p.w.alice.sendReply(ctx, room, rec.Txn, rec.Nonce, p.parents[room])
			if err == nil {
				p.mu.Lock()
				rec.EventID = eventID
				p.mu.Unlock()
			}
			return err
		})
	}
}

// remoteWriter writes into the remote room the HA server peeks into.
func (p *mscProbe) remoteWriter(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(mscInterval):
		}
		rec := &sendRecord{Txn: p.w.nextTxn(kindMSCRemoteWrite), Nonce: "mscremote" + nonce(), First: time.Now()}
		p.mu.Lock()
		p.remoteSends = append(p.remoteSends, rec)
		p.mu.Unlock()
		p.w.retry(ctx, kindMSCRemoteWrite, rec, func(ctx context.Context) error {
			eventID, err := p.remote.send(ctx, p.remoteRoom, rec.Txn, rec.Nonce)
			if err == nil {
				p.mu.Lock()
				rec.EventID = eventID
				p.mu.Unlock()
			}
			return err
		})
	}
}

// syncer long-polls a peeker's sync, keeping its token across failures.
func (p *mscProbe) syncer(ctx context.Context, c *mxClient, seen map[string]time.Time) {
	for ctx.Err() == nil {
		if err := p.syncOnce(ctx, c, seen, 10*time.Second); err != nil && ctx.Err() == nil {
			time.Sleep(500 * time.Millisecond)
		}
	}
}

func (p *mscProbe) syncOnce(ctx context.Context, c *mxClient, seen map[string]time.Time, timeout time.Duration) error {
	p.mu.Lock()
	since := p.since[c.user]
	p.mu.Unlock()
	requestCtx, cancel := context.WithTimeout(ctx, 40*time.Second)
	resp, err := c.peekSync(requestCtx, since, timeout)
	cancel()
	if err != nil {
		if ctx.Err() == nil && !retryable(err) {
			p.mu.Lock()
			p.failures = append(p.failures, fmt.Sprintf("%s: peek sync since %s: %v", c.user, since, err))
			p.mu.Unlock()
		}
		return err
	}
	now := time.Now()
	p.mu.Lock()
	for _, peeked := range resp.Rooms.Peek {
		for _, ev := range peeked.Timeline.Events {
			if _, ok := seen[ev.Content.Body]; !ok {
				seen[ev.Content.Body] = now
			}
		}
	}
	p.since[c.user] = resp.NextBatch
	p.mu.Unlock()
	for room, peeked := range resp.Rooms.Peek {
		if peeked.Timeline.Limited && since != "" {
			p.fillGap(ctx, c, room, peeked.Timeline.PrevBatch, seen)
		}
	}
	return nil
}

// fillGap pages back through a limited peek timeline, as a client does.
func (p *mscProbe) fillGap(ctx context.Context, c *mxClient, room, from string, seen map[string]time.Time) {
	for range 50 {
		events, end, err := c.page(ctx, room, from)
		if err != nil {
			p.mu.Lock()
			p.failures = append(p.failures, fmt.Sprintf("%s: filling a limited peek timeline of %s: %v", c.user, room, err))
			p.mu.Unlock()
			return
		}
		reached := false
		now := time.Now()
		p.mu.Lock()
		for _, ev := range events {
			if _, ok := seen[ev.Content.Body]; ok {
				reached = true
			} else {
				seen[ev.Content.Body] = now
			}
		}
		p.mu.Unlock()
		if reached || end == "" || len(events) == 0 {
			return
		}
		from = end
	}
}

// verifyMSCs checks that every acknowledged MSC write reached its index and
// every peeker, whatever the scenario did in between.
func verifyMSCs(ctx context.Context, r *result, p *mscProbe, settle time.Duration) {
	if p == nil {
		return
	}
	p.mu.Lock()
	replies := append([]*mscReply(nil), p.replies...)
	remoteSends := make([]sendRecord, 0, len(p.remoteSends))
	for _, s := range p.remoteSends {
		remoteSends = append(remoteSends, *s)
	}
	failures := append([]string(nil), p.failures...)
	p.mu.Unlock()
	var acked []mscReply
	for _, reply := range replies {
		p.mu.Lock()
		if reply.rec.Acked {
			acked = append(acked, mscReply{room: reply.room, rec: &sendRecord{Txn: reply.rec.Txn, Nonce: reply.rec.Nonce, EventID: reply.rec.EventID}})
		}
		p.mu.Unlock()
	}
	remoteAcked := 0
	for _, s := range remoteSends {
		if s.Acked {
			remoteAcked++
		}
	}
	r.notef("MSC probe: %d of %d thread replies and %d of %d remote peeked-room writes acknowledged", len(acked), len(replies), remoteAcked, len(remoteSends))
	if len(acked) == 0 {
		r.failf("MSC probe: no thread reply was acknowledged")
	}

	// MSC2836: an accepted reply must reach the relationship index, even when
	// its first index write failed or the owner changed before it was made.
	client := newClient(p.e.router.url()).withToken(p.w.alice)
	var missingIndex []mscReply
	awaitAll(ctx, r, settle, "acknowledged thread reply returned by /event_relationships", func() []string {
		missingIndex = nil
		var missing []string
		children := map[string]map[string]bool{}
		for _, room := range p.rooms {
			found, err := client.children(ctx, room, p.parents[room])
			if err != nil {
				return []string{err.Error()}
			}
			children[room] = found
		}
		for _, reply := range acked {
			if !children[reply.room][reply.rec.EventID] {
				missingIndex = append(missingIndex, reply)
				missing = append(missing, reply.rec.Txn)
			}
		}
		return missing
	})
	for _, reply := range missingIndex {
		diagnosis := fmt.Sprintf("thread reply %s (event %s) in %s, root %s\nrelationship edges: %s\n\n%s",
			reply.rec.Txn, reply.rec.EventID, reply.room, p.parents[reply.room],
			p.e.relationshipEdges(ctx, reply.rec.EventID), p.e.pg.diagnoseEvent(ctx, reply.rec.EventID))
		_ = os.WriteFile(filepath.Join(p.e.logs, "missing-relationship-"+reply.rec.Txn+".txt"), []byte(diagnosis), 0o644)
	}

	// MSC2753 and inbound MSC2444: every accepted reply reaches the local
	// peeker and the remote peeker, whose sync tokens predate any takeover.
	for _, target := range []struct {
		c    *mxClient
		seen map[string]time.Time
		what string
	}{
		{p.peeker, p.peekerSeen, "acknowledged reply delivered to a local peek (MSC2753)"},
		{p.remote, p.remoteSeen, "acknowledged reply delivered to a remote peek over federation (MSC2444)"},
	} {
		awaitAll(ctx, r, settle, target.what, func() []string {
			_ = p.syncOnce(ctx, target.c, target.seen, 0)
			p.mu.Lock()
			defer p.mu.Unlock()
			var missing []string
			for _, reply := range acked {
				if _, ok := target.seen[reply.rec.Nonce]; !ok {
					missing = append(missing, reply.rec.Txn)
				}
			}
			return missing
		})
	}

	// Outbound MSC2444: writes in the remote room reach the HA server's peeker.
	awaitAll(ctx, r, settle, "acknowledged remote write delivered to a peek from the HA server (MSC2444)", func() []string {
		_ = p.syncOnce(ctx, p.peeker, p.peekerSeen, 0)
		p.mu.Lock()
		defer p.mu.Unlock()
		var missing []string
		for _, s := range remoteSends {
			if _, ok := p.peekerSeen[s.Nonce]; s.Acked && !ok {
				missing = append(missing, s.Txn)
			}
		}
		return missing
	})
	for _, f := range failures {
		r.failf("MSC probe: %s", f)
	}
}

// relationshipEdges describes the relationship index rows naming child.
func (e *environment) relationshipEdges(ctx context.Context, child string) string {
	var out string
	err := e.pg.query(ctx, func(conn *pgx.Conn) error {
		rows, err := conn.Query(ctx, "SELECT parent_event_id, rel_type FROM msc2836_edges WHERE child_event_id = $1", child)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var parent, relType string
			if err = rows.Scan(&parent, &relType); err != nil {
				return err
			}
			out += fmt.Sprintf("%s (%s) ", parent, relType)
		}
		return rows.Err()
	})
	if err != nil {
		return "unavailable: " + err.Error()
	}
	if out == "" {
		return "none"
	}
	return out
}

// mscIndexFailureScenario reproduces the relationship index losing writes:
// PostgreSQL rejects every relationship index insert while replies keep being
// accepted, and the owner is frozen and replaced before the fault heals. The
// probe's verification then requires every accepted reply to be indexed.
func mscIndexFailureScenario() scenario {
	return scenario{
		name:        "msc-relationship-index-failure",
		description: "PostgreSQL rejects MSC2836 relationship index writes while thread replies are accepted, and the active candidate is frozen and replaced before the writes are allowed again.",
		tiers:       allTiers,
		run: func(ctx context.Context, s *run) error {
			p := s.w.msc
			if p == nil {
				return errors.New("the scenario needs the MSC probe; unset HACHAOS_MSCS")
			}
			s.e.timeline.add(eventFault, "postgres", "reject MSC2836 relationship index inserts")
			installed := time.Now()
			if err := s.e.pg.query(ctx, func(conn *pgx.Conn) error {
				_, err := conn.Exec(ctx, `CREATE FUNCTION hachaos_reject_msc_edge() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'hachaos: injected relationship index failure'; END $$;
CREATE TRIGGER hachaos_reject_msc_edge BEFORE INSERT ON msc2836_edges FOR EACH ROW EXECUTE FUNCTION hachaos_reject_msc_edge()`)
				return err
			}); err != nil {
				return fmt.Errorf("install the index fault: %w", err)
			}
			heal := func() error {
				return s.e.pg.query(context.WithoutCancel(ctx), func(conn *pgx.Conn) error {
					_, err := conn.Exec(context.WithoutCancel(ctx), `DROP TRIGGER IF EXISTS hachaos_reject_msc_edge ON msc2836_edges; DROP FUNCTION IF EXISTS hachaos_reject_msc_edge()`)
					return err
				})
			}
			defer func() { _ = heal() }()

			// Prove the fault is hit: a reply is accepted while its index
			// write is refused.
			if err := waitFor(ctx, time.Minute, func() error {
				p.mu.Lock()
				var accepted []string
				for _, reply := range p.replies {
					if reply.rec.Acked && reply.rec.First.After(installed) {
						accepted = append(accepted, reply.rec.EventID)
					}
				}
				p.mu.Unlock()
				for _, eventID := range accepted {
					if s.e.relationshipEdges(ctx, eventID) == "none" {
						s.r.notef("reply %s was accepted while its relationship index write was refused", eventID)
						return nil
					}
				}
				return errors.New("no accepted reply is missing from the index yet")
			}); err != nil {
				return err
			}

			leader, err := s.active()
			if err != nil {
				return err
			}
			s.faultf(leader.name, "SIGSTOP active candidate while relationship index writes fail")
			if err = s.pause(leader); err != nil {
				return err
			}
			if err = s.successor(leader, 3*s.e.timings.Lease+time.Minute); err != nil {
				_ = s.resume(leader)
				return err
			}
			// The successor inherits the refused writes and keeps failing.
			time.Sleep(5 * time.Second)
			s.e.timeline.add(eventFault, "postgres", "heal: accept MSC2836 relationship index inserts")
			if err = heal(); err != nil {
				_ = s.resume(leader)
				return fmt.Errorf("heal the index fault: %w", err)
			}
			if err = s.resume(leader); err != nil {
				return err
			}
			s.expectRefused(ctx, leader, 5*time.Second)
			return nil
		},
	}
}
