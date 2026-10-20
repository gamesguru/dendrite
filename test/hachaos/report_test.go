//go:build linux

package hachaos

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"
)

// writeReport records the evidence a release decision needs: configuration,
// measured recovery per scenario against the objective, failures, and what
// the run could not cover.
func writeReport(s settings, results []result) error {
	body, err := json.MarshalIndent(map[string]any{"tier": s.tier, "timings": s.timings, "target": s.target, "results": results}, "", "  ")
	if err != nil {
		return err
	}
	if err = os.WriteFile(filepath.Join(s.artifacts, "results.json"), body, 0o644); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(s.artifacts, "report.md"), []byte(renderReport(s, results)), 0o644)
}

func seconds(d *time.Duration) string {
	if d == nil {
		return "n/a"
	}
	return fmt.Sprintf("%.1f s", d.Seconds())
}

func renderReport(s settings, results []result) string {
	var b strings.Builder
	passed := 0
	for _, r := range results {
		if r.Passed {
			passed++
		}
	}
	fmt.Fprintf(&b, "# HA qualification report\n\n")
	fmt.Fprintf(&b, "Tier `%s`, %d of %d scenarios passed, generated %s.\n\n", s.tier, passed, len(results), time.Now().UTC().Format(time.RFC3339))
	fmt.Fprintf(&b, "## Configuration\n\n")
	fmt.Fprintf(&b, "| Setting | Value |\n| --- | --- |\n")
	fmt.Fprintf(&b, "| Election timings | lease %s, renew %s, retry %s, ownership %s |\n", s.timings.Lease, s.timings.Renew, s.timings.Retry, s.timings.Ownership)
	fmt.Fprintf(&b, "| Recovery objective | %s from fault to successful client and federation requests |\n", s.target)
	fmt.Fprintf(&b, "| Candidates | %d, one routing layer checking the active-only endpoint every %s |\n", s.candidates, routerCheckInterval)
	fmt.Fprintf(&b, "| PostgreSQL | primary with one synchronous standby, `synchronous_commit=on`, promotion behind a stable address |\n")
	fmt.Fprintf(&b, "| JetStream | %d nodes, file storage, `sync_interval: always`, replicas 3 |\n", natsNodes)
	fmt.Fprintf(&b, "| Media | S3 API (gofakes3, directory-backed) |\n")
	fmt.Fprintf(&b, "| Kubernetes | kube-apiserver and etcd from envtest, RBAC limited to Lease get, create, update |\n")
	fmt.Fprintf(&b, "| Federation | standalone homeserver joined to the test room |\n")
	fmt.Fprintf(&b, "| Client workload | %.0f messages/s with retried transaction IDs, long-poll sync, to-device, receipts, media, search |\n", s.rate)
	if s.seedRooms > 0 {
		fmt.Fprintf(&b, "| Seeded state | %d users, %d rooms |\n", s.seedUsers, s.seedRooms)
	}
	if s.searchHistory > 0 {
		fmt.Fprintf(&b, "| Search history | %d messages |\n", s.searchHistory)
	}
	fmt.Fprintf(&b, "| Host | %s/%s, %d CPUs |\n\n", runtime.GOOS, runtime.GOARCH, runtime.NumCPU())

	fmt.Fprintf(&b, "## Recovery\n\n")
	fmt.Fprintf(&b, "Times run from fault injection to the first success after the first failure the fault caused, or to the first success when nothing failed.\n")
	fmt.Fprintf(&b, "Scenarios marked *suspends* stop serving by design while the fault lasts, so the objective does not apply to them.\n")
	fmt.Fprintf(&b, "Scenarios marked *no failed writes* delayed some requests but failed none.\n\n")
	fmt.Fprintf(&b, "| Scenario | Result | Client write | Sync | Federation in | Federation out | Generations |\n| --- | --- | --- | --- | --- | --- | --- |\n")
	for _, r := range results {
		status := "pass"
		if !r.Passed {
			status = "**fail**"
		}
		if r.Recovery.SuspendsObjective {
			status += ", suspends"
		}
		if !r.Recovery.Disrupted {
			status += ", no failed writes"
		}
		fmt.Fprintf(&b, "| %s | %s | %s | %s | %s | %s | %d |\n", r.Name, status, seconds(r.Recovery.ClientWrite), seconds(r.Recovery.Sync), seconds(r.Recovery.FederationInbound), seconds(r.Recovery.FederationOutbound), len(r.Generations))
	}
	fmt.Fprintf(&b, "\n## Durability and idempotency\n\n")
	fmt.Fprintf(&b, "| Scenario | Acked writes | Lost | Stored, not in /messages | Duplicated | Duplicated across takeover | Retried | Unacked | Unacked but committed | Remote writes | To-device | HTTP 503 |\n| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |\n")
	for _, r := range results {
		c := r.Counts
		fmt.Fprintf(&b, "| %s | %d | %d | %d | %d | %d | %d | %d | %d | %d | %d | %d |\n", r.Name, c.Acked, c.Lost, c.Unserved, c.Duplicated, c.DuplicatedAcrossTakeover, c.Retried, c.Unacked, c.UnackedCommitted, c.RemoteAcked, c.ToDeviceAcked, c.Unavailable)
	}
	fmt.Fprintf(&b, "\n## Federated delivery\n\n")
	fmt.Fprintf(&b, "Transactions the remote server sent to the HA server, carrying the remote user's messages, to-device messages and read receipts.\n")
	fmt.Fprintf(&b, "Attempts count every request, including retries; transactions count distinct transaction IDs; items count distinct to-device messages and receipts carried by a failed attempt.\n\n")
	fmt.Fprintf(&b, "| Scenario | Attempts | Failed attempts | Cut off | Transactions | Affected | Never acknowledged | Items in failed attempts | PDU errors | To-device acked | Missing | Duplicated | Receipts acked | Broker catch-up |\n| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |\n")
	for _, r := range results {
		d := r.Delivery
		fmt.Fprintf(&b, "| %s | %d | %d | %d | %d | %d | %d | %d | %d | %d | %d | %d | %d | %s |\n", r.Name, d.Attempts, d.Failed, d.CutOff, d.Transactions, d.Affected, d.Undelivered, d.ItemsAffected, d.PDUErrors, d.ToDeviceAcked, d.ToDeviceMissing, d.ToDeviceDuplicated, d.ReceiptsAcked, seconds(r.Recovery.BrokerCatchUp))
	}
	renderUpgrades(&b, results)
	fmt.Fprintf(&b, "\n## Ownership boundaries\n\n")
	fmt.Fprintf(&b, "| Scenario | Audited commits | Broker publications | Retired-generation publications | Media writes | Federation requests |\n| --- | --- | --- | --- | --- | --- |\n")
	for _, r := range results {
		c := r.Counts
		fmt.Fprintf(&b, "| %s | %d | %d | %d | %d | %d |\n", r.Name, c.AuditedCommits, c.BrokerMessages, c.LateBroker, c.MediaWrites, c.FederationSends)
	}
	fmt.Fprintf(&b, "\n## Scenarios\n")
	for _, r := range results {
		fmt.Fprintf(&b, "\n### %s\n\n%s\n\n", r.Name, r.Description)
		fmt.Fprintf(&b, "Duration %s.\n", r.Duration.Round(time.Second))
		for _, f := range r.Failures {
			fmt.Fprintf(&b, "\n- **Failure:** %s", f)
		}
		for _, n := range r.Notes {
			fmt.Fprintf(&b, "\n- %s", n)
		}
		if len(r.Failures)+len(r.Notes) > 0 {
			b.WriteString("\n")
		}
	}
	fmt.Fprintf(&b, "\n## Limitations of this run\n\n")
	for _, l := range []string{
		"Every process shares one host and one clock, so network partitions are emulated by proxies rather than by packet loss between machines.",
		"The routing layer is the harness's own implementation of the chart's HAProxy check, not HAProxy itself.",
		"The S3 server is gofakes3, not a replicated object store; S3 failover itself is not exercised.",
		"The PostgreSQL operator is emulated: promotion is triggered by the harness, and synchronous commit is relaxed while no standby exists.",
		"Infrastructure power loss and correlated storage failure are outside the scope of a process-level harness.",
	} {
		fmt.Fprintf(&b, "- %s\n", l)
	}
	return b.String()
}

// seedReference creates the ADR reference state: users, and rooms that
// each of them has joined, so promotion has realistic state to recover.
func seedReference(ctx context.Context, e *environment, users, rooms int) error {
	e.timeline.add(eventNote, "seed", "creating %d users and %d rooms", users, rooms)
	clients := make([]*mxClient, users)
	for i := range users {
		c, err := register(ctx, e.router.url(), fmt.Sprintf("seed%03d", i))
		if err != nil {
			return err
		}
		clients[i] = c
	}
	const workers = 8
	jobs := make(chan int)
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				owner := clients[i%users]
				room, err := owner.createRoom(ctx, fmt.Sprintf("seed-%d", i))
				if err != nil {
					errs <- err
					return
				}
				// Two more members per room keep membership realistic.
				for _, j := range []int{(i + 1) % users, (i + 7) % users} {
					if err = clients[j].join(ctx, room, ""); err != nil {
						errs <- err
						return
					}
				}
				if _, err = owner.send(ctx, room, "seed-"+nonce(), "seed message "+nonce()); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	var err error
	for i := range rooms {
		select {
		case jobs <- i:
			continue
		case err = <-errs:
		}
		break
	}
	close(jobs)
	wg.Wait()
	if err == nil {
		select {
		case err = <-errs:
		default:
		}
	}
	e.timeline.add(eventNote, "seed", "done: %v", err)
	return err
}

// seedSearchHistory sends n searchable messages before the fault. A
// successor rebuilds its search index from all of them, which took about 32
// seconds for 84,000 messages in production and once held every client
// request back until it finished (#451).
func seedSearchHistory(ctx context.Context, e *environment, n int) error {
	e.timeline.add(eventNote, "seed", "sending %d searchable messages", n)
	started := time.Now()
	c, err := registerRetrying(ctx, e.router.url(), "historian")
	if err != nil {
		return err
	}
	const rooms, workers = 16, 16
	roomIDs := make([]string, rooms)
	for i := range roomIDs {
		if err = retryingSetup(ctx, func() error {
			var createErr error
			roomIDs[i], createErr = c.patient().createRoom(ctx, fmt.Sprintf("history-%d", i))
			return createErr
		}); err != nil {
			return err
		}
	}
	words := strings.Fields("zendrite search history rebuild takeover successor index bleve analyzer token stream position redaction edit federation room member topic name message body activation standby lease renewal owner generation fencing")
	jobs := make(chan int)
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				// Production messages average about 1 KB of event JSON.
				var body strings.Builder
				fmt.Fprintf(&body, "history %d", i)
				for j := range 60 {
					fmt.Fprintf(&body, " %s%d", words[(i+j*7)%len(words)], (i*31+j)%1009)
				}
				if _, sendErr := c.send(ctx, roomIDs[i%rooms], fmt.Sprintf("history-%d", i), body.String()); sendErr != nil {
					errs <- sendErr
					return
				}
			}
		}()
	}
	for i := range n {
		select {
		case jobs <- i:
			continue
		case err = <-errs:
		}
		break
	}
	close(jobs)
	wg.Wait()
	if err == nil {
		select {
		case err = <-errs:
		default:
		}
	}
	if err == nil {
		// The sync API indexes the room output stream in order, so once a
		// marker sent after every other message is searchable, all of them
		// are. A fault before then would add the sync backlog to takeover.
		marker := "historymarker" + nonce()
		if _, err = c.send(ctx, roomIDs[0], "history-marker", marker); err == nil {
			err = waitFor(ctx, 30*time.Minute, func() error {
				ids, searchErr := c.search(ctx, marker)
				if searchErr != nil {
					return searchErr
				}
				if len(ids) == 0 {
					return fmt.Errorf("the sync API has not indexed the search history yet")
				}
				return nil
			})
		}
	}
	e.timeline.add(eventNote, "seed", "search history done in %s: %v", time.Since(started).Round(time.Second), err)
	return err
}

// renderUpgrades reports the planned operations, whose claim is that no
// request failed rather than that service recovered.
func renderUpgrades(b *strings.Builder, results []result) {
	var planned []result
	for _, r := range results {
		if r.Upgrade != nil {
			planned = append(planned, r)
		}
	}
	if len(planned) == 0 {
		return
	}
	fmt.Fprintf(b, "\n## Planned upgrades and restarts\n\n")
	fmt.Fprintf(b, "Requests that overlapped the operation, from the first SIGTERM until every candidate was replaced and prepared.\n")
	fmt.Fprintf(b, "Failed requests fail the scenario; retries do not count as uninterrupted service.\n")
	fmt.Fprintf(b, "Search answering 503 while the successor rebuilds its index is the documented exception.\n\n")
	fmt.Fprintf(b, "| Scenario | Window | Requests | Failed | Write latency max | Write latency p99 | Outbound federation retried | Search 503 | Longest search outage |\n| --- | --- | --- | --- | --- | --- | --- | --- | --- |\n")
	for _, r := range planned {
		u := r.Upgrade
		fmt.Fprintf(b, "| %s | %.1f s | %d | %d | %.2f s | %.2f s | %d | %d of %d | %.1f s |\n", r.Name, u.Window.Seconds(), sum(u.Requests), sum(u.Failed),
			u.MaxLatency[kindWrite].Seconds(), u.P99Latency[kindWrite].Seconds(), u.OutboundFailed, u.Search.Unavailable, u.Search.Requests, u.Search.Longest.Seconds())
	}
}
