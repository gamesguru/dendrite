//go:build linux

package hachaos

import (
	"bufio"
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

// delivery traces the items the remote server sends to the HA server in
// federation transactions, from what the sender was told to what a local
// client received. It counts transaction attempts separately from distinct
// transactions and distinct items, so a burst of failed retries of one
// transaction does not read as many lost items.
type delivery struct {
	// Attempts is every request carrying a transaction, Failed those whose
	// sender was not told it was delivered, and CutOff those the gateway
	// abandoned while the HA server was still processing them.
	Attempts int `json:"txn_attempts"`
	Failed   int `json:"txn_attempts_failed"`
	CutOff   int `json:"txn_attempts_cut_off"`
	// Transactions is the number of distinct transaction IDs, Affected those
	// with at least one failed attempt, and Undelivered those never
	// acknowledged at all.
	Transactions int `json:"transactions"`
	Affected     int `json:"transactions_affected"`
	Undelivered  int `json:"transactions_undelivered"`
	// PDUErrors counts events a 200 response reported as failed.
	PDUErrors int `json:"pdu_errors"`
	// ItemsAffected counts distinct to-device messages and receipts carried
	// by at least one failed attempt.
	ItemsAffected int `json:"items_in_failed_attempts"`

	ToDeviceAcked      int `json:"to_device_acked"`
	ToDeviceMissing    int `json:"to_device_missing"`
	ToDeviceDuplicated int `json:"to_device_duplicated"`
	ReceiptsAcked      int `json:"receipts_acked"`

	// PublishFailures counts the candidates' logged publication failures by
	// message, with the distinct events or receipts they concerned.
	PublishFailures map[string]logCount `json:"publish_failures,omitempty"`
	// Metrics are increases of the degradation metrics over the scenario,
	// summed across candidate processes, and gauge maxima.
	Metrics map[string]float64 `json:"metrics,omitempty"`
}

// logCount is how often a message was logged and how many distinct items
// those lines named.
type logCount struct {
	Lines    int `json:"lines"`
	Distinct int `json:"distinct"`
}

// verifyFederatedDelivery checks the remote user's to-device messages and read
// receipts, which reach the HA server only as EDUs in federation transactions,
// and accounts for every transaction attempt.
//
// To-device messages are queued per device and must each arrive. Receipts
// coalesce: a newer receipt replaces an older one, so only the newest
// acknowledged receipt, or one still in flight when the workload stopped, has
// to be visible.
func verifyFederatedDelivery(ctx context.Context, r *result, w *workload, e *environment, settle time.Duration) {
	d := &r.Delivery
	toDevice, receipts := w.federatedSnapshot()
	missing := awaitAll(ctx, r, settle, "acknowledged to-device message from the remote server delivered", func() []string {
		w.catchUp(ctx)
		w.mu.Lock()
		defer w.mu.Unlock()
		var missing []string
		for _, s := range toDevice {
			if _, ok := w.toDeviceSeen[s.Nonce]; s.Acked && !ok {
				missing = append(missing, s.Nonce)
			}
		}
		return missing
	})
	w.mu.Lock()
	for _, s := range toDevice {
		if !s.Acked {
			continue
		}
		d.ToDeviceAcked++
		if n := w.toDeviceDeliveries[s.Nonce]; n > 1 {
			d.ToDeviceDuplicated++
		}
	}
	w.mu.Unlock()
	d.ToDeviceMissing = len(missing)
	if d.ToDeviceDuplicated > 0 {
		r.notef("%d to-device message(s) from the remote server reached Bob more than once; to-device delivery is at least once", d.ToDeviceDuplicated)
	}
	txns := e.fed.inboundTxns()
	if len(missing) > 0 {
		_ = os.WriteFile(filepath.Join(e.logs, "missing-federated-to-device.txt"), []byte(traceItems(txns, missing, func(t inboundTxn) []string { return t.ToDevice })+diagnoseToDevice(ctx, w, e, missing)), 0o644)
	}
	for _, s := range receipts {
		if s.Acked {
			d.ReceiptsAcked++
		}
	}
	verifyRemoteReceipt(ctx, r, w, e, receipts, txns, settle)
	accountTxns(d, txns)
	d.PublishFailures = countPublishFailures(e)
}

// verifyRemoteReceipt checks that the remote user's newest acknowledged read
// receipt, or one sent after it, is visible to a local member.
func verifyRemoteReceipt(ctx context.Context, r *result, w *workload, e *environment, receipts []sendRecord, txns []inboundTxn, settle time.Duration) {
	accepted := newestReceipts(receipts)
	if len(accepted) == 0 {
		r.notef("no remote read receipt was acknowledged during the scenario")
		return
	}
	alice := newClient(e.router.url()).withToken(w.alice)
	var shown string
	err := waitFor(ctx, settle, func() error {
		resp, err := alice.sync(ctx, "", 0)
		if err != nil {
			return err
		}
		shown = readReceiptOf(resp, w.room, w.remote.user)
		if slices.Contains(accepted, shown) {
			return nil
		}
		return fmt.Errorf("remote receipt for %s not visible; the server shows %q", accepted[0], shown)
	})
	if err != nil {
		r.failf("acknowledged remote read receipt: %v", err)
		var items []string
		for _, id := range accepted {
			items = append(items, w.remote.user+" "+id)
		}
		_ = os.WriteFile(filepath.Join(e.logs, "missing-federated-receipt.txt"), []byte(traceItems(txns, items, func(t inboundTxn) []string { return t.Receipts })), 0o644)
	}
}

// diagnoseToDevice follows missing to-device messages past the federation
// response: published to the broker or not, consumed by the sync API, still
// held for the device, and the to-device positions of Bob's syncs.
func diagnoseToDevice(ctx context.Context, w *workload, e *environment, missing []string) string {
	var b strings.Builder
	b.WriteString("\nto-device publications of the missing messages (at, nonce, sender, message ID):\n")
	for _, p := range e.nats.toDevicePublications() {
		if slices.Contains(missing, p.Nonce) {
			fmt.Fprintf(&b, "  %s %s %s %s\n", p.At.Format(time.RFC3339Nano), p.Nonce, p.Sender, p.MsgID)
		}
	}
	b.WriteString("\n" + e.nats.consumerState("OutputSendToDeviceEvent", "SyncAPISendToDeviceConsumerPull") + "\n\n")
	b.WriteString(e.pg.toDeviceState(ctx, w.bob.user, w.bob.device, missing))
	b.WriteString("\n/sync requests by " + w.bob.user + " (start, end, since, next_batch):\n")
	for _, t := range w.syncTraces() {
		if t.User == w.bob.user {
			fmt.Fprintf(&b, "  %s %s %s %s to-device %v\n", t.Start.Format("15:04:05.000"), t.End.Format("15:04:05.000"), t.Since, t.NextBatch, t.ToDevice)
		}
	}
	return b.String()
}

// readReceiptOf returns the event a user's m.read receipt points at.
func readReceiptOf(resp *syncResponse, room, user string) string {
	shown := ""
	for _, ev := range resp.Rooms.Join[room].Ephemeral.Events {
		if ev.Type != "m.receipt" {
			continue
		}
		for eventID, byType := range ev.Content {
			if _, ok := byType["m.read"][user]; ok {
				shown = eventID
			}
		}
	}
	return shown
}

// traceItems lists every transaction attempt that carried one of the items,
// so a missing item can be attributed: never sent, sent but refused, or
// acknowledged by the HA server and then lost.
func traceItems(txns []inboundTxn, items []string, carried func(inboundTxn) []string) string {
	var b strings.Builder
	for _, item := range items {
		fmt.Fprintf(&b, "%s:\n", item)
		found := false
		for _, t := range txns {
			if !slices.Contains(carried(t), item) {
				continue
			}
			found = true
			fmt.Fprintf(&b, "  txn %s %s-%s status %d cut_off=%t pdus=%d edus=%v\n", t.TxnID,
				t.At.Format("15:04:05.000"), t.End.Format("15:04:05.000"), t.Status, t.CutOff, t.PDUs, t.EDUs)
		}
		if !found {
			b.WriteString("  carried by no transaction the remote server sent\n")
		}
	}
	return b.String()
}

// accountTxns counts transaction attempts, distinct transactions and the
// distinct items in failed attempts.
func accountTxns(d *delivery, txns []inboundTxn) {
	type state struct{ failed, delivered bool }
	byID := map[string]*state{}
	affected := map[string]bool{}
	for _, t := range txns {
		d.Attempts++
		st := byID[t.TxnID]
		if st == nil {
			st = &state{}
			byID[t.TxnID] = st
		}
		d.PDUErrors += len(t.PDUErrors)
		if t.CutOff {
			d.CutOff++
		}
		if t.ok() {
			st.delivered = true
			continue
		}
		d.Failed++
		st.failed = true
		for _, item := range append(slices.Clone(t.ToDevice), t.Receipts...) {
			affected[item] = true
		}
	}
	d.Transactions = len(byID)
	for _, st := range byID {
		if st.failed {
			d.Affected++
		}
		if !st.delivered {
			d.Undelivered++
		}
	}
	d.ItemsAffected = len(affected)
}

// publishFailureLogs are the candidates' log messages for a publication that
// failed, and the field naming the item each concerned.
var publishFailureLogs = []struct{ msg, field string }{
	{"Failed to send receipt event to JetStream", "events"},
	{"Failed to send send-to-device event to JetStream", "user_id"},
	{"Roomserver failed to queue async event", "event_id"},
	{"Refusing federation transaction for a retry", "txn_id"},
}

var logField = regexp.MustCompile(`(\w+)=("(?:[^"\\]|\\.)*"|\S+)`)

// countPublishFailures reads every candidate's log for failed publications.
func countPublishFailures(e *environment) map[string]logCount {
	lines := map[string]int{}
	distinct := map[string]map[string]bool{}
	for _, sl := range e.slots {
		f, err := os.Open(filepath.Join(e.logs, sl.name+".log"))
		if err != nil {
			continue
		}
		scanner := bufio.NewScanner(f)
		scanner.Buffer(make([]byte, 1<<20), 1<<20)
		for scanner.Scan() {
			line := scanner.Text()
			for _, p := range publishFailureLogs {
				if !strings.Contains(line, p.msg) {
					continue
				}
				lines[p.msg]++
				if distinct[p.msg] == nil {
					distinct[p.msg] = map[string]bool{}
				}
				for _, m := range logField.FindAllStringSubmatch(line, -1) {
					if m[1] == p.field {
						distinct[p.msg][strings.Trim(m[2], `"`)] = true
					}
				}
			}
		}
		_ = f.Close()
	}
	out := map[string]logCount{}
	for msg, n := range lines {
		out[msg] = logCount{Lines: n, Distinct: len(distinct[msg])}
	}
	return out
}

// noteDelivery summarizes the federated delivery accounting.
func noteDelivery(r *result) {
	d := r.Delivery
	r.notef("inbound federation: %d transaction attempt(s) for %d transaction(s); %d attempt(s) failed (%d cut off by the sender), affecting %d transaction(s) and %d to-device message(s) or receipt(s); %d transaction(s) never acknowledged",
		d.Attempts, d.Transactions, d.Failed, d.CutOff, d.Affected, d.ItemsAffected, d.Undelivered)
	var msgs []string
	for msg := range d.PublishFailures {
		msgs = append(msgs, msg)
	}
	sort.Strings(msgs)
	for _, msg := range msgs {
		c := d.PublishFailures[msg]
		r.notef("candidate logs: %q %d time(s), for %d distinct item(s)", msg, c.Lines, c.Distinct)
	}
	if len(d.Metrics) > 0 {
		var keys []string
		for k := range d.Metrics {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		var parts []string
		for _, k := range keys {
			parts = append(parts, fmt.Sprintf("%s %g", k, d.Metrics[k]))
		}
		r.notef("degradation metrics: %s", strings.Join(parts, ", "))
	}
}

// degradationMetrics are the metric families sampled during a scenario.
// Counters report their increase, gauges their maximum.
var degradationMetrics = map[string]bool{
	"zendrite_jetstream_publish_retries_total":       true,
	"zendrite_jetstream_publish_failures_total":      true,
	"zendrite_jetstream_consumer_fetch_errors_total": true,
	"zendrite_jetstream_consumer_redeliveries_total": true,
	"zendrite_federationapi_recv_transactions_total": true,
	"zendrite_jetstream_consumer_pending_messages":   false,
}

// metricsSampler scrapes the serving candidates' metrics while a scenario
// runs. Counters restart with each process, so increases are tracked per
// process and summed.
type metricsSampler struct {
	e       *environment
	stopped chan struct{}
	done    chan struct{}

	once  sync.Once
	out   map[string]float64
	mu    sync.Mutex
	first map[string]float64
	last  map[string]float64
	max   map[string]float64
}

func startMetricsSampler(e *environment) *metricsSampler {
	m := &metricsSampler{e: e, stopped: make(chan struct{}), done: make(chan struct{}), first: map[string]float64{}, last: map[string]float64{}, max: map[string]float64{}}
	go m.run()
	return m
}

func (m *metricsSampler) run() {
	defer close(m.done)
	client := &http.Client{Timeout: 2 * time.Second}
	for {
		for _, sl := range m.e.slots {
			proc := sl.current()
			if proc == nil {
				continue
			}
			m.scrape(client, sl, proc.pid())
		}
		select {
		case <-m.stopped:
			return
		case <-time.After(2 * time.Second):
		}
	}
}

func (m *metricsSampler) scrape(client *http.Client, sl *slot, pid int) {
	req, err := http.NewRequest(http.MethodGet, sl.httpURL()+"/metrics", nil)
	if err != nil {
		return
	}
	req.SetBasicAuth("metrics", "metrics")
	resp, err := client.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return
	}
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 1<<20), 1<<20)
	m.mu.Lock()
	defer m.mu.Unlock()
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "#") {
			continue
		}
		series, value, ok := strings.Cut(line, " ")
		if !ok {
			continue
		}
		family, _, _ := strings.Cut(series, "{")
		if _, sampled := degradationMetrics[family]; !sampled {
			continue
		}
		v, err := strconv.ParseFloat(strings.TrimSpace(value), 64)
		if err != nil {
			continue
		}
		key := fmt.Sprintf("%d %s", pid, series)
		if _, seen := m.first[key]; !seen {
			m.first[key] = v
		}
		m.last[key] = v
		if v > m.max[key] {
			m.max[key] = v
		}
	}
}

// stop ends sampling and reports counter increases and gauge maxima per
// series, summed or maximized across processes.
func (m *metricsSampler) stop() map[string]float64 {
	m.once.Do(func() {
		close(m.stopped)
		<-m.done
		m.out = m.totals()
	})
	return m.out
}

func (m *metricsSampler) totals() map[string]float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := map[string]float64{}
	for key, last := range m.last {
		_, series, _ := strings.Cut(key, " ")
		family, _, _ := strings.Cut(series, "{")
		if degradationMetrics[family] {
			if inc := last - m.first[key]; inc > 0 {
				out[series] += inc
			}
			continue
		}
		if mx := m.max[key]; mx > out[series] {
			out[series] = mx
		}
	}
	return out
}

// brokerCaughtUp reports whether every stream and durable consumer has a
// leader and every replica is current, which a broker that rejoined after an
// absence must reach before the cluster tolerates another loss.
func (c *natsCluster) brokerCaughtUp() error {
	nc, err := nats.Connect(c.directURLs(), nats.Timeout(2*time.Second), nats.Name(harnessApp))
	if err != nil {
		return err
	}
	defer nc.Close()
	js, err := nc.JetStream(nats.MaxWait(5 * time.Second))
	if err != nil {
		return err
	}
	var lagging []string
	check := func(what string, cluster *nats.ClusterInfo) {
		switch {
		case cluster == nil:
			lagging = append(lagging, what+" has no cluster information")
		case cluster.Leader == "":
			lagging = append(lagging, what+" has no leader")
		case len(cluster.Replicas) != natsNodes-1:
			lagging = append(lagging, fmt.Sprintf("%s has %d followers", what, len(cluster.Replicas)))
		default:
			for _, peer := range cluster.Replicas {
				if !peer.Current || peer.Offline {
					lagging = append(lagging, fmt.Sprintf("%s replica %s current=%t offline=%t lag=%d", what, peer.Name, peer.Current, peer.Offline, peer.Lag))
				}
			}
		}
	}
	streams := 0
	for info := range js.StreamsInfo() {
		if !strings.HasPrefix(info.Config.Name, topicPrefix) {
			continue
		}
		streams++
		check("stream "+info.Config.Name, info.Cluster)
		for ci := range js.ConsumersInfo(info.Config.Name) {
			if ci.Config.Durable == "" {
				continue
			}
			check("consumer "+info.Config.Name+"/"+ci.Name, ci.Cluster)
		}
	}
	if streams == 0 {
		return fmt.Errorf("no %s stream is listed", topicPrefix)
	}
	if len(lagging) > 0 {
		return fmt.Errorf("%d not caught up, e.g. %s", len(lagging), strings.Join(lagging[:min(3, len(lagging))], "; "))
	}
	return nil
}

// verifyBrokerCatchUp waits for every replica to be current after a broker
// rejoined and records how long that took from the rejoin.
func verifyBrokerCatchUp(ctx context.Context, r *result, e *environment, rejoined time.Time, timeout time.Duration) {
	err := waitFor(ctx, timeout, e.nats.brokerCaughtUp)
	if err != nil {
		r.failf("broker replicas did not catch up within %s of the rejoin: %v", timeout, err)
		return
	}
	r.Recovery.BrokerCatchUp = durationPtr(time.Since(rejoined))
	r.notef("every stream and durable consumer replica was current %s after the broker rejoined", r.Recovery.BrokerCatchUp.Round(time.Millisecond))
}

func TestAccountTxns(t *testing.T) {
	txns := []inboundTxn{
		{TxnID: "1", Status: 502, CutOff: true, ToDevice: []string{"a"}, Receipts: []string{"@r $1"}},
		{TxnID: "1", Status: 503, ToDevice: []string{"a"}, Receipts: []string{"@r $1"}},
		{TxnID: "1", Status: 200, ToDevice: []string{"a"}, Receipts: []string{"@r $1"}},
		{TxnID: "2", Status: 200, PDUErrors: map[string]string{"$e": "boom"}},
		{TxnID: "3", Status: 502, ToDevice: []string{"b"}},
	}
	var d delivery
	accountTxns(&d, txns)
	want := delivery{Attempts: 5, Failed: 3, CutOff: 1, Transactions: 3, Affected: 2, Undelivered: 1, PDUErrors: 1, ItemsAffected: 3}
	if d.Attempts != want.Attempts || d.Failed != want.Failed || d.CutOff != want.CutOff || d.Transactions != want.Transactions ||
		d.Affected != want.Affected || d.Undelivered != want.Undelivered || d.PDUErrors != want.PDUErrors || d.ItemsAffected != want.ItemsAffected {
		t.Fatalf("accountTxns = %+v, want %+v", d, want)
	}
}

func TestParseInboundTxn(t *testing.T) {
	body := `{"pdus":[{}],"edus":[
		{"edu_type":"m.receipt","content":{"!r:x":{"m.read":{"@rita:remote":{"event_ids":["$e"],"data":{"ts":1}}}}}},
		{"edu_type":"m.direct_to_device","content":{"sender":"@rita:remote","type":"t","message_id":"m","messages":{"@bob:ha":{"DEV":{"nonce":"n1"}}}}},
		{"edu_type":"m.typing","content":{}}]}`
	got := parseInboundTxn(sendPath+"17-3", []byte(body))
	if got.TxnID != "17-3" || got.PDUs != 1 || got.EDUs["m.typing"] != 1 ||
		!slices.Equal(got.Receipts, []string{"@rita:remote $e"}) || !slices.Equal(got.ToDevice, []string{"n1"}) {
		t.Fatalf("parseInboundTxn = %+v", got)
	}
	if errs := pduErrors([]byte(`{"pdus":{"$a":{},"$b":{"error":"x"}}}`)); len(errs) != 1 || errs["$b"] != "x" {
		t.Fatalf("pduErrors = %v", errs)
	}
}
