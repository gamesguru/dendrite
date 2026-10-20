//go:build linux

package hachaos

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

// natsCluster is a three-node JetStream cluster. Each node sits behind its own
// fault proxy, and nodes do not advertise their addresses to clients, so a
// candidate can only reach the broker through the proxies it was configured
// with.
type natsCluster struct {
	bin      string
	logs     string
	timeline *timeline
	nodes    []*natsNode

	mu       sync.Mutex
	effects  []brokerEffect
	receipts []receiptPublication
	toDevice []toDevicePublication
	watcher  *nats.Conn
}

type natsNode struct {
	name    string
	config  string
	store   string
	port    int
	monitor int
	proxy   *proxy
	proc    *process
}

// brokerEffect is one publication stamped with an ownership generation.
type brokerEffect struct {
	At         time.Time
	Subject    string
	Generation int64
}

// receiptPublication is one read receipt published to the receipt stream,
// whichever server it came from.
type receiptPublication struct {
	At         time.Time
	User       string
	EventID    string
	Generation string
}

// toDevicePublication is one to-device message published for the sync API,
// identified by the workload nonce in its content.
type toDevicePublication struct {
	At     time.Time
	Nonce  string
	Sender string
	MsgID  string
}

const (
	natsNodes     = 3
	topicPrefix   = "HAChaos"
	generationH   = "Zendrite-HA-Generation"
	receiptTopic  = topicPrefix + "OutputReceiptEvent"
	toDeviceTopic = topicPrefix + "OutputSendToDeviceEvent"
)

const natsConfig = `
server_name: %q
listen: "127.0.0.1:%d"
http: "127.0.0.1:%d"
jetstream {
	store_dir: %q
	sync_interval: always
	max_file_store: 2G
	max_memory_store: 128M
}
cluster {
	name: "hachaos"
	listen: "127.0.0.1:%d"
	routes: [%s]
	no_advertise: true
}
`

func startNATS(ctx context.Context, dir, logs, bin string, o *owners, tl *timeline) (*natsCluster, error) {
	c := &natsCluster{bin: bin, logs: logs, timeline: tl}
	clusterPorts := make([]int, natsNodes)
	for i := range natsNodes {
		node := &natsNode{name: fmt.Sprintf("nats-%d", i)}
		var err error
		if node.port, err = freePort(); err != nil {
			return nil, err
		}
		if node.monitor, err = freePort(); err != nil {
			return nil, err
		}
		if clusterPorts[i], err = freePort(); err != nil {
			return nil, err
		}
		port := node.port
		if node.proxy, err = newProxy(node.name, func() string { return fmt.Sprintf("127.0.0.1:%d", port) }, o, tl); err != nil {
			return nil, err
		}
		c.nodes = append(c.nodes, node)
	}
	routes := make([]string, 0, natsNodes)
	for _, p := range clusterPorts {
		routes = append(routes, fmt.Sprintf("%q", fmt.Sprintf("nats-route://127.0.0.1:%d", p)))
	}
	for i, node := range c.nodes {
		store := filepath.Join(dir, node.name)
		if err := os.MkdirAll(store, 0o755); err != nil {
			return nil, err
		}
		node.store = store
		node.config = filepath.Join(dir, node.name+".conf")
		body := fmt.Sprintf(natsConfig, node.name, node.port, node.monitor, store, clusterPorts[i], strings.Join(routes, ", "))
		if err := os.WriteFile(node.config, []byte(body), 0o644); err != nil {
			return nil, err
		}
	}
	for _, node := range c.nodes {
		if err := c.start(node); err != nil {
			return nil, err
		}
	}
	if err := waitFor(ctx, 2*time.Minute, c.jetStreamReady); err != nil {
		return nil, fmt.Errorf("JetStream cluster: %w", err)
	}
	if err := c.watch(); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *natsCluster) start(node *natsNode) error {
	proc, err := startProcess(c.logs, processSpec{name: node.name, bin: c.bin, env: []string{roleEnv + "=nats", "HACHAOS_CONFIG=" + node.config}})
	if err != nil {
		return err
	}
	node.proc = proc
	c.timeline.add(eventProc, node.name, "started")
	return nil
}

// restart starts a node that was stopped or killed, with its stored state.
func (c *natsCluster) restart(ctx context.Context, node *natsNode) error {
	node.proc.kill()
	if err := c.start(node); err != nil {
		return err
	}
	return waitFor(ctx, 2*time.Minute, c.jetStreamReady)
}

// directURLs reach the brokers without the fault proxies, for the harness.
func (c *natsCluster) directURLs() string {
	urls := make([]string, 0, len(c.nodes))
	for _, node := range c.nodes {
		urls = append(urls, fmt.Sprintf("nats://127.0.0.1:%d", node.port))
	}
	return strings.Join(urls, ",")
}

// candidateAddresses are the proxied addresses candidates are configured with.
func (c *natsCluster) candidateAddresses() []string {
	out := make([]string, 0, len(c.nodes))
	for _, node := range c.nodes {
		out = append(out, "nats://"+node.proxy.addr())
	}
	return out
}

func (c *natsCluster) jetStreamReady() error {
	nc, err := nats.Connect(c.directURLs(), nats.Timeout(2*time.Second), nats.Name(harnessApp))
	if err != nil {
		return err
	}
	defer nc.Close()
	js, err := nc.JetStream(nats.MaxWait(2 * time.Second))
	if err != nil {
		return err
	}
	_, err = js.AccountInfo()
	return err
}

// watch records every publication that carries an ownership generation. Core
// subscribers see JetStream publications as they arrive, including ones that
// interest-retention streams delete after acknowledgement, so this is a
// complete record of broker effects rather than a view of what remains.
func (c *natsCluster) watch() error {
	nc, err := nats.Connect(c.directURLs(), nats.Name(harnessApp+"-watch"), nats.MaxReconnects(-1), nats.ReconnectWait(200*time.Millisecond))
	if err != nil {
		return err
	}
	_, err = nc.Subscribe(">", func(msg *nats.Msg) {
		// Consumer deliveries also match, carrying the original subject and
		// headers; only publications are effects of the publisher.
		if strings.HasPrefix(msg.Reply, "$JS.ACK.") {
			return
		}
		if msg.Subject == toDeviceTopic {
			var out struct {
				Sender  string `json:"sender"`
				Content struct {
					Nonce string `json:"nonce"`
				} `json:"content"`
			}
			if json.Unmarshal(msg.Data, &out) == nil && out.Content.Nonce != "" {
				c.mu.Lock()
				c.toDevice = append(c.toDevice, toDevicePublication{At: time.Now(), Nonce: out.Content.Nonce, Sender: out.Sender, MsgID: msg.Header.Get(nats.MsgIdHdr)})
				c.mu.Unlock()
			}
		}
		if msg.Subject == receiptTopic {
			c.mu.Lock()
			c.receipts = append(c.receipts, receiptPublication{
				At: time.Now(), User: msg.Header.Get("user_id"), EventID: msg.Header.Get("event_id"), Generation: msg.Header.Get(generationH),
			})
			c.mu.Unlock()
		}
		value := msg.Header.Get(generationH)
		if value == "" {
			return
		}
		generation, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return
		}
		c.mu.Lock()
		c.effects = append(c.effects, brokerEffect{At: time.Now(), Subject: msg.Subject, Generation: generation})
		c.mu.Unlock()
	})
	if err != nil {
		nc.Close()
		return err
	}
	c.watcher = nc
	return nil
}

func (c *natsCluster) publications() []brokerEffect {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]brokerEffect(nil), c.effects...)
}

func (c *natsCluster) toDevicePublications() []toDevicePublication {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]toDevicePublication(nil), c.toDevice...)
}

func (c *natsCluster) receiptPublications() []receiptPublication {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]receiptPublication(nil), c.receipts...)
}

// consumerState describes a durable consumer's progress through its stream,
// which separates a message that was never delivered from one delivered and
// not acknowledged.
func (c *natsCluster) consumerState(stream, durable string) string {
	nc, err := nats.Connect(c.directURLs(), nats.Timeout(2*time.Second), nats.Name(harnessApp))
	if err != nil {
		return "unknown: " + err.Error()
	}
	defer nc.Close()
	js, err := nc.JetStream(nats.MaxWait(5 * time.Second))
	if err != nil {
		return "unknown: " + err.Error()
	}
	s, err := js.StreamInfo(topicPrefix + stream)
	if err != nil {
		return "unknown: " + err.Error()
	}
	info, err := js.ConsumerInfo(topicPrefix+stream, topicPrefix+durable)
	if err != nil {
		return "unknown: " + err.Error()
	}
	return fmt.Sprintf("stream %s: messages=%d first_seq=%d last_seq=%d\n"+
		"consumer %s: delivered_stream_seq=%d ack_floor_stream_seq=%d num_pending=%d num_ack_pending=%d num_redelivered=%d num_waiting=%d",
		s.Config.Name, s.State.Msgs, s.State.FirstSeq, s.State.LastSeq,
		info.Name, info.Delivered.Stream, info.AckFloor.Stream, info.NumPending, info.NumAckPending, info.NumRedelivered, info.NumWaiting)
}

// streamConsumers describes a stream and every consumer on it, including
// ephemeral ones, which shows whether a candidate's stream-wide subscription
// still exists.
func (c *natsCluster) streamConsumers(stream string) string {
	nc, err := nats.Connect(c.directURLs(), nats.Timeout(2*time.Second), nats.Name(harnessApp))
	if err != nil {
		return "unknown: " + err.Error()
	}
	defer nc.Close()
	js, err := nc.JetStream(nats.MaxWait(5 * time.Second))
	if err != nil {
		return "unknown: " + err.Error()
	}
	s, err := js.StreamInfo(topicPrefix + stream)
	if err != nil {
		return "unknown: " + err.Error()
	}
	var b strings.Builder
	fmt.Fprintf(&b, "stream %s: messages=%d first_seq=%d last_seq=%d consumers=%d\n",
		s.Config.Name, s.State.Msgs, s.State.FirstSeq, s.State.LastSeq, s.State.Consumers)
	for info := range js.Consumers(topicPrefix + stream) {
		fmt.Fprintf(&b, "consumer %s: durable=%q filter=%q headers_only=%t delivered_stream_seq=%d ack_floor_stream_seq=%d num_pending=%d num_ack_pending=%d push_bound=%t\n",
			info.Name, info.Config.Durable, info.Config.FilterSubject, info.Config.HeadersOnly,
			info.Delivered.Stream, info.AckFloor.Stream, info.NumPending, info.NumAckPending, info.PushBound)
	}
	return b.String()
}

// streamLeader reports the node that leads a stream's replica group.
func (c *natsCluster) streamLeader(stream string) (*natsNode, error) {
	nc, err := nats.Connect(c.directURLs(), nats.Timeout(2*time.Second), nats.Name(harnessApp))
	if err != nil {
		return nil, err
	}
	defer nc.Close()
	js, err := nc.JetStream(nats.MaxWait(5 * time.Second))
	if err != nil {
		return nil, err
	}
	info, err := js.StreamInfo(topicPrefix + stream)
	if err != nil {
		return nil, err
	}
	if info.Cluster == nil {
		return nil, fmt.Errorf("stream %s has no cluster information", stream)
	}
	for _, node := range c.nodes {
		if node.name == info.Cluster.Leader {
			return node, nil
		}
	}
	return nil, fmt.Errorf("stream %s leader %q is not a known node", stream, info.Cluster.Leader)
}

func (c *natsCluster) stop() {
	if c.watcher != nil {
		c.watcher.Close()
	}
	for _, node := range c.nodes {
		node.proxy.close()
		if node.proc != nil {
			node.proc.stop(10 * time.Second)
		}
	}
}
