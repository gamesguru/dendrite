//go:build linux

package hachaos

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"syscall"
	"time"
)

// brokerAbsence is how long a lost broker stays away before it is replaced.
// Production lost one for hours; the harness keeps the absence long enough
// for every client timeout, consumer expiry and stale-connection check to
// fire, and short enough for CI.
func brokerAbsence(tier string) time.Duration {
	if tier == tierCI {
		return 90 * time.Second
	}
	return 4 * time.Minute
}

// brokerLossTarget is the recovery objective when one broker of three is lost.
// No takeover is needed: the candidate must notice the silent broker and
// reconnect to another, which the client's ping check bounds at 30 seconds.
const brokerLossTarget = 60 * time.Second

// inboundSenderTimeout is how long the remote server waits for the HA server
// to answer a transaction in the broker scenarios, as Synapse's federation
// client or a proxy's server timeout would, before abandoning the request.
const inboundSenderTimeout = 20 * time.Second

// brokerHostFrozenScenario freezes the broker the active candidate is
// connected to, which is how a broker on a host that goes NotReady looks: its
// connections stay open and nothing answers. The broker is then replaced, as
// Kubernetes replaces the pod once the node is gone, on its retained storage.
func brokerHostFrozenScenario() scenario {
	return scenario{
		name:        "nats-broker-host-frozen",
		description: "The broker the active candidate is connected to stops responding without closing its connections, as on a host that goes NotReady, while the remote server abandons transactions it has waited 20 seconds for. After an extended absence the broker is replaced on its retained storage.",
		tiers:       allTiers,
		target:      brokerLossTarget,
		run: func(ctx context.Context, s *run) error {
			leader, err := s.active()
			if err != nil {
				return err
			}
			node, err := s.e.nats.brokerOf(leader.name)
			if err != nil {
				return err
			}
			s.e.fed.setInboundCutoff(inboundSenderTimeout)
			s.faultf(node.name, "SIGSTOP the broker %s is connected to", leader.name)
			if err = node.proc.signal(syscall.SIGSTOP); err != nil {
				return err
			}
			time.Sleep(brokerAbsence(s.e.settings.tier))
			s.e.timeline.add(eventFault, node.name, "replace the frozen broker on its retained storage")
			if err = s.e.nats.replace(ctx, node, false); err != nil {
				return err
			}
			s.rejoined = time.Now()
			return nil
		},
	}
}

// brokerReplacedEmptyScenario loses the broker leading the receipt stream and
// replaces it after an extended absence with empty storage, as a pod
// rescheduled onto a new volume is. The replacement must catch up from its
// peers.
func brokerReplacedEmptyScenario() scenario {
	return scenario{
		name:        "nats-broker-replaced-empty",
		description: "The broker leading the receipt stream is killed, and after an extended absence replaced by a broker with the same name and empty storage, which must catch up from its peers.",
		tiers:       allTiers,
		target:      brokerLossTarget,
		run: func(ctx context.Context, s *run) error {
			node, err := s.e.nats.streamLeader("OutputReceiptEvent")
			if err != nil {
				return err
			}
			s.e.fed.setInboundCutoff(inboundSenderTimeout)
			s.faultf(node.name, "SIGKILL the receipt stream leader")
			node.proc.kill()
			time.Sleep(brokerAbsence(s.e.settings.tier))
			s.e.timeline.add(eventFault, node.name, "replace the lost broker with empty storage")
			if err = s.e.nats.replace(ctx, node, true); err != nil {
				return err
			}
			s.rejoined = time.Now()
			return nil
		},
	}
}

// publishAcksLostScenario holds every broker's traffic towards the active
// candidate, so publications reach the stream but their acknowledgements do
// not: each publication is ambiguous until the candidate retries it.
func publishAcksLostScenario() scenario {
	return scenario{
		name:        "nats-publish-acks-lost",
		description: "For 15 seconds the brokers receive the active candidate's publications but their acknowledgements and deliveries are held, so every publication is ambiguous. Retries must not duplicate anything.",
		tiers:       allTiers,
		run: func(ctx context.Context, s *run) error {
			leader, err := s.active()
			if err != nil {
				return err
			}
			s.faultf(leader.name, "hold broker traffic towards the active candidate")
			s.partition(leader.name, dropDownstream, "nats")
			time.Sleep(15 * time.Second)
			s.healAll()
			return nil
		},
	}
}

// brokerOf reports the broker a candidate's JetStream connection is using.
// Candidates name their connection after their identity, which starts with
// the slot name.
func (c *natsCluster) brokerOf(candidate string) (*natsNode, error) {
	var found []string
	for _, node := range c.nodes {
		names, err := node.clientNames()
		if err != nil {
			return nil, fmt.Errorf("%s: %w", node.name, err)
		}
		for _, name := range names {
			if strings.HasPrefix(name, "Zendrite/") && strings.Contains(name, "/"+candidate+"_") {
				return node, nil
			}
			found = append(found, node.name+":"+name)
		}
	}
	return nil, fmt.Errorf("no broker has a connection from %s; connections: %v", candidate, found)
}

// clientNames lists the names of the broker's client connections.
func (n *natsNode) clientNames() ([]string, error) {
	resp, err := (&http.Client{Timeout: 2 * time.Second}).Get(fmt.Sprintf("http://127.0.0.1:%d/connz?limit=1024", n.monitor))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var connz struct {
		Connections []struct {
			Name string `json:"name"`
		} `json:"connections"`
	}
	if err = json.NewDecoder(resp.Body).Decode(&connz); err != nil {
		return nil, err
	}
	names := make([]string, 0, len(connz.Connections))
	for _, conn := range connz.Connections {
		names = append(names, conn.Name)
	}
	return names, nil
}

// replace stops a broker that was lost, killing it even when it is frozen,
// and starts a new process with the same name. With wipe, the new process
// starts on empty storage.
func (c *natsCluster) replace(ctx context.Context, node *natsNode, wipe bool) error {
	node.proc.kill()
	if wipe {
		if err := os.RemoveAll(node.store); err != nil {
			return err
		}
		if err := os.MkdirAll(node.store, 0o755); err != nil {
			return err
		}
	}
	return c.restart(ctx, node)
}
