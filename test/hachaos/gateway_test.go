//go:build linux

package hachaos

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"errors"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"time"
)

// gateway is the federation network. Every homeserver sends its federation
// traffic through it as an HTTPS proxy (the federation client honors
// HTTPS_PROXY), and the gateway terminates TLS itself. That lets it attribute
// every federation request to the process that sent it, record it, and
// partition one candidate from the remote server without touching the others.
type gateway struct {
	listener net.Listener
	owners   *owners
	timeline *timeline
	tls      *tls.Config

	mu      sync.Mutex
	cond    *sync.Cond
	routes  map[string]*httputil.ReverseProxy
	faults  map[string]fault
	effects []fedEffect
	txns    []inboundTxn
	// inboundCutoff, when set, abandons an inbound transaction to the HA
	// server that has not been answered in time, as a sender's request
	// timeout or a proxy's server timeout does.
	inboundCutoff time.Duration
}

// fedEffect is one federation request as it arrived at its destination.
type fedEffect struct {
	At     time.Time `json:"at"`
	Owner  string    `json:"owner"`
	Host   string    `json:"host"`
	Method string    `json:"method"`
	Path   string    `json:"path"`
	Status int       `json:"status"`
}

// inboundTxn is one attempt by the remote server to deliver a transaction to
// the HA server: what it carried and what the sender was told. Attempts that
// share a transaction ID are retries of one transaction.
type inboundTxn struct {
	At     time.Time `json:"at"`
	End    time.Time `json:"end"`
	TxnID  string    `json:"txn_id"`
	Status int       `json:"status"`
	// CutOff marks an attempt the gateway abandoned after inboundCutoff,
	// which cancels the request the HA server was processing.
	CutOff bool           `json:"cut_off,omitempty"`
	PDUs   int            `json:"pdus"`
	EDUs   map[string]int `json:"edus,omitempty"`
	// Receipts are "user event" pairs, ToDevice the workload nonces.
	Receipts []string `json:"receipts,omitempty"`
	ToDevice []string `json:"to_device,omitempty"`
	// PDUErrors are the per-event errors in a 200 response.
	PDUErrors map[string]string `json:"pdu_errors,omitempty"`
}

// ok reports whether the sender was told the transaction was delivered.
func (t inboundTxn) ok() bool { return t.Status/100 == 2 }

const sendPath = "/_matrix/federation/v1/send/"

// parseInboundTxn records what a transaction carries.
func parseInboundTxn(path string, body []byte) inboundTxn {
	t := inboundTxn{TxnID: strings.TrimPrefix(path, sendPath), EDUs: map[string]int{}}
	var txn struct {
		PDUs []json.RawMessage `json:"pdus"`
		EDUs []struct {
			Type    string          `json:"edu_type"`
			Content json.RawMessage `json:"content"`
		} `json:"edus"`
	}
	if json.Unmarshal(body, &txn) != nil {
		return t
	}
	t.PDUs = len(txn.PDUs)
	for _, edu := range txn.EDUs {
		t.EDUs[edu.Type]++
		switch edu.Type {
		case "m.receipt":
			var rooms map[string]map[string]map[string]struct {
				EventIDs []string `json:"event_ids"`
			}
			_ = json.Unmarshal(edu.Content, &rooms)
			for _, byType := range rooms {
				for user, receipt := range byType["m.read"] {
					for _, id := range receipt.EventIDs {
						t.Receipts = append(t.Receipts, user+" "+id)
					}
				}
			}
		case "m.direct_to_device":
			var msg struct {
				Messages map[string]map[string]struct {
					Nonce string `json:"nonce"`
				} `json:"messages"`
			}
			_ = json.Unmarshal(edu.Content, &msg)
			for _, byDevice := range msg.Messages {
				for _, content := range byDevice {
					if content.Nonce != "" {
						t.ToDevice = append(t.ToDevice, content.Nonce)
					}
				}
			}
		}
	}
	return t
}

// pduErrors reads the per-event errors from a transaction response.
func pduErrors(body []byte) map[string]string {
	var resp struct {
		PDUs map[string]struct {
			Error string `json:"error"`
		} `json:"pdus"`
	}
	if json.Unmarshal(body, &resp) != nil {
		return nil
	}
	var out map[string]string
	for id, res := range resp.PDUs {
		if res.Error == "" {
			continue
		}
		if out == nil {
			out = map[string]string{}
		}
		out[id] = res.Error
	}
	return out
}

func startGateway(o *owners, tl *timeline) (*gateway, error) {
	cert, err := selfSigned([]string{"ha.hachaos.test", "remote.hachaos.test"})
	if err != nil {
		return nil, err
	}
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	g := &gateway{
		listener: listener, owners: o, timeline: tl, routes: map[string]*httputil.ReverseProxy{}, faults: map[string]fault{},
		tls: &tls.Config{Certificates: []tls.Certificate{cert}, NextProtos: []string{"http/1.1"}, MinVersion: tls.VersionTLS12},
	}
	g.cond = sync.NewCond(&g.mu)
	go g.serve()
	return g, nil
}

// route sends federation traffic for a server name to a plain HTTP address.
func (g *gateway) route(serverName, target string) {
	u, _ := url.Parse(target)
	g.mu.Lock()
	defer g.mu.Unlock()
	g.routes[serverName] = httputil.NewSingleHostReverseProxy(u)
}

func (g *gateway) proxyURL() string { return "http://" + g.listener.Addr().String() }

func (g *gateway) setFault(owner string, f fault) {
	g.mu.Lock()
	if f == pass {
		delete(g.faults, owner)
	} else {
		g.faults[owner] = f
	}
	g.cond.Broadcast()
	g.mu.Unlock()
	g.timeline.add(eventFault, "federation", "%s -> %s", owner, f)
}

func (g *gateway) heal() {
	g.mu.Lock()
	g.faults = map[string]fault{}
	g.cond.Broadcast()
	g.mu.Unlock()
	g.timeline.add(eventFault, "federation", "healed")
}

// setInboundCutoff abandons inbound transactions to the HA server that take
// longer than d; zero waits for every answer.
func (g *gateway) setInboundCutoff(d time.Duration) {
	g.mu.Lock()
	g.inboundCutoff = d
	g.mu.Unlock()
	g.timeline.add(eventFault, "federation", "inbound transactions to the HA server cut off after %s", d)
}

// inboundTxns lists every attempt to deliver a transaction to the HA server.
func (g *gateway) inboundTxns() []inboundTxn {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]inboundTxn(nil), g.txns...)
}

func (g *gateway) faultFor(owner string) fault {
	if f, ok := g.faults[owner]; ok {
		return f
	}
	return g.faults[allOwners]
}

func (g *gateway) admit(ctx context.Context, owner string) bool {
	stop := context.AfterFunc(ctx, func() {
		g.mu.Lock()
		g.cond.Broadcast()
		g.mu.Unlock()
	})
	defer stop()
	g.mu.Lock()
	defer g.mu.Unlock()
	for {
		switch g.faultFor(owner) {
		case pass:
			return true
		case reject:
			return false
		case blackhole, dropUpstream, dropDownstream:
		}
		if ctx.Err() != nil {
			return false
		}
		g.cond.Wait()
	}
}

func (g *gateway) serve() {
	for {
		conn, err := g.listener.Accept()
		if err != nil {
			return
		}
		go g.handle(conn)
	}
}

// handle answers one CONNECT, then serves the tunneled TLS session itself.
func (g *gateway) handle(conn net.Conn) {
	owner := g.owners.of(conn)
	reader := bufio.NewReader(conn)
	req, err := http.ReadRequest(reader)
	if err != nil || req.Method != http.MethodConnect {
		_ = conn.Close()
		return
	}
	host := req.Host
	g.mu.Lock()
	reverse := g.routes[host]
	g.mu.Unlock()
	if reverse == nil {
		_, _ = conn.Write([]byte("HTTP/1.1 502 Bad Gateway\r\n\r\n"))
		_ = conn.Close()
		return
	}
	if _, err = conn.Write([]byte("HTTP/1.1 200 Connection Established\r\n\r\n")); err != nil {
		_ = conn.Close()
		return
	}
	tunnel := tls.Server(&bufferedConn{Conn: conn, reader: reader}, g.tls)
	server := &http.Server{
		ReadHeaderTimeout: 30 * time.Second,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Stamp the request when its sender sent it, not when a
			// partition released it.
			arrived := time.Now()
			if !g.admit(r.Context(), owner) {
				if hijacker, ok := w.(http.Hijacker); ok {
					if c, _, err := hijacker.Hijack(); err == nil {
						_ = c.Close()
						return
					}
				}
				w.WriteHeader(http.StatusBadGateway)
				return
			}
			if owner == "remote" && host == haServerName && r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, sendPath) {
				g.forwardTxn(w, r, reverse, arrived)
				return
			}
			recorder := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
			reverse.ServeHTTP(recorder, r)
			g.mu.Lock()
			g.effects = append(g.effects, fedEffect{At: arrived, Owner: owner, Host: host, Method: r.Method, Path: r.URL.Path, Status: recorder.status})
			g.mu.Unlock()
		}),
	}
	_ = server.Serve(&singleListener{conn: tunnel})
}

// forwardTxn forwards an inbound transaction to the HA server and records
// what it carried and what the sender was told.
func (g *gateway) forwardTxn(w http.ResponseWriter, r *http.Request, reverse *httputil.ReverseProxy, arrived time.Time) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadGateway)
		return
	}
	r.Body = io.NopCloser(bytes.NewReader(body))
	txn := parseInboundTxn(r.URL.Path, body)
	txn.At = arrived
	g.mu.Lock()
	cutoff := g.inboundCutoff
	g.mu.Unlock()
	ctx := r.Context()
	if cutoff > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, cutoff)
		defer cancel()
	}
	recorder := &bodyRecorder{statusRecorder: statusRecorder{ResponseWriter: w, status: http.StatusOK}}
	reverse.ServeHTTP(recorder, r.WithContext(ctx))
	txn.End, txn.Status = time.Now(), recorder.status
	txn.CutOff = cutoff > 0 && errors.Is(ctx.Err(), context.DeadlineExceeded)
	if txn.ok() {
		txn.PDUErrors = pduErrors(recorder.body.Bytes())
	}
	g.mu.Lock()
	g.effects = append(g.effects, fedEffect{At: arrived, Owner: "remote", Host: haServerName, Method: r.Method, Path: r.URL.Path, Status: txn.Status})
	g.txns = append(g.txns, txn)
	g.mu.Unlock()
}

// bodyRecorder keeps a copy of a response body as it is written.
type bodyRecorder struct {
	statusRecorder
	body bytes.Buffer
}

func (r *bodyRecorder) Write(b []byte) (int, error) {
	r.body.Write(b)
	return r.ResponseWriter.Write(b)
}

func (g *gateway) requests() []fedEffect {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]fedEffect(nil), g.effects...)
}

func (g *gateway) stop() { _ = g.listener.Close() }

// bufferedConn keeps bytes the CONNECT parser read ahead of the TLS handshake.
type bufferedConn struct {
	net.Conn
	reader *bufio.Reader
}

func (c *bufferedConn) Read(b []byte) (int, error) { return c.reader.Read(b) }

// singleListener serves exactly one connection, then reports closure once that
// connection ends so http.Server.Serve returns.
type singleListener struct {
	conn net.Conn
	once sync.Once
	done chan struct{}
	mu   sync.Mutex
}

var errListenerDone = errors.New("listener done")

func (l *singleListener) Accept() (net.Conn, error) {
	l.mu.Lock()
	if l.done == nil {
		l.done = make(chan struct{})
	}
	done := l.done
	l.mu.Unlock()
	var conn net.Conn
	l.once.Do(func() { conn = &closeNotifyConn{Conn: l.conn, done: done} })
	if conn != nil {
		return conn, nil
	}
	<-done
	return nil, errListenerDone
}

func (l *singleListener) Close() error { return nil }
func (l *singleListener) Addr() net.Addr {
	return l.conn.LocalAddr()
}

type closeNotifyConn struct {
	net.Conn
	done chan struct{}
	once sync.Once
}

func (c *closeNotifyConn) Close() error {
	c.once.Do(func() { close(c.done) })
	return c.Conn.Close()
}

// selfSigned issues a certificate for the federation names. Homeservers in
// the harness disable federation TLS validation, as test deployments do.
func selfSigned(names []string) (tls.Certificate, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, err
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()), Subject: pkix.Name{CommonName: names[0]},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(7 * 24 * time.Hour),
		DNSNames: names, KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		return tls.Certificate{}, err
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}, nil
}
