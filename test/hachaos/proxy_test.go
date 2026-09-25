//go:build linux

package hachaos

import (
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

// fault is what a proxy does to one owner's traffic.
type fault int

const (
	// Pass forwards traffic normally.
	pass fault = iota
	// Blackhole stops traffic in both directions without closing anything,
	// which is how a network partition looks to both ends: writes queue in
	// socket buffers and reads block until a timeout fires.
	blackhole
	// DropUpstream stops the owner's traffic towards the service while the
	// service can still answer, an asymmetric partition.
	dropUpstream
	// DropDownstream stops the service's traffic towards the owner while the
	// owner can still send.
	dropDownstream
	// Reject closes existing connections and refuses new ones, which is what
	// a stopped service or a firewall that sends resets looks like.
	reject
)

func (f fault) String() string {
	switch f {
	case pass:
		return "pass"
	case blackhole:
		return "blackhole"
	case dropUpstream:
		return "drop-upstream"
	case dropDownstream:
		return "drop-downstream"
	case reject:
		return "reject"
	}
	return fmt.Sprintf("fault(%d)", int(f))
}

// allOwners applies a fault to every owner that has no rule of its own.
const allOwners = "*"

// proxy is a TCP proxy whose faults are applied per owning process.
type proxy struct {
	name     string
	listener net.Listener
	target   func() string
	owners   *owners
	timeline *timeline

	mu     sync.Mutex
	cond   *sync.Cond
	faults map[string]fault
	conns  map[*proxyConn]struct{}
	closed bool
}

type proxyConn struct {
	owner      string
	downstream net.Conn
	upstream   net.Conn
	once       sync.Once
}

func (c *proxyConn) close() {
	c.once.Do(func() {
		_ = c.downstream.Close()
		if c.upstream != nil {
			_ = c.upstream.Close()
		}
	})
}

func newProxy(name string, target func() string, o *owners, tl *timeline) (*proxy, error) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	p := &proxy{name: name, listener: listener, target: target, owners: o, timeline: tl, faults: map[string]fault{}, conns: map[*proxyConn]struct{}{}}
	p.cond = sync.NewCond(&p.mu)
	go p.serve()
	return p, nil
}

func (p *proxy) addr() string { return p.listener.Addr().String() }

// port is the listening port, as a string for environment variables.
func (p *proxy) port() string {
	_, port, _ := net.SplitHostPort(p.addr())
	return port
}

// setFault applies f to owner's traffic, including connections that are
// already open, and wakes any copy that was held by the previous rule.
func (p *proxy) setFault(owner string, f fault) {
	p.mu.Lock()
	if f == pass {
		delete(p.faults, owner)
	} else {
		p.faults[owner] = f
	}
	var closing []*proxyConn
	if f == reject {
		for c := range p.conns {
			if owner == allOwners || c.owner == owner {
				closing = append(closing, c)
			}
		}
	}
	p.cond.Broadcast()
	p.mu.Unlock()
	for _, c := range closing {
		c.close()
	}
	if p.timeline != nil {
		p.timeline.add(eventFault, p.name, "%s -> %s", owner, f)
	}
}

// heal removes every rule.
func (p *proxy) heal() {
	p.mu.Lock()
	p.faults = map[string]fault{}
	p.cond.Broadcast()
	p.mu.Unlock()
	if p.timeline != nil {
		p.timeline.add(eventFault, p.name, "healed")
	}
}

// resetAll closes every open connection, which is what clients observe when
// the address behind a virtual IP moves to another server.
func (p *proxy) resetAll() {
	p.mu.Lock()
	var closing []*proxyConn
	for c := range p.conns {
		closing = append(closing, c)
	}
	p.mu.Unlock()
	for _, c := range closing {
		c.close()
	}
}

func (p *proxy) faultFor(owner string) fault {
	if f, ok := p.faults[owner]; ok {
		return f
	}
	return p.faults[allOwners]
}

// held reports whether traffic in one direction must wait. Callers hold mu.
func (p *proxy) held(owner string, upstream bool) bool {
	switch p.faultFor(owner) {
	case pass, reject:
		return false
	case blackhole:
		return true
	case dropUpstream:
		return upstream
	case dropDownstream:
		return !upstream
	}
	return false
}

// await blocks while traffic in the given direction is held, and reports
// whether the connection must be closed instead.
func (p *proxy) await(c *proxyConn, upstream bool) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	for {
		if p.closed {
			return false
		}
		if _, open := p.conns[c]; !open {
			return false
		}
		if p.faultFor(c.owner) == reject {
			return false
		}
		if !p.held(c.owner, upstream) {
			return true
		}
		p.cond.Wait()
	}
}

func (p *proxy) serve() {
	for {
		conn, err := p.listener.Accept()
		if err != nil {
			return
		}
		go p.handle(conn)
	}
}

func (p *proxy) handle(downstream net.Conn) {
	c := &proxyConn{owner: p.owners.of(downstream), downstream: downstream}
	p.mu.Lock()
	if p.closed || p.faultFor(c.owner) == reject {
		p.mu.Unlock()
		_ = downstream.Close()
		return
	}
	p.conns[c] = struct{}{}
	p.mu.Unlock()
	defer func() {
		p.mu.Lock()
		delete(p.conns, c)
		p.cond.Broadcast()
		p.mu.Unlock()
		c.close()
	}()
	// A partitioned client cannot even complete its connection to the
	// service, so hold the dial as well as the data.
	if !p.await(c, true) {
		return
	}
	upstream, err := net.DialTimeout("tcp", p.target(), 5*time.Second)
	if err != nil {
		return
	}
	p.mu.Lock()
	c.upstream = upstream
	p.mu.Unlock()
	done := make(chan struct{}, 2)
	go func() { p.pipe(c, downstream, upstream, true); done <- struct{}{} }()
	go func() { p.pipe(c, upstream, downstream, false); done <- struct{}{} }()
	<-done
}

// pipe copies one direction. It waits before reading as well as before
// writing, so a held direction stops draining the sender's socket and the
// sender's writes eventually block, exactly as they would behind a partition.
func (p *proxy) pipe(c *proxyConn, src, dst net.Conn, upstream bool) {
	buf := make([]byte, 32*1024)
	for {
		if !p.await(c, upstream) {
			return
		}
		n, err := src.Read(buf)
		if n > 0 {
			if !p.await(c, upstream) {
				return
			}
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return
			}
		}
		if err != nil {
			if !errors.Is(err, io.EOF) {
				return
			}
			if tcp, ok := dst.(*net.TCPConn); ok {
				_ = tcp.CloseWrite()
			}
			return
		}
	}
}

func (p *proxy) close() {
	p.mu.Lock()
	p.closed = true
	p.cond.Broadcast()
	var closing []*proxyConn
	for c := range p.conns {
		closing = append(closing, c)
	}
	p.mu.Unlock()
	_ = p.listener.Close()
	for _, c := range closing {
		c.close()
	}
}
