//go:build linux

package hachaos

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
)

// owners attributes loopback connections to the process that opened them.
//
// Candidates must share one database, broker and object-store address, because
// the HA compatibility fingerprint covers those endpoints. A proxy therefore
// cannot tell candidates apart by port. It asks the kernel instead: the client
// end of an accepted connection is a socket owned by exactly one process, and
// /proc exposes both the socket inode and every process's descriptors.
type owners struct {
	mu   sync.Mutex
	pids map[int]string
}

func newOwners() *owners { return &owners{pids: map[int]string{}} }

func (o *owners) track(pid int, name string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.pids[pid] = name
}

func (o *owners) forget(pid int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	delete(o.pids, pid)
}

func (o *owners) snapshot() map[int]string {
	o.mu.Lock()
	defer o.mu.Unlock()
	out := make(map[int]string, len(o.pids))
	for pid, name := range o.pids {
		out[pid] = name
	}
	return out
}

// unknownOwner labels connections from untracked processes, which includes
// the harness itself.
const unknownOwner = "harness"

// of reports the tracked process that owns the client end of conn.
func (o *owners) of(conn net.Conn) string {
	client, ok := conn.RemoteAddr().(*net.TCPAddr)
	if !ok {
		return unknownOwner
	}
	server, ok := conn.LocalAddr().(*net.TCPAddr)
	if !ok {
		return unknownOwner
	}
	inode, err := socketInode(client, server)
	if err != nil || inode == "" {
		return unknownOwner
	}
	target := "socket:[" + inode + "]"
	for pid, name := range o.snapshot() {
		entries, err := os.ReadDir(fmt.Sprintf("/proc/%d/fd", pid))
		if err != nil {
			continue
		}
		for _, entry := range entries {
			link, err := os.Readlink(fmt.Sprintf("/proc/%d/fd/%s", pid, entry.Name()))
			if err == nil && link == target {
				return name
			}
		}
	}
	return unknownOwner
}

// socketInode finds the inode of the socket whose local end is client and
// whose remote end is server.
func socketInode(client, server *net.TCPAddr) (string, error) {
	for _, table := range []string{"/proc/net/tcp", "/proc/net/tcp6"} {
		inode, err := findSocket(table, client, server)
		if err != nil {
			return "", err
		}
		if inode != "" {
			return inode, nil
		}
	}
	return "", nil
}

func findSocket(table string, client, server *net.TCPAddr) (string, error) {
	f, err := os.Open(table)
	if err != nil {
		return "", err
	}
	defer f.Close()
	local, remote := procAddr(client, table), procAddr(server, table)
	scanner := bufio.NewScanner(f)
	scanner.Scan() // header
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 10 {
			continue
		}
		if fields[1] == local && fields[2] == remote {
			return fields[9], nil
		}
	}
	return "", scanner.Err()
}

// procAddr renders an address the way /proc/net/tcp does: each 32-bit word of
// the address as the kernel stores it, printed in host (little-endian) order,
// then the port in hex.
func procAddr(addr *net.TCPAddr, table string) string {
	ip := addr.IP.To4()
	if strings.HasSuffix(table, "6") {
		ip = addr.IP.To16()
	}
	if ip == nil {
		return ""
	}
	var b strings.Builder
	for i := 0; i+3 < len(ip); i += 4 {
		fmt.Fprintf(&b, "%02X%02X%02X%02X", ip[i+3], ip[i+2], ip[i+1], ip[i])
	}
	fmt.Fprintf(&b, ":%04X", addr.Port)
	return b.String()
}
