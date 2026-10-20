//go:build linux

package hachaos

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"time"
)

// process is one supervised child. Every child runs in its own process group,
// so pausing or killing it also reaches helpers it forked, as a crashed or
// frozen host would.
type process struct {
	name string
	cmd  *exec.Cmd
	log  *os.File
	done chan struct{}
}

type processSpec struct {
	name string
	bin  string
	args []string
	env  []string
	dir  string
	// credential runs the child as another user, which PostgreSQL requires
	// when the harness itself runs as root.
	credential *syscall.Credential
}

func startProcess(logDir string, spec processSpec) (*process, error) {
	logFile, err := os.OpenFile(filepath.Join(logDir, spec.name+".log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	fmt.Fprintf(logFile, "==== %s starting %s %v\n", time.Now().UTC().Format(time.RFC3339Nano), spec.bin, spec.args)
	cmd := exec.Command(spec.bin, spec.args...)
	cmd.Env = append(os.Environ(), spec.env...)
	cmd.Dir = spec.dir
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL, Credential: spec.credential}
	if err = cmd.Start(); err != nil {
		_ = logFile.Close()
		return nil, fmt.Errorf("start %s: %w", spec.name, err)
	}
	p := &process{name: spec.name, cmd: cmd, log: logFile, done: make(chan struct{})}
	go func() {
		err := cmd.Wait()
		fmt.Fprintf(logFile, "==== %s exited: %v\n", time.Now().UTC().Format(time.RFC3339Nano), err)
		_ = logFile.Close()
		close(p.done)
	}()
	return p, nil
}

func (p *process) pid() int { return p.cmd.Process.Pid }

func (p *process) exited() bool {
	select {
	case <-p.done:
		return true
	default:
		return false
	}
}

// signal delivers sig to the whole process group.
func (p *process) signal(sig syscall.Signal) error {
	if p.exited() {
		return fmt.Errorf("%s already exited", p.name)
	}
	return syscall.Kill(-p.pid(), sig)
}

// signalMain delivers sig to the main process only, which is how a
// supervisor asks a server such as PostgreSQL to shut down cleanly.
func (p *process) signalMain(sig syscall.Signal) error {
	if p.exited() {
		return fmt.Errorf("%s already exited", p.name)
	}
	return p.cmd.Process.Signal(sig)
}

func (p *process) wait(timeout time.Duration) error {
	select {
	case <-p.done:
		return nil
	case <-time.After(timeout):
		return fmt.Errorf("%s did not exit within %s", p.name, timeout)
	}
}

// stop asks for a clean exit, then kills the group if it does not come.
// A paused group is resumed first so it can act on the request.
func (p *process) stop(timeout time.Duration) {
	if p.exited() {
		return
	}
	_ = p.signal(syscall.SIGCONT)
	_ = p.signalMain(syscall.SIGTERM)
	if p.wait(timeout) == nil {
		return
	}
	p.kill()
}

func (p *process) kill() {
	if p.exited() {
		return
	}
	_ = p.signal(syscall.SIGKILL)
	_ = p.wait(10 * time.Second)
}

var errNotExited = errors.New("process has not exited")

// exitCode reports the exit status, or errNotExited while it runs.
func (p *process) exitCode() (int, error) {
	if !p.exited() {
		return 0, errNotExited
	}
	return p.cmd.ProcessState.ExitCode(), nil
}
