//go:build linux

package hachaos

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"testing"
)

// The test binary doubles as the NATS and S3 server processes, so the harness
// needs no separately built broker or object store and faults such as SIGSTOP
// apply to a real, separate process.
const roleEnv = "HACHAOS_ROLE"

func TestMain(m *testing.M) {
	switch role := os.Getenv(roleEnv); role {
	case "":
		os.Exit(m.Run())
	case "nats":
		runRole(runNATSRole)
	case "s3":
		runRole(runS3Role)
	default:
		fmt.Fprintf(os.Stderr, "unknown %s %q\n", roleEnv, role)
		os.Exit(2)
	}
}

// runRole runs a server role until it is asked to stop. A role that fails to
// start exits nonzero so the harness sees the failure in its log.
func runRole(run func(stop <-chan os.Signal) error) {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)
	if err := run(stop); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	os.Exit(0)
}
