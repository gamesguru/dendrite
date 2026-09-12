package ha

import (
	"errors"
	"io"
	"testing"

	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"k8s.io/klog/v2"
)

func TestKubernetesLogging(t *testing.T) {
	state := klog.CaptureState()
	defer state.Restore()
	logger, hook := logrustest.NewNullLogger()
	logger.SetOutput(io.Discard)
	configureLogging(logger)
	klog.InfoS("acquired lease", "lease", "matrix")
	klog.Background().V(2).Info("hidden diagnostic")
	entries := hook.AllEntries()
	if len(entries) != 1 || entries[0].Level != logrus.InfoLevel || entries[0].Data["lease"] != "matrix" || entries[0].Data["component"] != "kubernetes" {
		t.Fatalf("election log did not reach hooks with fields/level: %+v", entries)
	}
	logger.SetLevel(logrus.DebugLevel)
	named := klog.Background().WithName("election").WithName("renew").WithValues("candidate", "pod")
	named.V(2).Info("retry", "attempt", 3)
	entry := hook.LastEntry()
	if entry.Level != logrus.DebugLevel || entry.Data["logger"] != "election.renew" || entry.Data["candidate"] != "pod" || entry.Data["attempt"] != 3 {
		t.Fatalf("lost verbose level or contextual fields: %+v", entry)
	}
	logger.SetLevel(logrus.ErrorLevel)
	klog.InfoS("hidden election event")
	cause := errors.New("API unavailable")
	klog.ErrorS(cause, "renewal failed", "lease", "matrix")
	entries = hook.AllEntries()
	entry = hook.LastEntry()
	loggedError, ok := entry.Data[logrus.ErrorKey].(error)
	if len(entries) != 3 || entry.Level != logrus.ErrorLevel || !ok || !errors.Is(loggedError, cause) || entry.Data["lease"] != "matrix" {
		t.Fatalf("error routing or filtering failed: %+v", entries)
	}
	if _, leaked := entry.Data["candidate"]; leaked {
		t.Fatal("child logger fields leaked to parent")
	}
}
