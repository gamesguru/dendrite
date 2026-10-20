package ha

import (
	"fmt"

	"github.com/go-logr/logr"
	"github.com/sirupsen/logrus"
	"k8s.io/klog/v2"
)

// configureLogging installs the process-wide klog bridge before HA starts any
// Kubernetes work. Standalone mode never changes klog's global configuration.
func configureLogging(logger *logrus.Logger) {
	klog.SetLoggerWithOptions(logr.New(&logrusSink{entry: logger.WithField("component", "kubernetes")}), klog.ContextualLogger(true))
}

type logrusSink struct {
	entry *logrus.Entry
	name  string
}

func (s *logrusSink) Init(logr.RuntimeInfo) {}

func (s *logrusSink) Enabled(level int) bool {
	return s.entry.Logger.IsLevelEnabled(logLevel(level))
}

func (s *logrusSink) Info(level int, message string, values ...any) {
	s.withValues(values).Log(logLevel(level), message)
}

func (s *logrusSink) Error(err error, message string, values ...any) {
	s.withValues(values).WithError(err).Error(message)
}

func (s *logrusSink) WithValues(values ...any) logr.LogSink {
	return &logrusSink{entry: s.withValues(values), name: s.name}
}

func (s *logrusSink) WithName(name string) logr.LogSink {
	if s.name != "" {
		name = s.name + "." + name
	}
	return &logrusSink{entry: s.entry.WithField("logger", name), name: name}
}

func (s *logrusSink) withValues(values []any) *logrus.Entry {
	fields := logrus.Fields{}
	for i := 0; i < len(values); i += 2 {
		value := any("<missing>")
		if i+1 < len(values) {
			value = values[i+1]
		}
		fields[fmt.Sprint(values[i])] = value
	}
	return s.entry.WithFields(fields)
}

func logLevel(verbosity int) logrus.Level {
	if verbosity > 0 {
		return logrus.DebugLevel
	}
	return logrus.InfoLevel
}
