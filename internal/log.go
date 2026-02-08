// Copyright 2024 New Vector Ltd.
// Copyright 2017 Vector Creations Ltd
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package internal

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"sync"

	"github.com/DeRuina/timberjack"
	"github.com/matrix-org/util"
	"github.com/sirupsen/logrus"

	"codefloe.com/pat-s/dendrite/setup/config"
)

// logrus is using a global variable when we're using `logrus.AddHook`
// this unfortunately results in us adding the same hook multiple times.
// This map ensures we only ever add one level hook.
var (
	stdLevelLogAdded = make(map[logrus.Level]bool)
	levelLogAddedMu  = &sync.Mutex{}
)

type utcFormatter struct {
	logrus.Formatter
}

func (f utcFormatter) Format(entry *logrus.Entry) ([]byte, error) {
	entry.Time = entry.Time.UTC()
	return f.Formatter.Format(entry)
}

// Logrus hook which wraps another hook and filters log entries according to their level.
// (Note that we cannot use solely logrus.SetLevel, because Dendrite supports multiple
// levels of logging at the same time.)
type logLevelHook struct {
	level logrus.Level
	logrus.Hook
}

// writerHook is a logrus hook that writes formatted log entries to an io.Writer.
type writerHook struct {
	writer    io.Writer
	formatter logrus.Formatter
}

func (h *writerHook) Fire(entry *logrus.Entry) error {
	data, err := h.formatter.Format(entry)
	if err != nil {
		return err
	}
	_, err = h.writer.Write(data)
	return err
}

func (h *writerHook) Levels() []logrus.Level {
	return logrus.AllLevels
}

// Levels returns all the levels supported by this hook.
func (h *logLevelHook) Levels() []logrus.Level {
	levels := make([]logrus.Level, 0)

	for _, level := range logrus.AllLevels {
		if level <= h.level {
			levels = append(levels, level)
		}
	}

	return levels
}

// callerPrettyfier is a function that given a runtime.Frame object, will
// extract the calling function's name and file, and return them in a nicely
// formatted way.
func callerPrettyfier(f *runtime.Frame) (string, string) {
	// Retrieve just the function name
	s := strings.Split(f.Function, ".")
	funcname := s[len(s)-1]

	// Append a newline + tab to it to move the actual log content to its own line
	funcname += "\n\t"

	// Use a shortened file path which just has the filename to avoid having lots of redundant
	// directories which contribute significantly to overall log sizes!
	filename := fmt.Sprintf(" [%s:%d]", path.Base(f.File), f.Line)

	return funcname, filename
}

// SetupPprof starts a pprof listener. We use the DefaultServeMux here because it is
// simplest, and it gives us the freedom to run pprof on a separate port.
//
// WARNING: The pprof endpoint has no authentication and serves over plain HTTP.
// It exposes sensitive runtime information (goroutine stacks, heap dumps, CPU
// profiles). Only bind to localhost or a trusted network interface.
func SetupPprof() {
	if hostPort := os.Getenv("PPROFLISTEN"); hostPort != "" {
		logrus.Warnf("Starting pprof listener on %s — this endpoint has NO authentication, do not expose to untrusted networks", hostPort)
		go func() {
			logrus.WithError(http.ListenAndServe(hostPort, nil)).Error("Failed to setup pprof listener")
		}()
	}
}

// SetupStdLogging configures the logging format to standard output. Typically, it is called when the config is not yet loaded.
func SetupStdLogging() {
	levelLogAddedMu.Lock()
	defer levelLogAddedMu.Unlock()
	logrus.SetReportCaller(true)
	logrus.SetFormatter(&utcFormatter{
		&logrus.TextFormatter{
			TimestampFormat:  "2006-01-02T15:04:05.000000000Z07:00",
			FullTimestamp:    true,
			DisableColors:    false,
			DisableTimestamp: false,
			QuoteEmptyFields: true,
			CallerPrettyfier: callerPrettyfier,
		},
	})
}

// File type hooks should be provided a path to a directory to store log files.
func checkFileHookParams(params map[string]any) {
	path, ok := params["path"]
	if !ok {
		logrus.Fatalf("Expecting a parameter \"path\" for logging hook of type \"file\"")
	}

	if _, ok := path.(string); !ok {
		logrus.Fatalf("Parameter \"path\" for logging hook of type \"file\" should be a string")
	}
}

// Add a new FSHook to the logger. Each component will log in its own file.
func setupFileHook(hook config.LogrusHook, level logrus.Level) {
	dirPath, ok := (hook.Params["path"]).(string)
	if !ok {
		logrus.Fatal("log hook 'path' param is not a string")
	}
	fullPath := filepath.Join(dirPath, "dendrite.log")

	if err := os.MkdirAll(path.Dir(fullPath), os.ModePerm); err != nil {
		logrus.Fatalf("Couldn't create directory %s: %q", path.Dir(fullPath), err)
	}

	// Create timberjack logger with daily rotation and gzip compression
	writer := &timberjack.Logger{
		Filename:    fullPath,
		MaxBackups:  7,                 //nolint:mnd                 // keep 7 days of backups
		Compression: "gzip",            // compress rotated files
		RotateAt:    []string{"00:00"}, // rotate daily at midnight
	}

	logrus.AddHook(&logLevelHook{
		level,
		&writerHook{
			writer: writer,
			formatter: &utcFormatter{
				&logrus.TextFormatter{
					TimestampFormat:  "2006-01-02T15:04:05.000000000Z07:00",
					DisableColors:    true,
					DisableTimestamp: false,
					DisableSorting:   false,
					QuoteEmptyFields: true,
				},
			},
		},
	})
}

// CloseAndLogIfError Closes io.Closer and logs the error if any.
func CloseAndLogIfError(ctx context.Context, closer io.Closer, message string) { //nolint:contextcheck
	if closer == nil {
		return
	}
	err := closer.Close()
	if ctx == nil {
		ctx = context.TODO()
	}
	if err != nil {
		util.GetLogger(ctx).WithError(err).Error(message)
	}
}
