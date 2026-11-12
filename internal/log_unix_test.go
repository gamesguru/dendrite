// Copyright 2024 New Vector Ltd.
// Copyright 2021 The Matrix.org Foundation C.I.C.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

//go:build !windows

package internal

import (
	"io"
	"os"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"

	"codefloe.com/pat-s/zendrite/setup/config"
)

// TestSetupHookLoggingDoesNotDuplicateStdOutput covers a configured std hook
// at a level below info. SetupHookLogging used to add a fallback info hook
// unconditionally, so every entry that matched both levels was written twice.
func TestSetupHookLoggingDoesNotDuplicateStdOutput(t *testing.T) {
	logger := logrus.StandardLogger()
	prevOut := logger.Out
	prevHooks := make(logrus.LevelHooks, len(logger.Hooks))
	for level, hooks := range logger.Hooks {
		prevHooks[level] = append([]logrus.Hook(nil), hooks...)
	}
	prevLevel := logger.Level
	prevReportCaller := logger.ReportCaller
	t.Cleanup(func() {
		logger.SetOutput(prevOut)
		logger.ReplaceHooks(prevHooks)
		logger.SetLevel(prevLevel)
		logger.SetReportCaller(prevReportCaller)
		stdLevelLogAdded = make(map[logrus.Level]bool)
	})

	stdLevelLogAdded = make(map[logrus.Level]bool)
	logrus.SetOutput(io.Discard)

	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	prevStdout := os.Stdout
	os.Stdout = writer
	t.Cleanup(func() { os.Stdout = prevStdout })

	SetupStdLogging()
	SetupHookLogging([]config.LogrusHook{
		{Type: "std", Level: "debug"},
		{Type: "std", Level: "info"},
	})
	logrus.Info("duplicate check")

	if err = writer.Close(); err != nil {
		t.Fatalf("close pipe: %v", err)
	}
	out, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read pipe: %v", err)
	}

	if got, want := strings.Count(string(out), "duplicate check"), 1; got != want {
		t.Fatalf("log entry written %d times, want %d:\n%s", got, want, out)
	}
}
