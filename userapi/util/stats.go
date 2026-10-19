// Copyright 2024 New Vector Ltd.
// Copyright 2022 The Matrix.org Foundation C.I.C.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

//go:build !wasm && !windows

package util

import (
	"syscall"
	"time"

	"github.com/sirupsen/logrus"
)

func getMemoryStats(p *phoneHomeStats) error {
	oldUsage := p.prevData
	newUsage := syscall.Rusage{}
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &newUsage); err != nil {
		logrus.WithError(err).Error("unable to get usage")
		return err
	}
	newData := timestampToRUUsage{timestamp: time.Now().Unix(), usage: newUsage}
	p.prevData = newData

	// Rusage time fields are int32 on 32-bit platforms, so widen before mixing with the int64 timestamps.
	usedCPUTime := (int64(newUsage.Utime.Sec) + int64(newUsage.Stime.Sec)) - (int64(oldUsage.usage.Utime.Sec) + int64(oldUsage.usage.Stime.Sec))

	if usedCPUTime == 0 || newData.timestamp == oldUsage.timestamp {
		p.stats["cpu_average"] = 0
	} else {
		p.stats["cpu_average"] = usedCPUTime / (newData.timestamp - oldUsage.timestamp) * 100 //nolint:mnd
	}
	p.stats["memory_rss"] = newUsage.Maxrss
	return nil
}
