// Copyright 2026 The Dendrite contributors.
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

// Command backfill-state-hamt persists HAMT records for snapshots created
// before HAMT writes were enabled. Run with -tags rezzy.
package main

import (
	"context"
	"flag"
	"fmt"
	"time"

	"codefloe.com/pat-s/zendrite/internal/caching"
	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	"codefloe.com/pat-s/zendrite/internal/statehamt"
	"codefloe.com/pat-s/zendrite/roomserver/storage"
	"codefloe.com/pat-s/zendrite/setup"
	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/setup/process"
)

var batchSize = flag.Int("batch-size", 100, "maximum snapshots to backfill per transaction batch")

func main() {
	if !statehamt.Enabled {
		panic("state HAMT support is disabled; rebuild with -tags rezzy")
	}
	ctx := context.Background()
	cfg := setup.ParseFlags(true)
	cfg.Logging = append(cfg.Logging[:0], config.LogrusHook{Type: "std", Level: "error"})
	processCtx := process.NewProcessContext()
	connections := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions)
	dbOpts := cfg.RoomServer.Database
	if dbOpts.ConnectionString == "" {
		dbOpts = cfg.Global.DatabaseOptions
	}
	db, err := storage.Open(
		processCtx.Context(), connections, &dbOpts,
		caching.NewRistrettoCache(8*1024*1024, time.Minute*5, caching.DisableMetrics),
	)
	if err != nil {
		panic(err)
	}
	if *batchSize <= 0 {
		panic("-batch-size must be positive")
	}

	total := 0
	for {
		count, err := db.BackfillStateHAMTs(ctx, *batchSize)
		if err != nil {
			panic(err)
		}
		total += count
		fmt.Printf("backfilled %d snapshots (total %d)\n", count, total)
		if count == 0 {
			break
		}
	}
}
