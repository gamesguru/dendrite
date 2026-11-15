// Copyright 2026 The Dendrite contributors.
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

// Command backfill-state-hamt persists HAMT records for snapshots created
// before HAMT writes were enabled. Run with -tags rezzy.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"time"

	"codefloe.com/pat-s/zendrite/internal/caching"
	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	"codefloe.com/pat-s/zendrite/internal/statehamt"
	"codefloe.com/pat-s/zendrite/roomserver/storage"
	"codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/setup"
	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/setup/process"
)

var batchSize = flag.Int("batch-size", 100, "maximum snapshots to backfill per transaction batch")
var requireComplete = flag.Bool("require-complete", false,
	"exit non-zero unless every state snapshot has a persisted HAMT")
var verify = flag.Bool("verify", false,
	"compare every persisted HAMT with the state blocks it was built from and exit non-zero on any mismatch")

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

	snapshots, withHAMT, err := db.StateHAMTCoverage(ctx)
	if err != nil {
		panic(err)
	}
	fmt.Printf("state HAMT coverage: %d/%d snapshots\n", withHAMT, snapshots)
	if *requireComplete && withHAMT != snapshots {
		panic(fmt.Sprintf("%d state snapshots still have no HAMT", snapshots-withHAMT))
	}

	if *verify {
		checked, missing, mismatched, err := verifyStateHAMTs(ctx, db, *batchSize)
		if err != nil {
			panic(err)
		}
		fmt.Printf("state HAMT verification: %d snapshots match, %d have no HAMT, %d disagree\n",
			checked, missing, mismatched)
		if mismatched > 0 {
			panic(fmt.Sprintf("%d state HAMTs disagree with their state blocks", mismatched))
		}
	}
}

// verifyStateHAMTs compares every persisted HAMT with the state blocks it was
// built from, paging through the snapshots so a large database does not have
// to fit in memory.
func verifyStateHAMTs(
	ctx context.Context, db storage.Database, page int,
) (checked, missing, mismatched int, err error) {
	if page <= 0 {
		page = 1000
	}
	var after types.StateSnapshotNID
	for {
		nids, err := db.StateSnapshotNIDs(ctx, after, page)
		if err != nil {
			return checked, missing, mismatched, err
		}
		if len(nids) == 0 {
			return checked, missing, mismatched, nil
		}
		for _, nid := range nids {
			after = nid
			switch err := db.VerifyStateHAMT(ctx, nid); {
			case err == nil:
				checked++
			case errors.Is(err, types.ErrNoStateHAMT):
				missing++
			default:
				mismatched++
				fmt.Printf("MISMATCH snapshot %d: %v\n", nid, err)
			}
		}
		fmt.Printf("verified %d snapshots (after snapshot %d)\n", checked+missing+mismatched, after)
	}
}
