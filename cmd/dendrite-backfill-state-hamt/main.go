// Copyright 2026 The Dendrite contributors.
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

// Command dendrite-backfill-state-hamt persists HAMT records for snapshots created by
// builds that predate HAMT-backed writes.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"time"

	"codefloe.com/pat-s/zendrite/internal/caching"
	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	"codefloe.com/pat-s/zendrite/roomserver/storage"
	"codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/setup"
	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/setup/process"
)

const (
	cacheMaxBytes    = 8 << 20
	cacheTTL         = 5 * time.Minute
	defaultBatchSize = 100
)

var (
	batchSize       = flag.Int("batch-size", defaultBatchSize, "maximum snapshots to fetch per query (each snapshot is backfilled in its own transaction)")
	requireComplete = flag.Bool("require-complete", false,
		"exit non-zero unless every state snapshot has a persisted HAMT")
)

var verify = flag.Bool("verify", false,
	"compare every persisted HAMT with the state blocks it was built from and exit non-zero on any mismatch")

var collectGarbage = flag.Bool("collect-garbage", false,
	"trace all retained HAMT roots and delete unreachable nodes (holds an exclusive lock for the full scan)")

func main() {
	ctx := context.Background()
	cfg := setup.ParseFlags(true)
	cfg.Logging = append(cfg.Logging[:0], config.LogrusHook{Type: "std", Level: "error"})
	processCtx := process.NewProcessContext()
	connections := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions)
	dbOpts := cfg.RoomServer.Database
	if dbOpts.ConnectionString == "" {
		dbOpts = cfg.Global.DatabaseOptions
	}
	if *batchSize <= 0 {
		panic("-batch-size must be positive")
	}
	db, err := storage.Open(
		processCtx.Context(), connections, &dbOpts,
		caching.NewRistrettoCache(cacheMaxBytes, cacheTTL, caching.DisableMetrics),
	)
	if err != nil {
		panic(err)
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
	if *collectGarbage {
		collector, ok := db.(interface{ CollectStateHAMTGarbage(context.Context) error })
		if !ok {
			panic("database does not support state HAMT garbage collection")
		}
		if err = collector.CollectStateHAMTGarbage(ctx); err != nil {
			panic(err)
		}
		fmt.Println("collected unreachable state HAMT nodes")
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
