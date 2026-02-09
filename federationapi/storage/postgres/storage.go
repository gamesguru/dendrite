// Copyright 2024 New Vector Ltd.
// Copyright 2019, 2020 The Matrix.org Foundation C.I.C.
// Copyright 2017, 2018 New Vector Ltd
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package postgres

import (
	"context"
	"database/sql"
	"fmt"

	"codefloe.com/pat-s/gomatrixserverlib/spec"

	"codefloe.com/pat-s/dendrite/federationapi/storage/postgres/deltas"
	"codefloe.com/pat-s/dendrite/federationapi/storage/shared"
	"codefloe.com/pat-s/dendrite/internal/caching"
	"codefloe.com/pat-s/dendrite/internal/sqlutil"
	"codefloe.com/pat-s/dendrite/setup/config"
)

// Database stores information needed by the federation sender.
type Database struct {
	shared.Database
	db     *sql.DB
	writer sqlutil.Writer
}

// NewDatabase opens a new database.
func NewDatabase(ctx context.Context, conMan *sqlutil.Connections, dbProperties *config.DatabaseOptions, cache caching.FederationCache, isLocalServerName func(spec.ServerName) bool) (*Database, error) {
	var d Database
	var err error
	if d.db, d.writer, err = conMan.Connection(dbProperties); err != nil {
		return nil, err
	}
	blacklist, err := NewPostgresBlacklistTable(d.db)
	if err != nil {
		return nil, err
	}
	joinedHosts, err := NewPostgresJoinedHostsTable(d.db)
	if err != nil {
		return nil, err
	}
	queuePDUs, err := NewPostgresQueuePDUsTable(d.db)
	if err != nil {
		return nil, err
	}
	queueEDUs, err := NewPostgresQueueEDUsTable(d.db) //nolint:contextcheck
	if err != nil {
		return nil, err
	}
	queueJSON, err := NewPostgresQueueJSONTable(d.db)
	if err != nil {
		return nil, err
	}
	assumedOffline, err := NewPostgresAssumedOfflineTable(d.db)
	if err != nil {
		return nil, err
	}
	relayServers, err := NewPostgresRelayServersTable(d.db)
	if err != nil {
		return nil, err
	}
	inboundPeeks, err := NewPostgresInboundPeeksTable(d.db)
	if err != nil {
		return nil, err
	}
	outboundPeeks, err := NewPostgresOutboundPeeksTable(d.db)
	if err != nil {
		return nil, err
	}
	notaryJSON, err := NewPostgresNotaryServerKeysTable(d.db)
	if err != nil {
		return nil, fmt.Errorf("NewPostgresNotaryServerKeysTable: %w", err)
	}
	notaryMetadata, err := NewPostgresNotaryServerKeysMetadataTable(d.db)
	if err != nil {
		return nil, fmt.Errorf("NewPostgresNotaryServerKeysMetadataTable: %w", err)
	}
	serverSigningKeys, err := NewPostgresServerSigningKeysTable(d.db)
	if err != nil {
		return nil, err
	}
	retryState, err := NewPostgresRetryStateTable(d.db)
	if err != nil {
		return nil, err
	}
	m := sqlutil.NewMigrator(d.db)
	m.AddMigrations(sqlutil.Migration{
		Version: "federationsender: drop federationsender_rooms",
		Up:      deltas.UpRemoveRoomsTable,
	})
	err = m.Up(ctx)
	if err != nil {
		return nil, err
	}
	if err = queueEDUs.Prepare(); err != nil {
		return nil, err
	}
	d.Database = shared.Database{
		DB:                       d.db,
		IsLocalServerName:        isLocalServerName,
		Cache:                    cache,
		Writer:                   d.writer,
		FederationJoinedHosts:    joinedHosts,
		FederationQueuePDUs:      queuePDUs,
		FederationQueueEDUs:      queueEDUs,
		FederationQueueJSON:      queueJSON,
		FederationBlacklist:      blacklist,
		FederationAssumedOffline: assumedOffline,
		FederationRelayServers:   relayServers,
		FederationInboundPeeks:   inboundPeeks,
		FederationOutboundPeeks:  outboundPeeks,
		NotaryServerKeysJSON:     notaryJSON,
		NotaryServerKeysMetadata: notaryMetadata,
		ServerSigningKeys:        serverSigningKeys,
		FederationRetryState:     retryState,
	}
	return &d, nil
}
