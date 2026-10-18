// Copyright 2024 New Vector Ltd.
// Copyright 2023 The Matrix.org Foundation C.I.C.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package sqlutil

import (
	"database/sql"
	"fmt"
	"sync"

	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/setup/process"
)

type Connections struct {
	globalConfig        config.DatabaseOptions
	processContext      *process.ProcessContext
	options             []ConnectionOption
	existingConnections sync.Map
}

type con struct {
	db     *sql.DB
	writer Writer
}

// NewConnectionManager returns a manager for the homeserver's database
// handles. HA supplies ownership options so that every pooled connection is
// bound to the authorized activation; standalone supplies none.
func NewConnectionManager(processCtx *process.ProcessContext, globalConfig config.DatabaseOptions, options ...ConnectionOption) *Connections {
	return &Connections{
		globalConfig:   globalConfig,
		processContext: processCtx,
		options:        options,
	}
}

func (c *Connections) Connection(dbProperties *config.DatabaseOptions) (*sql.DB, Writer, error) {
	var err error
	// If no connectionString was provided, try the global one
	if dbProperties.ConnectionString == "" {
		dbProperties = &c.globalConfig
		// If we still don't have a connection string, that's a problem
		if dbProperties.ConnectionString == "" {
			return nil, nil, fmt.Errorf("no database connections configured")
		}
	}

	writer := NewDummyWriter()
	if dbProperties.ConnectionString.IsSQLite() {
		writer = NewExclusiveWriter()
	}

	existing, loaded := c.existingConnections.LoadOrStore(dbProperties.ConnectionString, &con{})
	if loaded {
		// We found an existing connection
		ex, ok := existing.(*con)
		if !ok {
			return nil, nil, fmt.Errorf("unexpected type for existing connection")
		}
		return ex.db, ex.writer, nil
	}

	// Open a new database connection using the supplied config.
	db, err := Open(dbProperties, writer, c.options...)
	if err != nil {
		return nil, nil, err
	}
	c.existingConnections.Store(dbProperties.ConnectionString, &con{db: db, writer: writer})
	if c.processContext != nil {
		c.processContext.Defer(func() { _ = db.Close() })
	}
	return db, writer, nil
}
