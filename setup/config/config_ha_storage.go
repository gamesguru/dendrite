package config

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
)

// VerifyHAStorage rejects storage that cannot be recovered by a fresh candidate.
func (c *Zendrite) VerifyHAStorage(errs *ConfigErrors) {
	if !c.Global.HA.Enabled {
		return
	}
	// A single database also supplies the migration serialization and compatibility record.
	if c.Global.DatabaseOptions.ConnectionString == "" || c.Global.DatabaseOptions.ConnectionString.IsSQLite() {
		errs.Add("HA requires a global PostgreSQL database")
	} else if _, err := pgx.ParseConfig(string(c.Global.DatabaseOptions.ConnectionString)); err != nil {
		errs.Add("HA global PostgreSQL connection string is invalid")
	}
	for _, db := range c.haDatabases() {
		if db != "" && db != c.Global.DatabaseOptions.ConnectionString {
			errs.Add("HA requires all component databases to use the global PostgreSQL target")
			break
		}
	}
	js := c.Global.JetStream
	for _, address := range js.Addresses {
		if strings.TrimSpace(address) == "" {
			errs.Add("HA requires nonempty external JetStream addresses")
		}
	}
	if len(js.Addresses) == 0 || js.InMemory {
		errs.Add("HA requires external JetStream with persistent storage")
	}
	if js.Replicas != 3 && js.Replicas != 5 {
		errs.Add("HA requires global.jetstream.replicas to be 3 or 5")
	}
	if js.TopicPrefix == "" {
		errs.Add("HA requires a nonempty JetStream topic_prefix")
	}
	if c.MediaAPI.StorageType() != "s3" && (c.MediaAPI.StorageType() != "local" || !c.MediaAPI.Storage.Local.Shared) {
		errs.Add("HA requires S3 media or media_api.storage.local.shared with a shared POSIX filesystem")
	}
	if c.SyncAPI.Fulltext.Enabled && !c.SyncAPI.Fulltext.RebuildOnStartup {
		errs.Add("HA search requires sync_api.search.rebuild_on_startup")
	}
	if len(c.MSCs.MSCs) != 0 {
		errs.Add("HA does not support experimental MSCs")
	}
}

func (c *Zendrite) haDatabases() []DataSource {
	return []DataSource{c.RoomServer.Database.ConnectionString, c.SyncAPI.Database.ConnectionString, c.MediaAPI.Database.ConnectionString, c.FederationAPI.Database.ConnectionString, c.KeyServer.Database.ConnectionString, c.UserAPI.AccountDatabase.ConnectionString, c.RelayAPI.Database.ConnectionString}
}

// HACompatibility fingerprints shared identities, namespaces and the exact binary.
// Credentials and private signing material are never included in the record.
func (c *Zendrite) HACompatibility(binaryDigest string) (string, error) {
	db, err := pgx.ParseConfig(string(c.Global.DatabaseOptions.ConnectionString))
	if err != nil {
		return "", fmt.Errorf("invalid HA PostgreSQL target")
	}
	identities := make([]string, 0)
	for _, id := range c.Global.SigningIdentities() {
		if len(id.PrivateKey) != ed25519.PrivateKeySize {
			return "", fmt.Errorf("invalid HA signing identity")
		}
		identities = append(identities, string(id.ServerName)+":"+string(id.KeyID)+":"+hex.EncodeToString(id.PrivateKey[ed25519.SeedSize:]))
	}
	sort.Strings(identities)
	addresses := make([]string, 0, len(c.Global.JetStream.Addresses))
	for _, address := range c.Global.JetStream.Addresses {
		u, parseErr := url.Parse(address)
		if parseErr != nil {
			return "", fmt.Errorf("invalid HA NATS address")
		}
		u.User = nil
		addresses = append(addresses, u.String())
	}
	sort.Strings(addresses)
	targets := []string{fmt.Sprintf("%s:%d", db.Host, db.Port)}
	for _, fallback := range db.Fallbacks {
		targets = append(targets, fmt.Sprintf("%s:%d", fallback.Host, fallback.Port))
	}
	s3 := c.MediaAPI.Storage.S3
	// Runtime parameters include search_path; only their digest enters the final digest.
	data, err := json.Marshal([]any{binaryDigest, identities, targets, db.User, db.Database, db.RuntimeParams, addresses, c.Global.JetStream.TopicPrefix, c.Global.JetStream.JetStreamDomain, c.Global.JetStream.Replicas, c.MediaAPI.StorageType(), c.MediaAPI.Storage.Local, c.MediaAPI.BasePath, s3.Endpoint, s3.Region, s3.Bucket, s3.Prefix, c.SyncAPI.Fulltext.Enabled, c.SyncAPI.Fulltext.Language})
	if err != nil {
		return "", fmt.Errorf("encode HA compatibility: %w", err)
	}
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:]), nil
}
