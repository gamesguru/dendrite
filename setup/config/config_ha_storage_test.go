package config

import (
	"crypto/ed25519"
	"strings"
	"testing"
)

func haStorageConfig() *Zendrite {
	c := &Zendrite{}
	c.Defaults(DefaultOpts{Generate: true, SingleDatabase: true})
	c.Global.HA.Enabled = true
	c.Global.DatabaseOptions.ConnectionString = "postgres://user:password@db/matrix?sslmode=disable"
	c.Global.JetStream.Addresses = []string{"nats://broker:4222"}
	c.Global.JetStream.Replicas = 3
	c.MediaAPI.Storage.Type = "s3"
	c.MediaAPI.Storage.S3.Bucket = "media"
	return c
}

func TestHAStorageValidation(t *testing.T) {
	for _, tc := range []struct {
		name    string
		change  func(*Zendrite)
		invalid bool
	}{
		{"valid", func(*Zendrite) {}, false},
		{"standalone", func(c *Zendrite) { c.Global.HA.Enabled = false; c.Global.DatabaseOptions.ConnectionString = "file:db" }, false},
		{"sqlite", func(c *Zendrite) { c.Global.DatabaseOptions.ConnectionString = "file:db" }, true},
		{"missing database", func(c *Zendrite) { c.Global.DatabaseOptions.ConnectionString = "" }, true},
		{"component override", func(c *Zendrite) { c.SyncAPI.Database.ConnectionString = "postgres://other/matrix" }, true},
		{"component sqlite", func(c *Zendrite) { c.MediaAPI.Database.ConnectionString = "file:media" }, true},
		{"embedded", func(c *Zendrite) { c.Global.JetStream.Addresses = nil }, true},
		{"memory", func(c *Zendrite) { c.Global.JetStream.InMemory = true }, true},
		{"one replica", func(c *Zendrite) { c.Global.JetStream.Replicas = 1 }, true},
		{"five replicas", func(c *Zendrite) { c.Global.JetStream.Replicas = 5 }, false},
		{"local media", func(c *Zendrite) { c.MediaAPI.Storage.Type = "local" }, true},
		{"shared media", func(c *Zendrite) { c.MediaAPI.Storage.Type = "local"; c.MediaAPI.Storage.Local.Shared = true }, false},
		{"local search", func(c *Zendrite) { c.SyncAPI.Fulltext.Enabled = true }, true},
		{"rebuild search", func(c *Zendrite) { c.SyncAPI.Fulltext.Enabled = true; c.SyncAPI.Fulltext.RebuildOnStartup = true }, false},
		{"experimental", func(c *Zendrite) { c.MSCs.MSCs = []string{"msc-test"} }, true},
		{"qualified MSCs", func(c *Zendrite) { c.MSCs.MSCs = []string{"msc2836", "msc2444", "msc2753"} }, false},
		{"unqualified MSC", func(c *Zendrite) { c.MSCs.MSCs = []string{"msc2836", "msc3861"} }, true},
		{"dehydrated devices", func(c *Zendrite) { c.MSCs.MSCs = []string{"msc3814"} }, true},
		{"MSC name that enables nothing", func(c *Zendrite) { c.MSCs.MSCs = []string{"MSC2836"} }, true},
		{"MSC database is global", func(c *Zendrite) {
			c.MSCs.MSCs = []string{"msc2836"}
			c.MSCs.Database.ConnectionString = c.Global.DatabaseOptions.ConnectionString
		}, false},
		{"MSC database sqlite", func(c *Zendrite) {
			c.MSCs.MSCs = []string{"msc2836"}
			c.MSCs.Database.ConnectionString = "file:mscs.db"
		}, true},
		{"MSC database override", func(c *Zendrite) {
			c.MSCs.MSCs = []string{"msc2836"}
			c.MSCs.Database.ConnectionString = "postgres://other/matrix"
		}, true},
		{"unused MSC database", func(c *Zendrite) {
			c.MSCs.MSCs = []string{"msc2444", "msc2753"}
			c.MSCs.Database.ConnectionString = "file:mscs.db"
		}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := haStorageConfig()
			tc.change(c)
			errs := &ConfigErrors{}
			c.VerifyHAStorage(errs)
			if (len(*errs) > 0) != tc.invalid {
				t.Fatalf("unexpected validation: %v", errs)
			}
			if strings.Contains(strings.Join(*errs, " "), "password") {
				t.Fatal("credentials leaked")
			}
		})
	}
}

func TestHACompatibility(t *testing.T) {
	c := haStorageConfig()
	c.Global.PrivateKey = ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize))
	original, err := c.HACompatibility()
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		change func(*Zendrite)
		same   bool
	}{
		{"credentials", func(c *Zendrite) {
			c.Global.DatabaseOptions.ConnectionString = "postgres://user:rotated@db/matrix?sslmode=disable"
			c.MediaAPI.Storage.S3.SecretAccessKey = "rotated"
		}, true},
		{"candidate", func(c *Zendrite) { c.Global.HA.CandidateID = "other" }, true},
		{"database", func(c *Zendrite) {
			c.Global.DatabaseOptions.ConnectionString = "postgres://user:password@other/matrix?sslmode=disable"
		}, false},
		{"server", func(c *Zendrite) { c.Global.ServerName = "other" }, false},
		{"key", func(c *Zendrite) {
			seed := make([]byte, ed25519.SeedSize)
			seed[0] = 1
			c.Global.PrivateKey = ed25519.NewKeyFromSeed(seed)
		}, false},
		{"stream namespace", func(c *Zendrite) { c.Global.JetStream.TopicPrefix = "other" }, false},
		{"media namespace", func(c *Zendrite) { c.MediaAPI.Storage.S3.Prefix = "other" }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			copy := *c
			tc.change(&copy)
			got, err := copy.HACompatibility()
			if err != nil {
				t.Fatal(err)
			}
			if (got == original) != tc.same {
				t.Fatal("unexpected compatibility result")
			}
		})
	}
	// The executable is not part of the deployment's identity: the upgrade
	// contract decides which builds may share it.
	again, err := c.HACompatibility()
	if err != nil || again != original {
		t.Fatal("the deployment identity is not stable")
	}
}

// Candidates with different MSCs serve different routes and consumers, so
// they must not share a deployment; the order MSCs are listed in is irrelevant.
func TestHACompatibilityMSCs(t *testing.T) {
	compatibility := func(mscs ...string) string {
		t.Helper()
		c := haStorageConfig()
		c.Global.PrivateKey = ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize))
		c.MSCs.MSCs = mscs
		got, err := c.HACompatibility()
		if err != nil {
			t.Fatal(err)
		}
		return got
	}
	original := compatibility("msc2836", "msc2753")
	if compatibility("msc2753", "msc2836", "msc2753") != original {
		t.Fatal("reordered or repeated MSCs changed the compatibility")
	}
	for _, mscs := range [][]string{nil, {"msc2836"}, {"msc2836", "msc2753", "msc2444"}} {
		if compatibility(mscs...) == original {
			t.Fatalf("MSCs %v accepted alongside %v", mscs, []string{"msc2836", "msc2753"})
		}
	}
}
