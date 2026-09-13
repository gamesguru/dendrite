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
	original, err := c.HACompatibility("binary-one")
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
			got, err := copy.HACompatibility("binary-one")
			if err != nil {
				t.Fatal(err)
			}
			if (got == original) != tc.same {
				t.Fatal("unexpected compatibility result")
			}
		})
	}
	other, err := c.HACompatibility("binary-two")
	if err != nil || other == original {
		t.Fatal("mixed binaries accepted")
	}
}
