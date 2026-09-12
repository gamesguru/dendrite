package config

import (
	"fmt"
	"math"
	"strings"
	"time"
	"unicode"

	"k8s.io/apimachinery/pkg/util/validation"
)

// HA configures opt-in Kubernetes Lease election for one homeserver.
// Election does not authorize application effects without an ownership gate.
type HA struct {
	Enabled        bool   `yaml:"enabled"`
	Backend        string `yaml:"backend"`
	LeaseName      string `yaml:"lease_name"`
	LeaseNamespace string `yaml:"lease_namespace"`
	// CandidateID identifies the deployment candidate; each process appends a UUID.
	CandidateID   string        `yaml:"candidate_id"`
	LeaseDuration time.Duration `yaml:"lease_duration"`
	RenewDeadline time.Duration `yaml:"renew_deadline"`
	RetryPeriod   time.Duration `yaml:"retry_period"`
}

// Defaults keeps HA disabled and supplies the reference election timings.
func (c *HA) Defaults() {
	c.Backend = "kubernetes"
	const (
		referenceLeaseDuration = 30 * time.Second
		referenceRenewDeadline = 20 * time.Second
		referenceRetryPeriod   = 5 * time.Second
	)
	c.LeaseDuration = referenceLeaseDuration
	c.RenewDeadline = referenceRenewDeadline
	c.RetryPeriod = referenceRetryPeriod
}

// Verify rejects ambiguous identities and unsafe election timings when enabled.
func (c *HA) Verify(errs *ConfigErrors) {
	if !c.Enabled {
		return
	}
	if c.Backend != "kubernetes" {
		errs.Add("global.ha.backend must be kubernetes")
	}
	for _, field := range []struct{ name, value string }{
		{"lease_name", c.LeaseName},
		{"lease_namespace", c.LeaseNamespace},
	} {
		name, value := field.name, field.value
		var problems []string
		if name == "lease_namespace" {
			problems = validation.IsDNS1123Label(value)
		} else {
			problems = validation.IsDNS1123Subdomain(value)
		}
		if len(problems) != 0 {
			errs.Add(fmt.Sprintf("global.ha.%s: %s", name, strings.Join(problems, "; ")))
		}
	}
	if c.CandidateID == "" || strings.TrimSpace(c.CandidateID) != c.CandidateID || strings.ContainsFunc(c.CandidateID, func(r rune) bool { return unicode.IsControl(r) || unicode.IsSpace(r) }) {
		errs.Add("global.ha.candidate_id must be a nonempty candidate identity without surrounding whitespace or control separators (use the pod UID)")
	}
	// The API stores LeaseDurationSeconds as int32; do not truncate or overflow it.
	if c.LeaseDuration%time.Second != 0 || c.LeaseDuration/time.Second > math.MaxInt32 {
		errs.Add("global.ha.lease_duration must be whole seconds and fit in a positive signed 32-bit LeaseDurationSeconds value")
	}
	// Match client-go's jitter constraint without importing its election runtime here.
	const jitterFactor = 1.2
	if c.RetryPeriod <= 0 || c.RenewDeadline <= 0 || c.LeaseDuration < time.Second || c.LeaseDuration <= c.RenewDeadline || float64(c.RenewDeadline) <= jitterFactor*float64(c.RetryPeriod) {
		errs.Add("global.ha durations require lease_duration >= 1s, lease_duration > renew_deadline > 1.2 * retry_period > 0")
	}
}
