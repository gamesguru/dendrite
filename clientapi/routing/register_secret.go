package routing

import (
	"context"
	"crypto/hmac"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/jellydator/ttlcache/v3"
	"github.com/matrix-org/util"

	"codefloe.com/pat-s/zendrite/internal"
)

type SharedSecretRegistrationRequest struct {
	User        string `json:"username"`
	Password    string `json:"password"`
	Nonce       string `json:"nonce"`
	MacBytes    []byte
	MacStr      string `json:"mac"`
	Admin       bool   `json:"admin"`
	DisplayName string `json:"displayname,omitempty"`
}

func NewSharedSecretRegistrationRequest(reader io.ReadCloser) (*SharedSecretRegistrationRequest, error) {
	defer internal.CloseAndLogIfError(context.Background(), reader, "NewSharedSecretRegistrationRequest: failed to close request body")
	var ssrr SharedSecretRegistrationRequest
	err := json.NewDecoder(reader).Decode(&ssrr)
	if err != nil {
		return nil, err
	}
	ssrr.MacBytes, err = hex.DecodeString(ssrr.MacStr)
	return &ssrr, err
}

type SharedSecretRegistration struct {
	sharedSecret string
	nonces       *ttlcache.Cache[string, bool]
}

func NewSharedSecretRegistration(sharedSecret string) *SharedSecretRegistration {
	cache := ttlcache.New[string, bool](
		ttlcache.WithTTL[string, bool](5 * time.Minute), //nolint:mnd
	)
	go cache.Start() // starts automatic cleanup
	return &SharedSecretRegistration{
		sharedSecret: sharedSecret,
		nonces:       cache,
	}
}

func (r *SharedSecretRegistration) GenerateNonce() string {
	nonce := util.RandomString(16) //nolint:mnd
	r.nonces.Set(nonce, true, ttlcache.DefaultTTL)
	return nonce
}

func (r *SharedSecretRegistration) validNonce(nonce string) bool {
	item := r.nonces.Get(nonce)
	return item != nil
}

func (r *SharedSecretRegistration) IsValidMacLogin(
	nonce, username, password string,
	isAdmin bool,
	givenMac []byte,
) (bool, error) {
	// Check that shared secret registration isn't disabled.
	if r.sharedSecret == "" {
		return false, errors.New("shared secret registration is disabled")
	}
	if !r.validNonce(nonce) {
		return false, fmt.Errorf("incorrect or expired nonce: %s", nonce)
	}

	// Check that username/password don't contain the HMAC delimiters.
	if strings.Contains(username, "\x00") {
		return false, errors.New("username contains invalid character")
	}
	if strings.Contains(password, "\x00") {
		return false, errors.New("password contains invalid character")
	}

	adminString := "notadmin"
	if isAdmin {
		adminString = "admin"
	}
	joined := strings.Join([]string{nonce, username, password, adminString}, "\x00")

	mac := hmac.New(sha1.New, []byte(r.sharedSecret))
	_, err := mac.Write([]byte(joined))
	if err != nil {
		return false, err
	}
	expectedMAC := mac.Sum(nil)

	return hmac.Equal(givenMac, expectedMAC), nil
}
