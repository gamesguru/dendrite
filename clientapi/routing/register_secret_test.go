package routing

import (
	"bytes"
	"io"
	"testing"

	"github.com/jellydator/ttlcache/v3"
)

func TestSharedSecretRegister(t *testing.T) {
	jsonStr := []byte(`{"admin":false,"mac":"7fdf82d4a424cd7aeee3d159a29a10c5b7410984","nonce":"759f047f312b99ff428b21d581256f8592b8976e58bc1b543972dc6147e529a79657605b52d7becd160ff5137f3de11975684319187e06901955f79e5a6c5a79","password":"wonderland","username":"alice","displayname":"rabbit"}`)
	sharedSecret := "zendritetest"

	req, err := NewSharedSecretRegistrationRequest(io.NopCloser(bytes.NewBuffer(jsonStr)))
	if err != nil {
		t.Fatalf("failed to read request: %s", err)
	}

	r := NewSharedSecretRegistration(sharedSecret)

	// force the nonce to be known
	r.nonces.Set(req.Nonce, true, ttlcache.DefaultTTL)

	valid, err := r.IsValidMacLogin(req.Nonce, req.User, req.Password, req.Admin, req.MacBytes)
	if err != nil {
		t.Fatalf("failed to check for valid mac: %s", err)
	}
	if !valid {
		t.Errorf("mac login failed, wanted success")
	}

	// modify the mac so it fails
	req.MacBytes[0] = 0xff
	valid, err = r.IsValidMacLogin(req.Nonce, req.User, req.Password, req.Admin, req.MacBytes)
	if err != nil {
		t.Fatalf("failed to check for valid mac: %s", err)
	}
	if valid {
		t.Errorf("mac login succeeded, wanted failure")
	}
}
