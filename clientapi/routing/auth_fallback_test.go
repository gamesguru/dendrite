package routing

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	altcha "github.com/altcha-org/altcha-lib-go"

	"codefloe.com/pat-s/zendrite/clientapi/auth/authtypes"
	"codefloe.com/pat-s/zendrite/setup/config"
)

func Test_AuthFallback(t *testing.T) {
	cfg := config.Zendrite{}
	cfg.Defaults(config.DefaultOpts{Generate: true, SingleDatabase: true})
	for _, useHCaptcha := range []bool{false, true} {
		for _, recaptchaEnabled := range []bool{false, true} {
			for _, wantErr := range []bool{false, true} {
				t.Run(fmt.Sprintf("useHCaptcha(%v) - recaptchaEnabled(%v) - wantErr(%v)", useHCaptcha, recaptchaEnabled, wantErr), func(t *testing.T) {
					// Set the defaults for each test
					cfg.ClientAPI.Defaults(config.DefaultOpts{Generate: true, SingleDatabase: true})
					cfg.ClientAPI.RecaptchaEnabled = recaptchaEnabled
					cfg.ClientAPI.RecaptchaPublicKey = "pub"
					cfg.ClientAPI.RecaptchaPrivateKey = "priv"
					if useHCaptcha {
						cfg.ClientAPI.RecaptchaSiteVerifyAPI = "https://hcaptcha.com/siteverify"
						cfg.ClientAPI.RecaptchaApiJsUrl = "https://js.hcaptcha.com/1/api.js"
						cfg.ClientAPI.RecaptchaFormField = "h-captcha-response"
						cfg.ClientAPI.RecaptchaSitekeyClass = "h-captcha"
					}
					cfgErrs := &config.ConfigErrors{}
					cfg.ClientAPI.Verify(cfgErrs)
					if len(*cfgErrs) > 0 {
						t.Fatalf("(hCaptcha=%v) unexpected config errors: %s", useHCaptcha, cfgErrs.Error())
					}

					req := httptest.NewRequest(http.MethodGet, "/?session=1337", nil)
					rec := httptest.NewRecorder()

					AuthFallback(rec, req, authtypes.LoginTypeRecaptcha, &cfg.ClientAPI)
					if !recaptchaEnabled {
						if rec.Code != http.StatusBadRequest {
							t.Fatalf("unexpected response code: %d, want %d", rec.Code, http.StatusBadRequest)
						}
						if rec.Body.String() != "Recaptcha login is disabled on this Homeserver" {
							t.Fatalf("unexpected response body: %s", rec.Body.String())
						}
					} else if !strings.Contains(rec.Body.String(), cfg.ClientAPI.RecaptchaSitekeyClass) {
						t.Fatalf("body does not contain %s: %s", cfg.ClientAPI.RecaptchaSitekeyClass, rec.Body.String())
					}

					srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						if wantErr {
							_, _ = w.Write([]byte(`{"success":false}`))
							return
						}
						_, _ = w.Write([]byte(`{"success":true}`))
					}))
					defer srv.Close()

					cfg.ClientAPI.RecaptchaSiteVerifyAPI = srv.URL

					// check the result after sending the captcha
					req = httptest.NewRequest(http.MethodPost, "/?session=1337", nil)
					req.Form = url.Values{}
					req.Form.Add(cfg.ClientAPI.RecaptchaFormField, "someRandomValue")
					rec = httptest.NewRecorder()
					AuthFallback(rec, req, authtypes.LoginTypeRecaptcha, &cfg.ClientAPI)
					if recaptchaEnabled {
						if !wantErr {
							if rec.Code != http.StatusOK {
								t.Fatalf("unexpected response code: %d, want %d", rec.Code, http.StatusOK)
							}
							if rec.Body.String() != successTemplate {
								t.Fatalf("unexpected response: %s, want %s", rec.Body.String(), successTemplate)
							}
						} else {
							if rec.Code != http.StatusUnauthorized {
								t.Fatalf("unexpected response code: %d, want %d", rec.Code, http.StatusUnauthorized)
							}
							wantString := "Authentication"
							if !strings.Contains(rec.Body.String(), wantString) {
								t.Fatalf("expected response to contain '%s', but didn't: %s", wantString, rec.Body.String())
							}
						}
					} else {
						if rec.Code != http.StatusBadRequest {
							t.Fatalf("unexpected response code: %d, want %d", rec.Code, http.StatusBadRequest)
						}
						if rec.Body.String() != "Recaptcha login is disabled on this Homeserver" {
							t.Fatalf("unexpected response: %s, want %s", rec.Body.String(), "successTemplate")
						}
					}
				})
			}
		}
	}

	t.Run("unknown fallbacks are handled correctly", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/?session=1337", nil)
		rec := httptest.NewRecorder()
		AuthFallback(rec, req, "DoesNotExist", &cfg.ClientAPI)
		if rec.Code != http.StatusNotImplemented {
			t.Fatalf("unexpected http status: %d, want %d", rec.Code, http.StatusNotImplemented)
		}
	})

	t.Run("unknown methods are handled correctly", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodDelete, "/?session=1337", nil)
		rec := httptest.NewRecorder()
		AuthFallback(rec, req, authtypes.LoginTypeRecaptcha, &cfg.ClientAPI)
		if rec.Code != http.StatusMethodNotAllowed {
			t.Fatalf("unexpected http status: %d, want %d", rec.Code, http.StatusMethodNotAllowed)
		}
	})

	t.Run("missing session parameter is handled correctly", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		rec := httptest.NewRecorder()
		AuthFallback(rec, req, authtypes.LoginTypeRecaptcha, &cfg.ClientAPI)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("unexpected http status: %d, want %d", rec.Code, http.StatusBadRequest)
		}
	})

	t.Run("missing session parameter is handled correctly", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		rec := httptest.NewRecorder()
		AuthFallback(rec, req, authtypes.LoginTypeRecaptcha, &cfg.ClientAPI)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("unexpected http status: %d, want %d", rec.Code, http.StatusBadRequest)
		}
	})

	t.Run("missing 'response' is handled correctly", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/?session=1337", nil)
		rec := httptest.NewRecorder()
		AuthFallback(rec, req, authtypes.LoginTypeRecaptcha, &cfg.ClientAPI)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("unexpected http status: %d, want %d", rec.Code, http.StatusBadRequest)
		}
	})
}

func Test_AuthFallback_Altcha(t *testing.T) {
	const hmacKey = "test-hmac-secret"

	newAltchaCfg := func() config.ClientAPI {
		var c config.ClientAPI
		c.Defaults(config.DefaultOpts{Generate: true, SingleDatabase: true})
		c.RecaptchaEnabled = true
		c.CaptchaProvider = "altcha"
		c.AltchaHMACKey = hmacKey
		c.AltchaMaxNumber = 1000
		c.AltchaExpiry = "5m"
		return c
	}

	// solveChallenge creates a challenge and solves it, returning the
	// Base64-encoded payload that the client would submit.
	solveChallenge := func(t *testing.T, cfg *config.ClientAPI) string {
		t.Helper()
		challenge, err := altcha.CreateChallenge(altcha.ChallengeOptions{
			HMACKey:   cfg.AltchaHMACKey,
			MaxNumber: cfg.AltchaMaxNumber,
		})
		if err != nil {
			t.Fatalf("CreateChallenge: %v", err)
		}
		solution, err := altcha.SolveChallenge(
			challenge.Challenge, challenge.Salt,
			altcha.Algorithm(challenge.Algorithm),
			int(challenge.MaxNumber), 0, nil,
		)
		if err != nil {
			t.Fatalf("SolveChallenge: %v", err)
		}
		if solution == nil {
			t.Fatal("SolveChallenge returned nil")
		}
		payload := altcha.Payload{
			Algorithm: challenge.Algorithm,
			Challenge: challenge.Challenge,
			Number:    int64(solution.Number),
			Salt:      challenge.Salt,
			Signature: challenge.Signature,
		}
		payloadJSON, err := json.Marshal(payload)
		if err != nil {
			t.Fatalf("json.Marshal: %v", err)
		}
		return base64.StdEncoding.EncodeToString(payloadJSON)
	}

	t.Run("GET serves ALTCHA widget template", func(t *testing.T) {
		cfg := newAltchaCfg()
		req := httptest.NewRequest(http.MethodGet, "/?session=1337", nil)
		rec := httptest.NewRecorder()
		AuthFallback(rec, req, authtypes.LoginTypeRecaptcha, &cfg)
		if rec.Code != http.StatusOK {
			t.Fatalf("unexpected status: %d, want %d", rec.Code, http.StatusOK)
		}
		if !strings.Contains(rec.Body.String(), "altcha-widget") {
			t.Fatalf("expected ALTCHA widget in body, got: %s", rec.Body.String())
		}
	})

	t.Run("POST with valid solution succeeds", func(t *testing.T) {
		cfg := newAltchaCfg()
		encoded := solveChallenge(t, &cfg)

		req := httptest.NewRequest(http.MethodPost, "/?session=1337", nil)
		req.Form = url.Values{}
		req.Form.Add("altcha", encoded)
		rec := httptest.NewRecorder()
		AuthFallback(rec, req, authtypes.LoginTypeRecaptcha, &cfg)
		if rec.Code != http.StatusOK {
			t.Fatalf("unexpected status: %d, want %d", rec.Code, http.StatusOK)
		}
		if rec.Body.String() != successTemplate {
			t.Fatalf("unexpected response body: %s", rec.Body.String())
		}
	})

	t.Run("POST with invalid solution returns 401", func(t *testing.T) {
		cfg := newAltchaCfg()
		// Submit garbage as the ALTCHA payload.
		bogus := base64.StdEncoding.EncodeToString([]byte(`{"algorithm":"SHA-256","challenge":"bad","number":0,"salt":"bad","signature":"bad"}`))

		req := httptest.NewRequest(http.MethodPost, "/?session=1337", nil)
		req.Form = url.Values{}
		req.Form.Add("altcha", bogus)
		rec := httptest.NewRecorder()
		AuthFallback(rec, req, authtypes.LoginTypeRecaptcha, &cfg)
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("unexpected status: %d, want %d", rec.Code, http.StatusUnauthorized)
		}
	})

	t.Run("POST with empty response returns 400", func(t *testing.T) {
		cfg := newAltchaCfg()
		req := httptest.NewRequest(http.MethodPost, "/?session=1337", nil)
		req.Form = url.Values{}
		rec := httptest.NewRecorder()
		AuthFallback(rec, req, authtypes.LoginTypeRecaptcha, &cfg)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("unexpected status: %d, want %d", rec.Code, http.StatusBadRequest)
		}
	})
}
