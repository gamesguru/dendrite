// Copyright 2024 New Vector Ltd.
// Copyright 2019 Parminder Singh <parmsingh129@gmail.com>
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package routing

import (
	"errors"
	"fmt"
	"html/template"
	"net/http"

	"github.com/matrix-org/util"

	"codefloe.com/pat-s/zendrite/clientapi/auth/authtypes"
	"codefloe.com/pat-s/zendrite/setup/config"
)

// recaptchaTemplate is an HTML webpage template for recaptcha auth.
const recaptchaTemplate = `
<html>
<head>
<title>Authentication</title>
<meta name='viewport' content='width=device-width, initial-scale=1,
    user-scalable=no, minimum-scale=1.0, maximum-scale=1.0'>
<script src="{{.apiJsUrl}}" async defer></script>
<script src="//code.jquery.com/jquery-1.11.2.min.js"></script>
<script>
function captchaDone() {
    $('#registrationForm').submit();
}
</script>
</head>
<body>
<form id="registrationForm" method="post" action="{{.myUrl}}">
    <div>
        <p>
        Hello! We need to prevent computer programs and other automated
        things from creating accounts on this server.
        </p>
        <p>
        Please verify that you're not a robot.
        </p>
		<input type="hidden" name="session" value="{{.session}}" />
        <div class="{{.sitekeyClass}}"
            data-sitekey="{{.sitekey}}"
            data-callback="captchaDone">
        </div>
        <noscript>
        <input type="submit" value="All Done" />
        </noscript>
        </div>
    </div>
</form>
</body>
</html>
`

// altchaTemplate is an HTML webpage template for ALTCHA proof-of-work auth.
const altchaTemplate = `
<html>
<head>
<title>Authentication</title>
<meta name='viewport' content='width=device-width, initial-scale=1,
    user-scalable=no, minimum-scale=1.0, maximum-scale=1.0'>
<script async defer src="https://cdn.jsdelivr.net/npm/altcha/dist/altcha.min.js" type="module"></script>
<style>
  body { font-family: sans-serif; margin: 2em; }
</style>
</head>
<body>
<form id="registrationForm" method="post" action="{{.myUrl}}">
    <div>
        <p>
        Hello! We need to prevent computer programs and other automated
        things from creating accounts on this server.
        </p>
        <p>
        Please complete the verification below.
        </p>
        <input type="hidden" name="session" value="{{.session}}" />
        <input type="hidden" name="altcha" id="altchaPayload" />
        <altcha-widget challengeurl="{{.challengeUrl}}" auto="onload"></altcha-widget>
    </div>
</form>
<script>
// Wait for the ALTCHA web component to be defined before adding listeners.
customElements.whenDefined('altcha-widget').then(function() {
    document.querySelector('altcha-widget').addEventListener('statechange', function(ev) {
        if (ev.detail && ev.detail.state === 'verified') {
            // Copy the payload from the widget into our hidden input
            // since Shadow DOM ElementInternals may not be included
            // in traditional form submissions.
            document.getElementById('altchaPayload').value = ev.detail.payload || '';
            document.getElementById('registrationForm').submit();
        }
    });
});
</script>
</body>
</html>
`

// successTemplate is an HTML template presented to the user after successful
// recaptcha completion.
const successTemplate = `
<html>
<head>
<title>Success!</title>
<meta name='viewport' content='width=device-width, initial-scale=1,
    user-scalable=no, minimum-scale=1.0, maximum-scale=1.0'>
<script>
if (window.onAuthDone) {
    window.onAuthDone();
} else if (window.opener && window.opener.postMessage) {
    window.opener.postMessage("authDone", "*");
}
</script>
</head>
<body>
    <div>
        <p>Thank you!</p>
        <p>You may now close this window and return to the application.</p>
    </div>
</body>
</html>
`

// serveTemplate fills template data and serves it using http.ResponseWriter.
func serveTemplate(w http.ResponseWriter, templateHTML string, data map[string]string) {
	t := template.Must(template.New("response").Parse(templateHTML))
	if err := t.Execute(w, data); err != nil {
		panic(err)
	}
}

// AuthFallback implements GET and POST /auth/{authType}/fallback/web?session={sessionID}.
func AuthFallback(
	w http.ResponseWriter, req *http.Request, authType string,
	cfg *config.ClientAPI,
) {
	switch authType {
	case authtypes.LoginTypeRecaptcha, authtypes.LoginTypeAltcha:
		if !cfg.RecaptchaEnabled {
			writeHTTPMessage(w, req,
				"Captcha login is disabled on this Homeserver",
				http.StatusBadRequest,
			)
			return
		}
	default:
		writeHTTPMessage(w, req, fmt.Sprintf("Unknown authtype %q", authType), http.StatusNotImplemented)
		return
	}

	sessionID := req.URL.Query().Get("session")
	if sessionID == "" {
		writeHTTPMessage(w, req,
			"Session ID not provided",
			http.StatusBadRequest,
		)
		return
	}

	serveRecaptcha := func() {
		data := map[string]string{
			"myUrl":        req.URL.String(),
			"session":      sessionID,
			"apiJsUrl":     cfg.RecaptchaApiJsUrl,
			"sitekey":      cfg.RecaptchaPublicKey,
			"sitekeyClass": cfg.RecaptchaSitekeyClass,
			"formField":    cfg.RecaptchaFormField,
		}
		serveTemplate(w, recaptchaTemplate, data)
	}

	serveAltcha := func() {
		data := map[string]string{
			"myUrl":        req.URL.String(),
			"session":      sessionID,
			"challengeUrl": "/_zendrite/altcha/challenge",
		}
		serveTemplate(w, altchaTemplate, data)
	}

	serveCaptchaPage := func() {
		if authType == authtypes.LoginTypeAltcha {
			serveAltcha()
		} else {
			serveRecaptcha()
		}
	}

	serveSuccess := func() {
		data := map[string]string{}
		serveTemplate(w, successTemplate, data)
	}

	switch req.Method {
	case http.MethodGet:
		serveCaptchaPage()
		return
	case http.MethodPost:
		err := req.ParseForm()
		if err != nil {
			util.GetLogger(req.Context()).WithError(err).Error("req.ParseForm failed")
			w.WriteHeader(http.StatusBadRequest)
			serveCaptchaPage()
			return
		}

		var captchaErr error
		if authType == authtypes.LoginTypeAltcha {
			response := req.Form.Get("altcha")
			captchaErr = validateAltcha(cfg, response)
		} else {
			clientIP := req.RemoteAddr
			response := req.Form.Get(cfg.RecaptchaFormField)
			captchaErr = validateRecaptcha(cfg, response, clientIP)
		}

		switch {
		case errors.Is(captchaErr, ErrMissingResponse):
			w.WriteHeader(http.StatusBadRequest)
			serveCaptchaPage()
			return
		case errors.Is(captchaErr, ErrInvalidCaptcha):
			w.WriteHeader(http.StatusUnauthorized)
			serveCaptchaPage()
			return
		case captchaErr == nil:
		default:
			util.GetLogger(req.Context()).WithError(captchaErr).Error("failed to validate captcha")
			serveCaptchaPage()
			return
		}

		// Success. Mark this auth stage as completed.
		sessions.addCompletedSessionStage(sessionID, authtypes.LoginType(authType))

		serveSuccess()
		return
	}
	writeHTTPMessage(w, req, "Bad method", http.StatusMethodNotAllowed)
}

// writeHTTPMessage writes the given header and message to the HTTP response writer.
// Returns an error JSONResponse obtained through httputil.LogThenError if the writing failed, otherwise nil.
func writeHTTPMessage(
	w http.ResponseWriter, req *http.Request,
	message string, header int,
) {
	w.WriteHeader(header)
	_, err := w.Write([]byte(message))
	if err != nil {
		util.GetLogger(req.Context()).WithError(err).Error("w.Write failed")
	}
}
