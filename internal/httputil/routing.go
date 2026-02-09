// Copyright 2024 New Vector Ltd.
// Copyright 2020 The Matrix.org Foundation C.I.C.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package httputil

import (
	"encoding/json"
	"net/http"
	"net/url"
	"regexp"
	"strings"

	"codefloe.com/pat-s/gomatrixserverlib/spec"
	"github.com/go-chi/chi/v5"
)

// URLDecodeMapValues is a function that iterates through each of the items in a
// map, URL decodes the value, and returns a new map with the decoded values
// under the same key names.
func URLDecodeMapValues(vmap map[string]string) (map[string]string, error) {
	decoded := make(map[string]string, len(vmap))
	for key, value := range vmap {
		decodedVal, err := url.PathUnescape(value)
		if err != nil {
			return make(map[string]string), err
		}
		decoded[key] = decodedVal
	}

	return decoded, nil
}

// Vars extracts all path variables from an http.Request using chi's URL params.
func Vars(r *http.Request) map[string]string {
	vars := make(map[string]string)
	rctx := chi.RouteContext(r.Context())
	if rctx == nil {
		return vars
	}
	for i, key := range rctx.URLParams.Keys {
		if i < len(rctx.URLParams.Values) {
			vars[key] = rctx.URLParams.Values[i]
		}
	}
	return vars
}

// Route represents a registered route that supports method chaining.
type Route struct {
	router   *Router
	patterns []string // may have multiple patterns due to regex alternatives
	handler  http.Handler
}

// Methods registers the route for the specified HTTP methods.
func (rt *Route) Methods(methods ...string) *Route {
	for _, pattern := range rt.patterns {
		fullPattern := rt.router.prefix + pattern
		for _, method := range methods {
			rt.router.mux.Method(method, fullPattern, rt.handler)
		}
	}
	return rt
}

// Name sets a name for the route (for compatibility with gorilla/mux).
// The name is not used with chi routing but is kept for API compatibility.
func (rt *Route) Name(_ string) *Route {
	return rt
}

// Router wraps chi.Mux with path prefix support and additional features.
type Router struct {
	mux    chi.Router
	prefix string
}

// NewRouter creates a new Router with the given path prefix.
func NewRouter(prefix string) *Router {
	mux := chi.NewRouter()
	mux.NotFound(notFoundHandler)
	mux.MethodNotAllowed(methodNotAllowedHandler)
	return &Router{
		mux:    mux,
		prefix: strings.TrimSuffix(prefix, "/"),
	}
}

// normalisePattern converts gorilla/mux style patterns to chi patterns.
// It converts {var:regex} to {var} and {var...} to {var:*}.
func normalisePattern(pattern string) string {
	// Remove regex patterns from path variables, e.g., {var:regex} becomes {var}
	re := regexp.MustCompile(`\{([^:}]+):[^}]+\}`)
	pattern = re.ReplaceAllString(pattern, "{$1}")
	// Convert {var...} wildcard to chi's {var:*} syntax
	pattern = strings.ReplaceAll(pattern, "...}", ":*}")
	return pattern
}

// expandPattern converts gorilla/mux style patterns to chi patterns.
// It handles regex alternatives like {var:(?:a|b)} by returning multiple patterns.
// Simple regex patterns like {var:[^/]+} are simplified to {var}.
func expandPattern(pattern string) []string {
	// Check for alternative patterns like {var:(?:opt1|opt2)}
	altRe := regexp.MustCompile(`\{[^:}]+:\(\?:([^)]+)\)\}`)
	if match := altRe.FindStringSubmatch(pattern); match != nil {
		alternatives := strings.Split(match[1], "|")
		var patterns []string
		for _, alt := range alternatives {
			// Replace the {var:(?:...)} with the literal alternative
			expanded := altRe.ReplaceAllString(pattern, alt)
			// Recursively expand in case there are more alternatives
			patterns = append(patterns, expandPattern(expanded)...)
		}
		return patterns
	}

	// No alternatives found, just normalise the pattern
	return []string{normalisePattern(pattern)}
}

// Handle registers a handler for the given pattern.
// Returns a Route for method chaining with .Methods()
// Pattern should not include the prefix - it will be added automatically.
func (r *Router) Handle(pattern string, handler http.Handler) *Route {
	return &Route{
		router:   r,
		patterns: expandPattern(pattern),
		handler:  handler,
	}
}

// HandleFunc registers a handler function for the given pattern.
// Returns a Route for method chaining with .Methods().
func (r *Router) HandleFunc(pattern string, f func(http.ResponseWriter, *http.Request)) *Route {
	return r.Handle(pattern, http.HandlerFunc(f))
}

// PathPrefix creates a subrouter with the given path prefix.
// This allows for hierarchical routing similar to gorilla/mux.
func (r *Router) PathPrefix(prefix string) *SubrouterBuilder {
	return &SubrouterBuilder{
		parent: r,
		prefix: normalisePattern(prefix),
	}
}

// SubrouterBuilder helps create subrouters.
type SubrouterBuilder struct {
	parent *Router
	prefix string
}

// Subrouter creates a new Router that shares the parent's mux but with added prefix.
func (sb *SubrouterBuilder) Subrouter() *Router {
	return &Router{
		mux:    sb.parent.mux,
		prefix: sb.parent.prefix + strings.TrimSuffix(sb.prefix, "/"),
	}
}

// Handler registers a handler for this path prefix (catch-all).
func (sb *SubrouterBuilder) Handler(handler http.Handler) {
	pattern := sb.prefix
	if !strings.HasSuffix(pattern, "/") {
		pattern += "/"
	}
	pattern += "*"
	sb.parent.mux.Handle(pattern, handler)
}

// HandlerFunc registers a handler function for this path prefix.
func (sb *SubrouterBuilder) HandlerFunc(f func(http.ResponseWriter, *http.Request)) {
	sb.Handler(http.HandlerFunc(f))
}

// ServeHTTP implements http.Handler.
func (r *Router) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	r.mux.ServeHTTP(w, req)
}

// SetNotFoundHandler sets the handler for 404 responses.
func (r *Router) SetNotFoundHandler(handler http.Handler) {
	r.mux.NotFound(handler.ServeHTTP)
}

// SetMethodNotAllowedHandler sets the handler for 405 responses.
func (r *Router) SetMethodNotAllowedHandler(handler http.Handler) {
	r.mux.MethodNotAllowed(handler.ServeHTTP)
}

// Routers holds all the routers for different API paths.
type Routers struct {
	Client        *Router
	Federation    *Router
	Keys          *Router
	Media         *Router
	WellKnown     *Router
	Static        *Router
	DendriteAdmin *Router
	SynapseAdmin  *Router
}

// NewRouters creates all routers with their respective path prefixes.
func NewRouters() Routers {
	return Routers{
		Client:        NewRouter(PublicClientPathPrefix),
		Federation:    NewRouter(PublicFederationPathPrefix),
		Keys:          NewRouter(PublicKeyPathPrefix),
		Media:         NewRouter(PublicMediaPathPrefix),
		WellKnown:     NewRouter(PublicWellKnownPrefix),
		Static:        NewRouter(PublicStaticPath),
		DendriteAdmin: NewRouter(DendriteAdminPathPrefix),
		SynapseAdmin:  NewRouter(SynapseAdminPathPrefix),
	}
}

func notFoundHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept, Authorization")
	w.WriteHeader(http.StatusNotFound)
	unrecognizedErr, err := json.Marshal(spec.Unrecognized("Unrecognized request"))
	if err != nil {
		return
	}
	_, _ = w.Write(unrecognizedErr)
}

func methodNotAllowedHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept, Authorization")
	w.WriteHeader(http.StatusMethodNotAllowed)
	unrecognizedErr, err := json.Marshal(spec.Unrecognized("Unrecognized request"))
	if err != nil {
		return
	}
	_, _ = w.Write(unrecognizedErr)
}

// NotAllowedHandler is the handler for method not allowed responses (for backwards compatibility).
var NotAllowedHandler = http.HandlerFunc(methodNotAllowedHandler)

// NotFoundCORSHandler is the handler for not found responses (for backwards compatibility).
var NotFoundCORSHandler = http.HandlerFunc(notFoundHandler)
