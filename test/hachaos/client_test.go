//go:build linux

package hachaos

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"
)

// mxClient is a minimal Matrix client. It keeps no state beyond its access
// token, so every retry decision is visible in the workload rather than hidden
// in a library.
type mxClient struct {
	base   string
	http   *http.Client
	user   string
	token  string
	device string
	// unavailable, when set, counts HTTP 503 responses.
	unavailable *atomic.Int64
}

// mxError is a Matrix error response.
type mxError struct {
	Status  int
	Code    string `json:"errcode"`
	Message string `json:"error"`
}

func (e *mxError) Error() string { return fmt.Sprintf("HTTP %d %s: %s", e.Status, e.Code, e.Message) }

// retryable reports whether a client should retry a failed request unchanged.
// Transport failures and 5xx are what clients see during takeover.
func retryable(err error) bool {
	var mx *mxError
	if errors.As(err, &mx) {
		return mx.Status >= http.StatusInternalServerError || mx.Status == http.StatusTooManyRequests
	}
	return err != nil
}

// setupRequestTimeout bounds one setup request, such as creating the
// workload's rooms. Setup is not what a scenario qualifies, and on a loaded
// host a new room's replicated JetStream consumers can take longer to come up
// than a steady-state request is allowed (#438). A setup step that is merely
// slow is noted in the report rather than failing the scenario.
const setupRequestTimeout = 3 * time.Minute

// slowSetupStep is how long a setup step may take before the report notes it.
const slowSetupStep = 30 * time.Second

// patient returns a copy of the client whose requests may take up to
// setupRequestTimeout.
func (c *mxClient) patient() *mxClient {
	cp := *c
	cp.http = &http.Client{Timeout: setupRequestTimeout, Transport: c.http.Transport}
	return &cp
}

func newClient(base string) *mxClient {
	return &mxClient{base: base, http: &http.Client{Timeout: 60 * time.Second, Transport: &http.Transport{MaxIdleConnsPerHost: 16, IdleConnTimeout: 30 * time.Second}}}
}

// withToken acts as another client's device, on this client's base URL.
func (c *mxClient) withToken(other *mxClient) *mxClient {
	c.user, c.token, c.device = other.user, other.token, other.device
	return c
}

func (c *mxClient) do(ctx context.Context, method, path string, body, out any) error {
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(encoded)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, reader)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode == http.StatusServiceUnavailable && c.unavailable != nil {
		c.unavailable.Add(1)
	}
	if resp.StatusCode != http.StatusOK {
		mx := &mxError{Status: resp.StatusCode}
		_ = json.Unmarshal(data, mx)
		return mx
	}
	if out != nil {
		return json.Unmarshal(data, out)
	}
	return nil
}

// register creates an account with the dummy flow, completing the
// user-interactive authentication round trip.
func register(ctx context.Context, base, username string) (*mxClient, error) {
	c := newClient(base)
	body := map[string]any{"username": username, "password": "hachaos-" + username, "initial_device_display_name": username}
	var first struct {
		Session string `json:"session"`
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/_matrix/client/v3/register", strings.NewReader(mustJSON(body)))
	if err != nil {
		return nil, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	data, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		return nil, fmt.Errorf("register %s: expected an authentication challenge, got HTTP %d: %s", username, resp.StatusCode, data)
	}
	if err = json.Unmarshal(data, &first); err != nil {
		return nil, err
	}
	body["auth"] = map[string]any{"type": "m.login.dummy", "session": first.Session}
	var out struct {
		UserID      string `json:"user_id"`
		AccessToken string `json:"access_token"`
		DeviceID    string `json:"device_id"`
	}
	if err = c.do(ctx, http.MethodPost, "/_matrix/client/v3/register", body, &out); err != nil {
		return nil, fmt.Errorf("register %s: %w", username, err)
	}
	c.user, c.token, c.device = out.UserID, out.AccessToken, out.DeviceID
	return c, nil
}

func mustJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return string(b)
}

func (c *mxClient) createRoom(ctx context.Context, name string) (string, error) {
	var out struct {
		RoomID string `json:"room_id"`
	}
	err := c.do(ctx, http.MethodPost, "/_matrix/client/v3/createRoom", map[string]any{"preset": "public_chat", "name": name, "visibility": "private"}, &out)
	return out.RoomID, err
}

func (c *mxClient) join(ctx context.Context, room, via string) error {
	path := "/_matrix/client/v3/join/" + url.PathEscape(room)
	if via != "" {
		path += "?server_name=" + url.QueryEscape(via)
	}
	return c.do(ctx, http.MethodPost, path, map[string]any{}, nil)
}

func (c *mxClient) send(ctx context.Context, room, txn, body string) (string, error) {
	var out struct {
		EventID string `json:"event_id"`
	}
	path := fmt.Sprintf("/_matrix/client/v3/rooms/%s/send/m.room.message/%s", url.PathEscape(room), url.PathEscape(txn))
	err := c.do(ctx, http.MethodPut, path, map[string]any{"msgtype": "m.text", "body": body}, &out)
	return out.EventID, err
}

func (c *mxClient) sendToDevice(ctx context.Context, txn, user, device, nonce string) error {
	path := "/_matrix/client/v3/sendToDevice/org.zendrite.hachaos/" + url.PathEscape(txn)
	return c.do(ctx, http.MethodPut, path, map[string]any{"messages": map[string]any{user: map[string]any{device: map[string]any{"nonce": nonce}}}}, nil)
}

func (c *mxClient) receipt(ctx context.Context, room, eventID string) error {
	path := fmt.Sprintf("/_matrix/client/v3/rooms/%s/receipt/m.read/%s", url.PathEscape(room), url.PathEscape(eventID))
	return c.do(ctx, http.MethodPost, path, map[string]any{}, nil)
}

type syncResponse struct {
	NextBatch string `json:"next_batch"`
	Rooms     struct {
		Join map[string]struct {
			Timeline struct {
				Events    []roomEvent `json:"events"`
				Limited   bool        `json:"limited"`
				PrevBatch string      `json:"prev_batch"`
			} `json:"timeline"`
			Ephemeral struct {
				Events []struct {
					Type    string                                           `json:"type"`
					Content map[string]map[string]map[string]json.RawMessage `json:"content"`
				} `json:"events"`
			} `json:"ephemeral"`
		} `json:"join"`
	} `json:"rooms"`
	ToDevice struct {
		Events []struct {
			Type    string `json:"type"`
			Sender  string `json:"sender"`
			Content struct {
				Nonce string `json:"nonce"`
			} `json:"content"`
		} `json:"events"`
	} `json:"to_device"`
}

type roomEvent struct {
	EventID string `json:"event_id"`
	Type    string `json:"type"`
	Sender  string `json:"sender"`
	Content struct {
		Body string `json:"body"`
	} `json:"content"`
}

// timelineFilter asks for enough timeline that a long outage is not truncated.
const timelineFilter = `{"room":{"timeline":{"limit":500}}}`

func (c *mxClient) sync(ctx context.Context, since string, timeout time.Duration) (*syncResponse, error) {
	q := url.Values{"timeout": {fmt.Sprint(timeout.Milliseconds())}, "filter": {timelineFilter}}
	if since != "" {
		q.Set("since", since)
	}
	var out syncResponse
	err := c.do(ctx, http.MethodGet, "/_matrix/client/v3/sync?"+q.Encode(), nil, &out)
	return &out, err
}

// page fetches one page of a room's history backwards from a token.
func (c *mxClient) page(ctx context.Context, room, from string) ([]roomEvent, string, error) {
	return c.pageWithLimit(ctx, room, from, 100)
}

// pageWithLimit fetches one page backwards from a token, or from the end of
// the room when from is empty.
func (c *mxClient) pageWithLimit(ctx context.Context, room, from string, limit int) ([]roomEvent, string, error) {
	q := url.Values{"dir": {"b"}, "limit": {fmt.Sprint(limit)}}
	if from != "" {
		q.Set("from", from)
	}
	var page struct {
		Chunk []roomEvent `json:"chunk"`
		End   string      `json:"end"`
	}
	err := c.do(ctx, http.MethodGet, fmt.Sprintf("/_matrix/client/v3/rooms/%s/messages?%s", url.PathEscape(room), q.Encode()), nil, &page)
	return page.Chunk, page.End, err
}

// messages pages a room's whole history, newest first.
func (c *mxClient) messages(ctx context.Context, room string) ([]roomEvent, error) {
	var all []roomEvent
	from := ""
	for range 1000 {
		q := url.Values{"dir": {"b"}, "limit": {"500"}}
		if from != "" {
			q.Set("from", from)
		}
		var page struct {
			Chunk []roomEvent `json:"chunk"`
			End   string      `json:"end"`
		}
		if err := c.do(ctx, http.MethodGet, fmt.Sprintf("/_matrix/client/v3/rooms/%s/messages?%s", url.PathEscape(room), q.Encode()), nil, &page); err != nil {
			return nil, err
		}
		all = append(all, page.Chunk...)
		if page.End == "" || len(page.Chunk) == 0 {
			return all, nil
		}
		from = page.End
	}
	return all, errors.New("room history did not end")
}

func (c *mxClient) upload(ctx context.Context, content []byte) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+"/_matrix/media/v3/upload?filename=hachaos.bin", bytes.NewReader(content))
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Content-Type", "application/octet-stream")
	resp, err := c.http.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		mx := &mxError{Status: resp.StatusCode}
		_ = json.Unmarshal(data, mx)
		return "", mx
	}
	var out struct {
		ContentURI string `json:"content_uri"`
	}
	if err = json.Unmarshal(data, &out); err != nil {
		return "", err
	}
	return out.ContentURI, nil
}

func (c *mxClient) download(ctx context.Context, mxc string) ([]byte, error) {
	server, media, ok := strings.Cut(strings.TrimPrefix(mxc, "mxc://"), "/")
	if !ok {
		return nil, fmt.Errorf("invalid content URI %q", mxc)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("%s/_matrix/client/v1/media/download/%s/%s", c.base, url.PathEscape(server), url.PathEscape(media)), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, &mxError{Status: resp.StatusCode, Message: string(data)}
	}
	return data, nil
}

// search reports the event IDs whose body matches term.
func (c *mxClient) search(ctx context.Context, term string) ([]string, error) {
	var out struct {
		Results struct {
			RoomEvents struct {
				Results []struct {
					Result roomEvent `json:"result"`
				} `json:"results"`
			} `json:"room_events"`
		} `json:"search_categories"`
	}
	body := map[string]any{"search_categories": map[string]any{"room_events": map[string]any{"search_term": term, "keys": []string{"content.body"}}}}
	if err := c.do(ctx, http.MethodPost, "/_matrix/client/v3/search", body, &out); err != nil {
		return nil, err
	}
	var ids []string
	for _, r := range out.Results.RoomEvents.Results {
		ids = append(ids, r.Result.EventID)
	}
	return ids, nil
}

func (c *mxClient) versions(ctx context.Context) error {
	return c.do(ctx, http.MethodGet, "/_matrix/client/versions", nil, nil)
}
