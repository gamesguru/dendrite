package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

const (
	usageExitCode  = 2
	requestTimeout = 30 * time.Second
	username       = "migration-test"
	password       = "migration-test-password"
	message        = "created by Dendrite HEAD"
)

type client struct {
	baseURL     string
	accessToken string
	httpClient  *http.Client
}

func main() {
	baseURL := flag.String("url", "http://127.0.0.1:8008", "homeserver client API URL")
	flag.Parse()

	if flag.NArg() != 1 || (flag.Arg(0) != "seed" && flag.Arg(0) != "verify") {
		fmt.Fprintln(os.Stderr, "usage: dendrite-migration-test [-url URL] seed|verify")
		os.Exit(usageExitCode)
	}

	c := &client{
		baseURL:    strings.TrimRight(*baseURL, "/"),
		httpClient: &http.Client{Timeout: requestTimeout},
	}

	var err error
	if flag.Arg(0) == "seed" {
		err = c.seed()
	} else {
		err = c.verify()
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func (c *client) seed() error {
	var register struct {
		AccessToken string `json:"access_token"`
	}
	err := c.request(http.MethodPost, "/_matrix/client/v3/register", map[string]any{
		"auth": map[string]string{
			"type": "m.login.dummy",
		},
		"username": username,
		"password": password,
	}, &register)
	if err != nil {
		return fmt.Errorf("register migration user: %w", err)
	}
	c.accessToken = register.AccessToken

	var room struct {
		RoomID string `json:"room_id"`
	}
	err = c.request(http.MethodPost, "/_matrix/client/v3/createRoom", map[string]any{
		"room_alias_name": "migration-test",
		"preset":          "public_chat",
	}, &room)
	if err != nil {
		return fmt.Errorf("create migration room: %w", err)
	}

	txnID := fmt.Sprintf("%d", time.Now().UnixNano())
	path := fmt.Sprintf(
		"/_matrix/client/v3/rooms/%s/send/m.room.message/%s",
		url.PathEscape(room.RoomID),
		txnID,
	)
	err = c.request(http.MethodPut, path, map[string]string{
		"msgtype": "m.text",
		"body":    message,
	}, nil)
	if err != nil {
		return fmt.Errorf("send migration message: %w", err)
	}

	fmt.Printf("seeded user %q and room %q\n", username, room.RoomID)
	return nil
}

func (c *client) verify() error {
	var login struct {
		AccessToken string `json:"access_token"`
	}
	err := c.request(http.MethodPost, "/_matrix/client/v3/login", map[string]any{
		"type": "m.login.password",
		"identifier": map[string]string{
			"type": "m.id.user",
			"user": username,
		},
		"password": password,
	}, &login)
	if err != nil {
		return fmt.Errorf("login after migration: %w", err)
	}
	c.accessToken = login.AccessToken

	var directory struct {
		RoomID string `json:"room_id"`
	}
	err = c.request(
		http.MethodGet,
		"/_matrix/client/v3/directory/room/"+url.PathEscape("#migration-test:hs1"),
		nil,
		&directory,
	)
	if err != nil {
		return fmt.Errorf("look up room after migration: %w", err)
	}

	var messages struct {
		Chunk []struct {
			Type    string `json:"type"`
			Content struct {
				Body string `json:"body"`
			} `json:"content"`
		} `json:"chunk"`
	}
	path := fmt.Sprintf(
		"/_matrix/client/v3/rooms/%s/messages?dir=b&limit=100",
		url.PathEscape(directory.RoomID),
	)
	err = c.request(http.MethodGet, path, nil, &messages)
	if err != nil {
		return fmt.Errorf("read messages after migration: %w", err)
	}
	for _, event := range messages.Chunk {
		if event.Type == "m.room.message" && event.Content.Body == message {
			fmt.Printf("verified user, room, and message in %q\n", directory.RoomID)
			return nil
		}
	}
	return errors.New("migration message was not found")
}

func (c *client) request(method, path string, requestBody, responseBody any) error {
	var body io.Reader
	if requestBody != nil {
		data, err := json.Marshal(requestBody)
		if err != nil {
			return err
		}
		body = bytes.NewReader(data)
	}

	req, err := http.NewRequest(method, c.baseURL+path, body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if c.accessToken != "" {
		req.Header.Set("Authorization", "Bearer "+c.accessToken)
	}

	res, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	data, err := io.ReadAll(res.Body)
	if err != nil {
		return err
	}
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return fmt.Errorf("%s %s returned %s: %s", method, path, res.Status, data)
	}
	if responseBody != nil {
		if err = json.Unmarshal(data, responseBody); err != nil {
			return fmt.Errorf("decode response: %w", err)
		}
	}
	return nil
}
