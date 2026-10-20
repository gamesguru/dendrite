// Copyright 2026 Patrick Schratz
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package routing

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"syscall"
	"time"

	"codefloe.com/pat-s/zendrite/mediaapi/types"
	"codefloe.com/pat-s/zendrite/setup/config"
)

const (
	// MaxMediaRedirects is how many further redirects a media Location may take.
	maxMediaRedirects = 5
	// MediaRedirectHeaderTimeout bounds the wait for the redirect target to
	// start answering. Reading the body is bounded by the request context and
	// the maximum file size.
	mediaRedirectHeaderTimeout = 30 * time.Second
	mediaRedirectDialTimeout   = 10 * time.Second
)

// sharedAddressSpace is RFC 6598 carrier-grade NAT space, which net.IP
// doesn't count as private but which is commonly used for internal networks.
var sharedAddressSpace = netip.MustParsePrefix("100.64.0.0/10")

// mediaRedirectClient fetches the media a remote server's federation media
// response points at with a Location header (MSC3860). It is a plain HTTP
// client rather than the federation client, which would resolve the host as a
// Matrix server name. As the URL comes from a remote server, it only connects
// to public addresses, so a remote can't make us fetch internal services.
var mediaRedirectClient = newMediaRedirectClient(false)

func newMediaRedirectClient(allowNonPublic bool) *http.Client {
	dialer := &net.Dialer{Timeout: mediaRedirectDialTimeout}
	if !allowNonPublic {
		// Checking the address being dialed, rather than the resolved host
		// name, also covers redirects and DNS rebinding.
		dialer.Control = func(_, address string, _ syscall.RawConn) error {
			host, _, err := net.SplitHostPort(address)
			if err != nil {
				return err
			}
			if ip := net.ParseIP(host); ip == nil || !isPublicIP(ip) {
				return fmt.Errorf("refusing to fetch media from non-public address %s", host)
			}
			return nil
		}
	}
	return &http.Client{
		Transport: &http.Transport{
			// No proxy: it would connect to the target on our behalf and
			// bypass the address check.
			Proxy:                 nil,
			DialContext:           dialer.DialContext,
			TLSHandshakeTimeout:   mediaRedirectDialTimeout,
			ResponseHeaderTimeout: mediaRedirectHeaderTimeout,
		},
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) > maxMediaRedirects {
				return errors.New("too many media redirects")
			}
			return checkMediaRedirectURL(req.URL)
		},
	}
}

func isPublicIP(ip net.IP) bool {
	addr, ok := netip.AddrFromSlice(ip)
	if !ok {
		return false
	}
	addr = addr.Unmap()
	return addr.IsGlobalUnicast() && !addr.IsPrivate() && !sharedAddressSpace.Contains(addr)
}

func checkMediaRedirectURL(u *url.URL) error {
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("unsupported media redirect scheme %q", u.Scheme)
	}
	if u.Host == "" {
		return errors.New("media redirect has no host")
	}
	return nil
}

// fetchMediaRedirect downloads the media at location, which a remote server
// returned in place of the media in its federation media response.
func (r *downloadRequest) fetchMediaRedirect(ctx context.Context, location string, maxFileSizeBytes config.FileSizeBytes) (*mediaPart, error) {
	u, err := url.Parse(location)
	if err != nil {
		return nil, fmt.Errorf("invalid media redirect: %w", err)
	}
	if err = checkMediaRedirectURL(u); err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	r.Logger.WithField("location", u.Redacted()).Debug("Following media redirect")
	resp, err := mediaRedirectClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to follow media redirect: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("media redirect returned HTTP %d", resp.StatusCode)
	}
	contentLength, reader, err := r.GetContentLengthAndReader(resp.Header.Get("Content-Length"), resp.Body, maxFileSizeBytes)
	if err != nil {
		resp.Body.Close()
		return nil, err
	}
	r.MediaMetadata.ContentType = types.ContentType(resp.Header.Get("Content-Type"))
	return &mediaPart{
		contentLength: contentLength,
		reader:        reader,
		disposition:   resp.Header.Get("Content-Disposition"),
		body:          resp.Body,
	}, nil
}
