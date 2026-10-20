// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package internal

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/matrix-org/gomatrix"
	"github.com/sirupsen/logrus"

	"codefloe.com/pat-s/zendrite/federationapi/api"
	"codefloe.com/pat-s/zendrite/federationapi/statistics"
	"codefloe.com/pat-s/zendrite/roomserver/version"
)

// PerformKnock implements api.FederationInternalAPI. It performs the
// make_knock and send_knock dance with the first server that completes it.
func (r *FederationInternalAPI) PerformKnock(
	ctx context.Context,
	request *api.PerformKnockRequest,
	response *api.PerformKnockResponse,
) error {
	userID, err := spec.NewUserID(request.UserID, true)
	if err != nil {
		return err
	}
	roomID, err := spec.NewRoomID(request.RoomID)
	if err != nil {
		return err
	}

	// Deduplicate the server names but keep their order, which reflects how
	// likely each server is to respond.
	seen := make(map[spec.ServerName]bool)
	var servers []spec.ServerName
	for _, srv := range request.ServerNames {
		if seen[srv] || r.cfg.Matrix.IsLocalServerName(srv) {
			continue
		}
		seen[srv] = true
		servers = append(servers, srv)
	}

	var lastErr error
	for _, serverName := range servers {
		if !r.shouldAttemptDirectFederation(serverName) { //nolint:contextcheck
			continue
		}
		if err = r.performKnockUsingServer(ctx, *roomID, *userID, request.Content, serverName, response); err != nil {
			logrus.WithError(err).WithFields(logrus.Fields{
				"server_name": serverName,
				"room_id":     request.RoomID,
			}).Warn("Failed to knock on room through server")
			lastErr = err
			continue
		}
		response.KnockedVia = serverName
		return nil
	}

	if lastErr == nil {
		lastErr = fmt.Errorf("no servers to knock through")
	}
	response.LastError = federationHTTPError(lastErr)
	return nil
}

func (r *FederationInternalAPI) performKnockUsingServer(
	ctx context.Context,
	roomID spec.RoomID,
	userID spec.UserID,
	content map[string]any,
	serverName spec.ServerName,
	response *api.PerformKnockResponse,
) error {
	respMakeKnock, err := r.federation.MakeKnock(
		ctx, userID.Domain(), serverName, roomID.String(), userID.String(), knockRoomVersions(),
	)
	if err != nil {
		r.statistics.ForServer(serverName).Failure() //nolint:contextcheck
		return fmt.Errorf("r.federation.MakeKnock: %w", err)
	}

	verImpl, err := version.SupportedRoomVersion(respMakeKnock.RoomVersion)
	if err != nil {
		return err
	}
	if verImpl.Version() == gomatrixserverlib.RoomVersionPseudoIDs {
		return fmt.Errorf("knocking is not supported in room version %s", verImpl.Version())
	}

	// Pin the fields that identify the knock rather than trusting the
	// remote server's template for them.
	senderID := userID.String()
	respMakeKnock.KnockEvent.Type = spec.MRoomMember
	respMakeKnock.KnockEvent.SenderID = senderID
	respMakeKnock.KnockEvent.StateKey = &senderID
	respMakeKnock.KnockEvent.RoomID = roomID.String()
	respMakeKnock.KnockEvent.Redacts = ""
	eventContent := map[string]any{}
	for k, v := range content {
		eventContent[k] = v
	}
	eventContent["membership"] = spec.Knock
	if err = respMakeKnock.KnockEvent.SetContent(eventContent); err != nil {
		return fmt.Errorf("respMakeKnock.KnockEvent.SetContent: %w", err)
	}
	if err = respMakeKnock.KnockEvent.SetUnsigned(struct{}{}); err != nil {
		return fmt.Errorf("respMakeKnock.KnockEvent.SetUnsigned: %w", err)
	}

	identity, err := r.cfg.Matrix.SigningIdentityFor(userID.Domain())
	if err != nil {
		return err
	}
	event, err := verImpl.NewEventBuilderFromProtoEvent(&respMakeKnock.KnockEvent).Build(
		time.Now(), userID.Domain(), identity.KeyID, identity.PrivateKey,
	)
	if err != nil {
		return fmt.Errorf("failed to build knock event: %w", err)
	}

	respSendKnock, err := r.federation.SendKnock(ctx, userID.Domain(), serverName, event)
	if err != nil {
		r.statistics.ForServer(serverName).Failure() //nolint:contextcheck
		return fmt.Errorf("r.federation.SendKnock: %w", err)
	}
	r.statistics.ForServer(serverName).Success(statistics.SendDirect) //nolint:contextcheck

	// MSC4311: the knock has already happened, so entries which fail
	// validation are dropped rather than failing the knock.
	userIDForSender := func(roomID spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) {
		return r.rsAPI.QueryUserIDForSender(ctx, roomID, senderID)
	}
	state, err := gomatrixserverlib.ValidateStrippedStatePDUs(
		ctx, roomID, verImpl.Version(), respSendKnock.KnockRoomState, r.keyRing, userIDForSender,
	)
	if err != nil {
		logrus.WithError(err).WithFields(logrus.Fields{
			"server_name": serverName,
			"room_id":     roomID.String(),
		}).Warn("Dropped knock_room_state entries which failed validation")
	}

	response.Event = event
	response.KnockRoomState = state
	return nil
}

// knockRoomVersions lists the room versions we can knock in, for the ver
// parameter of make_knock.
func knockRoomVersions() []gomatrixserverlib.RoomVersion {
	var versions []gomatrixserverlib.RoomVersion
	for v, impl := range version.SupportedRoomVersions() {
		if v == gomatrixserverlib.RoomVersionPseudoIDs {
			continue
		}
		if impl.CheckKnockingAllowed(string(v), "", "", spec.Knock, spec.Leave) == nil {
			versions = append(versions, v)
		}
	}
	sort.Slice(versions, func(i, j int) bool { return versions[i] < versions[j] })
	return versions
}

// federationHTTPError converts a federation error into the form the client
// API passes through, so the client sees the remote server's response.
func federationHTTPError(err error) *gomatrix.HTTPError {
	var specErr spec.HTTPError
	if errors.As(err, &specErr) {
		return &gomatrix.HTTPError{Code: specErr.Code, Message: string(specErr.Contents), Contents: specErr.Contents}
	}
	var gomatrixErr gomatrix.HTTPError
	if errors.As(err, &gomatrixErr) {
		gomatrixErr.Message = string(gomatrixErr.Contents)
		return &gomatrixErr
	}
	return &gomatrix.HTTPError{Message: err.Error()}
}
