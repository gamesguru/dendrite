// Copyright 2024 New Vector Ltd.
// Copyright 2022 The Matrix.org Foundation C.I.C.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package internal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/gamesguru/gomatrixserverlib/fclient"
	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/getsentry/sentry-go"
	"github.com/matrix-org/util"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"

	"codefloe.com/pat-s/zendrite/federationapi/producers"
	"codefloe.com/pat-s/zendrite/federationapi/types"
	"codefloe.com/pat-s/zendrite/internal/msc4155"
	"codefloe.com/pat-s/zendrite/roomserver/api"
	rstypes "codefloe.com/pat-s/zendrite/roomserver/types"
	syncTypes "codefloe.com/pat-s/zendrite/syncapi/types"
	userAPI "codefloe.com/pat-s/zendrite/userapi/api"
)

var (
	PDUCountTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "zendrite",
			Subsystem: "federationapi",
			Name:      "recv_pdus",
			Help:      "Number of incoming PDUs from remote servers with labels for success",
		},
		[]string{"status"}, // 'success' or 'total'
	)
	EDUCountTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "zendrite",
			Subsystem: "federationapi",
			Name:      "recv_edus",
			Help:      "Number of incoming EDUs from remote servers",
		},
	)
	// TxnCountTotal counts inbound transactions by outcome: "ok" when the
	// sender was told they were processed, "retry" when they were refused
	// so the sender sends them again.
	TxnCountTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "zendrite",
			Subsystem: "federationapi",
			Name:      "recv_transactions_total",
			Help:      "Inbound federation transactions by outcome: ok, or retry when they were refused because an event or EDU could not be queued",
		},
		[]string{"outcome"},
	)
)

// errQueue marks a failure to hand an event or EDU to the broker. It says
// nothing about the item itself, so the transaction is refused and its sender
// retries it, rather than told it was processed while the item was dropped.
var errQueue = errors.New("could not queue")

// queueFailures collects the queueing failures of one transaction, which the
// PDU loop and the EDU goroutine record concurrently.
type queueFailures struct {
	mu   sync.Mutex
	errs []error
}

func (q *queueFailures) add(what string, err error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.errs = append(q.errs, fmt.Errorf("%w %s: %w", errQueue, what, err))
}

func (q *queueFailures) err() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	return errors.Join(q.errs...)
}

type TxnReq struct {
	gomatrixserverlib.Transaction
	rsAPI                  api.FederationRoomserverAPI
	userAPI                userAPI.FederationUserAPI
	ourServerName          spec.ServerName
	keys                   gomatrixserverlib.JSONVerifier
	roomsMu                *MutexByRoom
	producer               *producers.SyncAPIProducer
	inboundPresenceEnabled bool
}

func NewTxnReq(
	rsAPI api.FederationRoomserverAPI,
	userAPI userAPI.FederationUserAPI,
	ourServerName spec.ServerName,
	keys gomatrixserverlib.JSONVerifier,
	roomsMu *MutexByRoom,
	producer *producers.SyncAPIProducer,
	inboundPresenceEnabled bool,
	pdus []json.RawMessage,
	edus []gomatrixserverlib.EDU,
	origin spec.ServerName,
	transactionID gomatrixserverlib.TransactionID,
	destination spec.ServerName,
) TxnReq {
	t := TxnReq{
		rsAPI:                  rsAPI,
		userAPI:                userAPI,
		ourServerName:          ourServerName,
		keys:                   keys,
		roomsMu:                roomsMu,
		producer:               producer,
		inboundPresenceEnabled: inboundPresenceEnabled,
	}

	t.PDUs = pdus
	t.EDUs = edus
	t.Origin = origin
	t.TransactionID = transactionID
	t.Destination = destination

	return t
}

// ProcessTransaction hands the transaction's events and EDUs to the broker.
//
// An event that fails its checks is reported in the per-event results, as the
// specification requires. An event or a durable EDU that could not be queued,
// because the broker did not take it, is a failure of this server, not of the
// item: the whole transaction is refused with HTTP 503 so that the sender
// retries it. Retrying is safe: the roomserver ignores events it already has,
// receipts and device updates are idempotent, and to-device messages carry a
// stable message ID that the stream deduplicates.
func (t *TxnReq) ProcessTransaction(ctx context.Context) (*fclient.RespSend, *util.JSONResponse) {
	var failures queueFailures
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if t.producer != nil {
			t.processEDUs(ctx, &failures)
		}
	}()

	results := make(map[string]fclient.PDUResult)
	roomVersions := make(map[string]gomatrixserverlib.RoomVersion)
	getRoomVersion := func(roomID string) gomatrixserverlib.RoomVersion {
		if v, ok := roomVersions[roomID]; ok {
			return v
		}
		roomVersion, err := t.rsAPI.QueryRoomVersionForRoom(ctx, roomID)
		if err != nil {
			util.GetLogger(ctx).WithError(err).Debug("Transaction: Failed to query room version for room", roomID)
			return ""
		}
		roomVersions[roomID] = roomVersion
		return roomVersion
	}

	for _, pdu := range t.PDUs {
		PDUCountTotal.WithLabelValues("total").Inc()
		var header struct {
			RoomID string `json:"room_id"`
		}
		if err := json.Unmarshal(pdu, &header); err != nil {
			util.GetLogger(ctx).WithError(err).Debug("Transaction: Failed to extract room ID from event")
			// We don't know the event ID at this point so we can't return the
			// failure in the PDU results
			continue
		}
		roomVersion := getRoomVersion(header.RoomID)
		verImpl, err := gomatrixserverlib.GetRoomVersion(roomVersion)
		if err != nil {
			continue
		}
		event, err := verImpl.NewEventFromUntrustedJSON(pdu)
		if err != nil {
			/* Do not reject the entire transaction for a single bad PDU, that's dumb.

			if _, ok := err.(gomatrixserverlib.BadJSONError); ok {
				// Room version 6 states that homeservers should strictly enforce canonical JSON
				// on PDUs.
				//
				// This enforces that the entire transaction is rejected if a single bad PDU is
				// sent. It is unclear if this is the correct behavior or not.
				//
				// See https://github.com/matrix-org/synapse/issues/7543
				return nil, &util.JSONResponse{
					Code: 400,
					JSON: spec.BadJSON("PDU contains bad JSON"),
				}
			} */
			util.GetLogger(ctx).WithError(err).Debugf("Transaction: Failed to parse event JSON of event %s", string(pdu))
			continue
		}
		if event.Type() == spec.MRoomCreate && event.StateKeyEquals("") {
			continue
		}
		if api.IsServerBannedFromRoom(ctx, t.rsAPI, event.RoomID().String(), t.Origin) {
			results[event.EventID()] = fclient.PDUResult{
				Error: "Forbidden by server ACLs",
			}
			continue
		}
		if err = gomatrixserverlib.VerifyEventSignatures(ctx, event, t.keys, func(roomID spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) {
			return t.rsAPI.QueryUserIDForSender(ctx, roomID, senderID)
		}); err != nil {
			util.GetLogger(ctx).WithError(err).Debugf("Transaction: Couldn't validate signature of event %q", event.EventID())
			results[event.EventID()] = fclient.PDUResult{
				Error: err.Error(),
			}
			continue
		}

		// MSC4155: an ignored or blocked invite must not become visible to
		// the invitee. The /invite endpoint rejects blocked invites, but the
		// inviter then also sends the signed event in a transaction, so drop
		// it here as well. Reporting success is important: an error would
		// make the sender retry forever and leak that the invite was ignored.
		dropInvite, filterErr := t.shouldDropFilteredInvite(ctx, event)
		if filterErr != nil {
			failures.add("invite rules for event "+event.EventID(), filterErr)
			results[event.EventID()] = fclient.PDUResult{Error: filterErr.Error()}
			continue
		}
		if dropInvite {
			results[event.EventID()] = fclient.PDUResult{}
			PDUCountTotal.WithLabelValues("success").Inc()
			continue
		}

		// A kick or ban ending a local user's invite or knock can reach us
		// for a room we are not in, where it cannot be authorized as usual.
		if event.Type() == spec.MRoomMember {
			retracted, retractErr := t.rsAPI.RetractPendingMembership(ctx, &rstypes.HeaderedEvent{PDU: event})
			if retractErr != nil {
				util.GetLogger(ctx).WithError(retractErr).Warnf("Transaction: Couldn't retract pending membership for event %q", event.EventID())
			}
			if retracted {
				results[event.EventID()] = fclient.PDUResult{}
				PDUCountTotal.WithLabelValues("success").Inc()
				continue
			}
		}

		// pass the event to the roomserver which will do auth checks
		// If the event fail auth checks, gmsl.NotAllowed error will be returned which we be silently
		// discarded by the caller of this function
		if err = api.SendEvents(
			ctx,
			t.rsAPI,
			api.KindNew,
			[]*rstypes.HeaderedEvent{
				{PDU: event},
			},
			t.Destination,
			t.Origin,
			api.DoNotSendToOtherServers,
			nil,
			true,
		); err != nil {
			util.GetLogger(ctx).WithError(err).Errorf("Transaction: Couldn't submit event %q to input queue: %s", event.EventID(), err)
			failures.add("event "+event.EventID(), err)
			results[event.EventID()] = fclient.PDUResult{
				Error: err.Error(),
			}
			continue
		}

		results[event.EventID()] = fclient.PDUResult{}
		PDUCountTotal.WithLabelValues("success").Inc()
	}

	wg.Wait()
	if err := failures.err(); err != nil {
		TxnCountTotal.WithLabelValues("retry").Inc()
		util.GetLogger(ctx).WithError(err).WithFields(logrus.Fields{
			"origin": t.Origin,
			"txn_id": t.TransactionID,
		}).Warn("Refusing federation transaction for a retry")
		return nil, &util.JSONResponse{
			Code: http.StatusServiceUnavailable,
			JSON: spec.Unknown("The transaction could not be queued; retry it"),
		}
	}
	TxnCountTotal.WithLabelValues("ok").Inc()
	return &fclient.RespSend{PDUs: results}, nil
}

// shouldDropFilteredInvite reports whether event is an invite to a local user
// whose MSC4155 invite rules block or ignore the sender.
func (t *TxnReq) shouldDropFilteredInvite(ctx context.Context, event gomatrixserverlib.PDU) (bool, error) {
	if event.Type() != spec.MRoomMember || event.StateKey() == nil {
		return false, nil
	}
	var content struct {
		Membership string `json:"membership"`
	}
	if err := json.Unmarshal(event.Content(), &content); err != nil || content.Membership != "invite" {
		return false, nil
	}

	invitedUser, ok, err := t.localUserForStateKey(ctx, event.RoomID(), *event.StateKey())
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}
	sender, err := t.rsAPI.QueryUserIDForSender(ctx, event.RoomID(), event.SenderID())
	if err != nil {
		return false, err
	}
	if sender == nil {
		return false, nil
	}

	blocked, ignored, err := msc4155.Disposition(ctx, t.userAPI, invitedUser, sender.String())
	if err != nil {
		util.GetLogger(ctx).WithError(err).WithField("event_id", event.EventID()).Warn("Transaction: failed to apply invite rules")
		return false, err
	}
	if blocked || ignored {
		util.GetLogger(ctx).WithFields(logrus.Fields{
			"event_id": event.EventID(),
			"invitee":  invitedUser.String(),
			"sender":   sender.String(),
			"blocked":  blocked,
		}).Debug("Transaction: dropping filtered invite")
		return true, nil
	}
	return false, nil
}

func (t *TxnReq) localUserForStateKey(ctx context.Context, roomID spec.RoomID, stateKey string) (spec.UserID, bool, error) {
	if userID, err := spec.NewUserID(stateKey, true); err == nil {
		if userID.Domain() == t.ourServerName {
			return *userID, true, nil
		}
		return spec.UserID{}, false, nil
	}
	// Room versions with pseudo-IDs may store a sender ID as the state key.
	userID, err := t.rsAPI.QueryUserIDForSender(ctx, roomID, spec.SenderID(stateKey))
	if err != nil {
		return spec.UserID{}, false, err
	}
	if userID != nil && userID.Domain() == t.ourServerName {
		return *userID, true, nil
	}
	return spec.UserID{}, false, nil
}

// processEDUs queues the transaction's EDUs. Typing notifications and
// presence are best effort: they expire or are superseded within seconds, so
// losing one costs less than retrying the transaction. To-device messages,
// receipts, device list updates and signing key updates are not resent by
// the sender unless the transaction fails, so failing to queue one is recorded
// in failures.
//
//nolint:gocyclo
func (t *TxnReq) processEDUs(ctx context.Context, failures *queueFailures) {
	for _, e := range t.EDUs {
		EDUCountTotal.Inc()
		switch e.Type {
		case spec.MTyping:
			// https://matrix.org/docs/spec/server_server/latest#typing-notifications
			var typingPayload struct {
				RoomID string `json:"room_id"`
				UserID string `json:"user_id"`
				Typing bool   `json:"typing"`
			}
			if err := json.Unmarshal(e.Content, &typingPayload); err != nil {
				util.GetLogger(ctx).WithError(err).Debug("Failed to unmarshal typing event")
				continue
			}
			_, serverName, err := gomatrixserverlib.SplitID('@', typingPayload.UserID)
			switch {
			case err != nil:
				continue
			case serverName == t.ourServerName:
				continue
			case serverName != t.Origin:
				continue
			}
			if api.IsServerBannedFromRoom(ctx, t.rsAPI, typingPayload.RoomID, serverName) {
				continue
			}
			if err := t.producer.SendTyping(ctx, typingPayload.UserID, typingPayload.RoomID, typingPayload.Typing, 30*1000); err != nil { //nolint:mnd
				util.GetLogger(ctx).WithError(err).Error("Failed to send typing event to JetStream")
			}
		case spec.MDirectToDevice:
			// https://matrix.org/docs/spec/server_server/r0.1.3#m-direct-to-device-schema
			var directPayload gomatrixserverlib.ToDeviceMessage
			if err := json.Unmarshal(e.Content, &directPayload); err != nil {
				util.GetLogger(ctx).WithError(err).Debug("Failed to unmarshal send-to-device events")
				continue
			}
			if _, serverName, err := gomatrixserverlib.SplitID('@', directPayload.Sender); err != nil {
				continue
			} else if serverName == t.ourServerName {
				continue
			} else if serverName != t.Origin {
				continue
			}
			for userID, byUser := range directPayload.Messages {
				for deviceID, message := range byUser {
					// TODO: check that the user and the device actually exist here
					if err := t.producer.SendToDevice(ctx, directPayload.Sender, userID, deviceID, directPayload.Type, directPayload.MessageID, message); err != nil {
						failures.add("to-device message "+directPayload.MessageID, err)
						sentry.CaptureException(err)
						util.GetLogger(ctx).WithError(err).WithFields(logrus.Fields{
							"sender":    directPayload.Sender,
							"user_id":   userID,
							"device_id": deviceID,
						}).Error("Failed to send send-to-device event to JetStream")
					}
				}
			}
		case spec.MDeviceListUpdate:
			if err := t.producer.SendDeviceListUpdate(ctx, e.Content, t.Origin); err != nil {
				failures.add("device list update", err)
				sentry.CaptureException(err)
				util.GetLogger(ctx).WithError(err).Error("failed to InputDeviceListUpdate")
			}
		case spec.MReceipt:
			// https://matrix.org/docs/spec/server_server/r0.1.4#receipts
			payload := map[string]types.FederationReceiptMRead{}

			if err := json.Unmarshal(e.Content, &payload); err != nil {
				util.GetLogger(ctx).WithError(err).Debug("Failed to unmarshal receipt event")
				continue
			}

			for roomID, receipt := range payload {
				for userID, mread := range receipt.User {
					_, domain, err := gomatrixserverlib.SplitID('@', userID)
					if err != nil {
						util.GetLogger(ctx).WithError(err).Debug("Failed to split domain from receipt event sender")
						continue
					}
					if t.Origin != domain {
						util.GetLogger(ctx).Debugf("Dropping receipt event where sender domain (%q) doesn't match origin (%q)", domain, t.Origin)
						continue
					}
					if api.IsServerBannedFromRoom(ctx, t.rsAPI, roomID, domain) {
						continue
					}
					if err := t.processReceiptEvent(ctx, userID, roomID, "m.read", mread.Data.ThreadID, mread.Data.TS, mread.EventIDs); err != nil {
						failures.add("receipt", err)
						util.GetLogger(ctx).WithError(err).WithFields(logrus.Fields{
							"sender":  t.Origin,
							"user_id": userID,
							"room_id": roomID,
							"events":  mread.EventIDs,
						}).Error("Failed to send receipt event to JetStream")
						continue
					}
				}
			}
		case types.MSigningKeyUpdate:
			if err := t.producer.SendSigningKeyUpdate(ctx, e.Content, t.Origin); err != nil {
				failures.add("signing key update", err)
				sentry.CaptureException(err)
				logrus.WithError(err).Errorf("Failed to process signing key update")
			}
		case spec.MPresence:
			if t.inboundPresenceEnabled {
				if err := t.processPresence(ctx, e); err != nil {
					logrus.WithError(err).Errorf("Failed to process presence update")
				}
			}
		default:
			util.GetLogger(ctx).WithField("type", e.Type).Debug("Unhandled EDU")
		}
	}
}

// processPresence handles m.receipt events.
func (t *TxnReq) processPresence(ctx context.Context, e gomatrixserverlib.EDU) error {
	payload := types.Presence{}
	if err := json.Unmarshal(e.Content, &payload); err != nil {
		return err
	}
	for _, content := range payload.Push {
		if _, serverName, err := gomatrixserverlib.SplitID('@', content.UserID); err != nil {
			continue
		} else if serverName == t.ourServerName {
			continue
		} else if serverName != t.Origin {
			continue
		}
		presence, ok := syncTypes.PresenceFromString(content.Presence)
		if !ok {
			continue
		}
		if err := t.producer.SendPresence(ctx, content.UserID, presence, content.StatusMsg, content.LastActiveAgo); err != nil {
			return err
		}
	}
	return nil
}

// processReceiptEvent sends receipt events to JetStream.
func (t *TxnReq) processReceiptEvent(ctx context.Context,
	userID, roomID, receiptType, threadID string,
	timestamp spec.Timestamp,
	eventIDs []string,
) error {
	if _, serverName, err := gomatrixserverlib.SplitID('@', userID); err != nil {
		return nil
	} else if serverName == t.ourServerName {
		return nil
	} else if serverName != t.Origin {
		return nil
	}
	// store every event
	for _, eventID := range eventIDs {
		if err := t.producer.SendReceipt(ctx, userID, roomID, eventID, receiptType, threadID, timestamp); err != nil {
			return fmt.Errorf("unable to set receipt event: %w", err)
		}
	}

	return nil
}
