// Copyright 2024 New Vector Ltd.
// Copyright 2017 Vector Creations Ltd
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package producers

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/nats-io/nats.go"
	log "github.com/sirupsen/logrus"

	"codefloe.com/pat-s/zendrite/setup/jetstream"
	"codefloe.com/pat-s/zendrite/syncapi/types"
	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

// SyncAPIProducer produces events for the sync API server to consume.
type SyncAPIProducer struct {
	TopicReceiptEvent      string
	TopicSendToDeviceEvent string
	TopicTypingEvent       string
	TopicPresenceEvent     string
	TopicPresenceRequest   string
	JetStream              nats.JetStreamContext
	NATS                   *nats.Conn
	ServerName             spec.ServerName
	UserAPI                userapi.ClientUserAPI
}

func (p *SyncAPIProducer) SendReceipt(
	ctx context.Context,
	userID, roomID, eventID, receiptType, threadID string, timestamp spec.Timestamp,
) error {
	m := &nats.Msg{
		Subject: p.TopicReceiptEvent,
		Header:  nats.Header{},
	}
	m.Header.Set(jetstream.UserID, userID)
	m.Header.Set(jetstream.RoomID, roomID)
	m.Header.Set(jetstream.EventID, eventID)
	m.Header.Set("type", receiptType)
	m.Header.Set("thread_id", threadID)
	m.Header.Set("timestamp", fmt.Sprintf("%d", timestamp))

	log.WithFields(log.Fields{}).Tracef("Producing to topic '%s'", p.TopicReceiptEvent)
	_, err := jetstream.Publish(ctx, p.JetStream, m)
	return err
}

func (p *SyncAPIProducer) SendToDevice(
	ctx context.Context, sender, userID, deviceID, eventType string,
	message json.RawMessage,
) error {
	devices := []string{}
	_, domain, err := gomatrixserverlib.SplitID('@', userID)
	if err != nil {
		return err
	}

	// If the event is targeted locally then we want to expand the wildcard
	// out into individual device IDs so that we can send them to each respective
	// device. If the event isn't targeted locally then we can't expand the
	// wildcard as we don't know about the remote devices, so instead we leave it
	// as-is, so that the federation sender can send it on with the wildcard intact.
	if domain == p.ServerName && deviceID == "*" {
		var res userapi.QueryDevicesResponse
		err = p.UserAPI.QueryDevices(context.TODO(), &userapi.QueryDevicesRequest{ //nolint:contextcheck
			UserID: userID,
		}, &res)
		if err != nil {
			return err
		}
		for _, dev := range res.Devices {
			devices = append(devices, dev.ID)
		}
	} else {
		devices = append(devices, deviceID)
	}

	log.WithFields(log.Fields{
		"user_id":     userID,
		"num_devices": len(devices),
		"type":        eventType,
	}).Tracef("Producing to topic '%s'", p.TopicSendToDeviceEvent)
	for i, device := range devices {
		ote := &types.OutputSendToDeviceEvent{
			UserID:   userID,
			DeviceID: device,
			SendToDeviceEvent: gomatrixserverlib.SendToDeviceEvent{
				Sender:  sender,
				Type:    eventType,
				Content: message,
			},
		}

		eventJSON, err := json.Marshal(ote)
		if err != nil {
			log.WithError(err).Error("sendToDevice failed json.Marshal")
			return err
		}
		m := nats.NewMsg(p.TopicSendToDeviceEvent)
		m.Data = eventJSON
		m.Header.Set("sender", sender)
		m.Header.Set(jetstream.UserID, userID)

		if _, err = jetstream.Publish(ctx, p.JetStream, m); err != nil {
			if i < len(devices)-1 {
				log.WithError(err).Warn("sendToDevice failed to PublishMsg, trying further devices")
				continue
			}
			log.WithError(err).Error("sendToDevice failed to PublishMsg for all devices")
			return err
		}
	}
	return nil
}

func (p *SyncAPIProducer) SendTyping(
	ctx context.Context, userID, roomID string, typing bool, timeoutMS int64,
) error {
	m := &nats.Msg{
		Subject: p.TopicTypingEvent,
		Header:  nats.Header{},
	}
	m.Header.Set(jetstream.UserID, userID)
	m.Header.Set(jetstream.RoomID, roomID)
	m.Header.Set("typing", strconv.FormatBool(typing))
	m.Header.Set("timeout_ms", strconv.Itoa(int(timeoutMS)))

	_, err := jetstream.Publish(ctx, p.JetStream, m)
	return err
}

func (p *SyncAPIProducer) SendPresence(
	ctx context.Context, userID string, presence types.Presence, statusMsg *string,
) error {
	m := nats.NewMsg(p.TopicPresenceEvent)
	m.Header.Set(jetstream.UserID, userID)
	m.Header.Set("presence", presence.String())
	// The sync API writes this update synchronously through RequestPresence.
	// Keep the durable event for federation without applying it a second time.
	m.Header.Set("from_sync", "true")
	if statusMsg != nil {
		m.Header.Set("status_msg", *statusMsg)
	}

	timestamp := spec.AsTimestamp(time.Now())
	m.Header.Set("last_active_ts", strconv.Itoa(int(timestamp)))

	_, err := jetstream.Publish(ctx, p.JetStream, m)
	if err != nil {
		return err
	}
	if p.NATS == nil || p.TopicPresenceRequest == "" {
		return nil
	}
	request := nats.NewMsg(p.TopicPresenceRequest)
	request.Header.Set(jetstream.UserID, userID)
	request.Header.Set("set_presence", "true")
	request.Header.Set("presence", presence.String())
	request.Header.Set("last_active_ts", strconv.Itoa(int(timestamp)))
	if statusMsg != nil {
		request.Header.Set("status_msg", *statusMsg)
	}
	response, err := p.NATS.RequestMsg(request, 10*time.Second)
	if err != nil {
		return fmt.Errorf("sync API failed to apply presence update: %w", err)
	}
	if errMsg := response.Header.Get("error"); errMsg != "" {
		return fmt.Errorf("sync API failed to apply presence update: %s", errMsg)
	}
	return nil
}
