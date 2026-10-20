package internal

import (
	"context"
	"database/sql"
	"errors"

	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"

	"codefloe.com/pat-s/zendrite/roomserver/api"
	rstypes "codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/syncapi/storage"
	"codefloe.com/pat-s/zendrite/syncapi/synctypes"
	"codefloe.com/pat-s/zendrite/syncapi/types"
)

const (
	relThread    = "m.thread"
	relReplace   = "m.replace"
	relReference = "m.reference"

	relationPageSize = 100
)

// bundledRelTypes are the relation types aggregated into unsigned.m.relations.
// Annotations (reactions) are deliberately absent: the spec defines no
// server-side aggregation format for them.
var bundledRelTypes = []string{relThread, relReplace, relReference}

// RelationBundler adds bundled aggregations (unsigned.m.relations) to events
// served to one user, as described in the "Aggregations of child events"
// section of the client-server API.
//
// Child events are history-visibility filtered and ignored senders skipped
// before aggregating, so neither IDs nor counts reveal hidden events. Bundles
// never recurse beyond the latest thread reply's own edits and references.
type RelationBundler struct {
	db        storage.DatabaseTransaction
	rsAPI     api.SyncRoomserverAPI
	userID    spec.UserID
	ignores   *types.IgnoredUsers
	watermark types.StreamPosition
	maxStream types.StreamPosition
}

// NewRelationBundler aggregates every relation visible in the snapshot.
func NewRelationBundler(ctx context.Context, db storage.DatabaseTransaction, rsAPI api.SyncRoomserverAPI, userID spec.UserID) (*RelationBundler, error) {
	watermark, err := db.MaxStreamPositionForRelations(ctx)
	if err != nil {
		return nil, err
	}
	ignores, err := db.IgnoresForUser(ctx, userID.String())
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	return NewRelationBundlerAt(db, rsAPI, userID, ignores, watermark), nil
}

// NewRelationBundlerAt aggregates only relations up to watermark, so that
// paginated endpoints can freeze their input across requests.
func NewRelationBundlerAt(db storage.DatabaseTransaction, rsAPI api.SyncRoomserverAPI, userID spec.UserID, ignores *types.IgnoredUsers, watermark types.StreamPosition) *RelationBundler {
	return &RelationBundler{db: db, rsAPI: rsAPI, userID: userID, ignores: ignores, watermark: watermark}
}

// UpTo returns a bundler that excludes child events after the given PDU
// stream position. A sync response uses it so that children the client will
// receive in a later response are not already counted in this one.
func (b RelationBundler) UpTo(pos types.StreamPosition) *RelationBundler {
	b.maxStream = pos
	return &b
}

// Bundle sets unsigned.m.relations on each client event, matching clients to
// events by event ID. Aggregations received over federation are always
// removed, whether or not the event has local children.
func (b *RelationBundler) Bundle(ctx context.Context, events []*rstypes.HeaderedEvent, clients []synctypes.ClientEvent, format synctypes.ClientEventFormat) error {
	if len(clients) == 0 {
		return nil
	}
	byID := make(map[string]*rstypes.HeaderedEvent, len(events))
	for _, event := range events {
		if event != nil {
			byID[event.EventID()] = event
		}
	}
	parents := make(map[string][]string)
	for i := range clients {
		if err := stripRelations(&clients[i]); err != nil {
			return err
		}
		event, ok := byID[clients[i].EventID]
		if !ok || event.StateKey() != nil {
			continue
		}
		roomID := event.RoomID().String()
		parents[roomID] = append(parents[roomID], event.EventID())
	}
	children := make(map[string]map[string]bool)
	for roomID, eventIDs := range parents {
		found, err := b.db.RelationTypesFor(ctx, roomID, eventIDs, bundledRelTypes, b.watermark)
		if err != nil {
			return err
		}
		for eventID, relTypes := range found {
			children[eventID] = relTypes
		}
	}
	for i := range clients {
		relTypes := children[clients[i].EventID]
		if len(relTypes) == 0 {
			continue
		}
		event := byID[clients[i].EventID]
		// Compare user IDs, which a client event in federation format may not carry.
		sender, err := b.sender(ctx, event)
		if err != nil {
			return err
		}
		if relTypes[relThread] {
			summary, err := b.ThreadSummary(ctx, event, sender)
			if err != nil {
				return err
			}
			if summary != nil {
				if err = b.AddThreadBundle(ctx, &clients[i], summary, format); err != nil {
					return err
				}
			}
		}
		if err = b.bundleEditsAndReferences(ctx, event, sender, &clients[i], format, relTypes); err != nil {
			return err
		}
	}
	return nil
}

// BundleEvent is Bundle for a single event.
func (b *RelationBundler) BundleEvent(ctx context.Context, event *rstypes.HeaderedEvent, client *synctypes.ClientEvent, format synctypes.ClientEventFormat) error {
	clients := []synctypes.ClientEvent{*client}
	if err := b.Bundle(ctx, []*rstypes.HeaderedEvent{event}, clients, format); err != nil {
		return err
	}
	*client = clients[0]
	return nil
}

// ThreadSummary describes the visible replies to a thread root.
type ThreadSummary struct {
	Latest       *rstypes.HeaderedEvent
	LatestStream types.StreamPosition
	Count        int
	Participated bool
}

// ThreadSummary returns nil when root cannot start a thread, because it is
// itself a relation, or when the user can see no reply to it.
func (b *RelationBundler) ThreadSummary(ctx context.Context, root *rstypes.HeaderedEvent, rootSender string) (*ThreadSummary, error) {
	if root.StateKey() != nil || gjson.GetBytes(root.Content(), `m\.relates_to.rel_type`).Exists() {
		return nil, nil
	}
	summary := &ThreadSummary{Participated: rootSender == b.userID.String()}
	err := b.eachRelation(ctx, root, relThread, func(event *rstypes.HeaderedEvent, pos types.StreamPosition, sender string) error {
		summary.Count++
		summary.Participated = summary.Participated || sender == b.userID.String()
		// The latest reply is the topologically latest; the stream position breaks ties.
		latest := summary.Latest
		if latest == nil || event.Depth() > latest.Depth() || (event.Depth() == latest.Depth() && pos > summary.LatestStream) {
			summary.Latest, summary.LatestStream = event, pos
		}
		return nil
	})
	if err != nil || summary.Latest == nil {
		return nil, err
	}
	return summary, nil
}

// AddThreadBundle sets the m.thread aggregation on a root, bundling the
// latest reply's own edits and references.
func (b *RelationBundler) AddThreadBundle(ctx context.Context, root *synctypes.ClientEvent, summary *ThreadSummary, format synctypes.ClientEventFormat) error {
	latest, err := b.clientEvent(ctx, summary.Latest, format)
	if err != nil {
		return err
	}
	if err = stripRelations(latest); err != nil {
		return err
	}
	found, err := b.db.RelationTypesFor(ctx, summary.Latest.RoomID().String(), []string{summary.Latest.EventID()}, []string{relReplace, relReference}, b.watermark)
	if err != nil {
		return err
	}
	if relTypes := found[summary.Latest.EventID()]; len(relTypes) > 0 {
		sender, err := b.sender(ctx, summary.Latest)
		if err != nil {
			return err
		}
		if err = b.bundleEditsAndReferences(ctx, summary.Latest, sender, latest, format, relTypes); err != nil {
			return err
		}
	}
	bundle := struct {
		LatestEvent  *synctypes.ClientEvent `json:"latest_event"`
		Count        int                    `json:"count"`
		Participated bool                   `json:"current_user_participated"`
	}{latest, summary.Count, summary.Participated}
	root.Unsigned, err = sjson.SetBytes(unsignedOrEmpty(root.Unsigned), `m\.relations.m\.thread`, bundle)
	return err
}

// BundleEditsAndReferences replaces any m.relations on client with the
// event's references and latest valid edit.
func (b *RelationBundler) BundleEditsAndReferences(ctx context.Context, event *rstypes.HeaderedEvent, client *synctypes.ClientEvent, format synctypes.ClientEventFormat) error {
	if err := stripRelations(client); err != nil {
		return err
	}
	sender, err := b.sender(ctx, event)
	if err != nil {
		return err
	}
	return b.bundleEditsAndReferences(ctx, event, sender, client, format, nil)
}

// bundleEditsAndReferences queries only the relation types present in
// relTypes, or all of them when relTypes is nil. The sender is the user ID
// of the event's sender.
func (b *RelationBundler) bundleEditsAndReferences(ctx context.Context, event *rstypes.HeaderedEvent, sender string, client *synctypes.ClientEvent, format synctypes.ClientEventFormat, relTypes map[string]bool) error {
	if event.StateKey() != nil {
		return nil
	}
	var err error
	if relTypes == nil || relTypes[relReference] {
		var references []map[string]string
		err = b.eachRelation(ctx, event, relReference, func(child *rstypes.HeaderedEvent, _ types.StreamPosition, _ string) error {
			references = append(references, map[string]string{"event_id": child.EventID()})
			return nil
		})
		if err != nil {
			return err
		}
		if len(references) > 0 {
			client.Unsigned, err = sjson.SetBytes(unsignedOrEmpty(client.Unsigned), `m\.relations.m\.reference`, map[string]any{"chunk": references})
			if err != nil {
				return err
			}
		}
	}
	// A redacted original carries no edits, and an edit cannot itself be edited.
	if (relTypes != nil && !relTypes[relReplace]) || isRedacted(event) || b.ignored(sender) || gjson.GetBytes(event.Content(), `m\.relates_to.rel_type`).Str == relReplace {
		return nil
	}
	var edit *rstypes.HeaderedEvent
	err = b.eachRelation(ctx, event, relReplace, func(child *rstypes.HeaderedEvent, _ types.StreamPosition, editor string) error {
		if child.StateKey() != nil || child.Type() != event.Type() || editor != sender || isRedacted(child) {
			return nil
		}
		if child.Type() != "m.room.encrypted" && !gjson.GetBytes(child.Content(), `m\.new_content`).IsObject() {
			return nil
		}
		if edit == nil || child.OriginServerTS() > edit.OriginServerTS() || (child.OriginServerTS() == edit.OriginServerTS() && child.EventID() > edit.EventID()) {
			edit = child
		}
		return nil
	})
	if err != nil || edit == nil {
		return err
	}
	clientEdit, err := b.clientEvent(ctx, edit, format)
	if err != nil {
		return err
	}
	if err = stripRelations(clientEdit); err != nil {
		return err
	}
	client.Unsigned, err = sjson.SetBytes(unsignedOrEmpty(client.Unsigned), `m\.relations.m\.replace`, clientEdit)
	return err
}

// eachRelation visits the visible children of parent with the given relation
// type, filtering before aggregating so that hidden events stay hidden.
func (b *RelationBundler) eachRelation(ctx context.Context, parent *rstypes.HeaderedEvent, relType string, visit func(*rstypes.HeaderedEvent, types.StreamPosition, string) error) error {
	roomID := parent.RoomID().String()
	var from types.StreamPosition
	for {
		children, _, next, err := b.db.RelationsFor(ctx, roomID, parent.EventID(), relType, "", from, b.watermark, false, relationPageSize)
		if err != nil {
			return err
		}
		events := make([]*rstypes.HeaderedEvent, 0, len(children))
		positions := make(map[string]types.StreamPosition, len(children))
		for _, child := range children {
			if child.RoomID().String() != roomID || (b.maxStream > 0 && child.StreamPosition > b.maxStream) {
				continue
			}
			events = append(events, child.HeaderedEvent)
			positions[child.EventID()] = child.StreamPosition
		}
		events, err = ApplyHistoryVisibilityFilter(ctx, b.db, b.rsAPI, events, nil, b.userID, "aggregations")
		if err != nil {
			return err
		}
		for _, event := range events {
			sender, err := b.rsAPI.QueryUserIDForSender(ctx, event.RoomID(), event.SenderID())
			if err != nil {
				return err
			}
			if sender == nil || b.ignored(sender.String()) {
				continue
			}
			if err = visit(event, positions[event.EventID()], sender.String()); err != nil {
				return err
			}
		}
		if next == "" {
			return nil
		}
		if from, err = types.NewStreamPositionFromString(next); err != nil {
			return err
		}
	}
}

func (b *RelationBundler) clientEvent(ctx context.Context, event *rstypes.HeaderedEvent, format synctypes.ClientEventFormat) (*synctypes.ClientEvent, error) {
	return synctypes.ToClientEvent(event.PDU, format, func(roomID spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) {
		return b.rsAPI.QueryUserIDForSender(ctx, roomID, senderID)
	})
}

// sender resolves the user ID of an event's sender, which differs from the
// sender ID in rooms with pseudo IDs.
func (b *RelationBundler) sender(ctx context.Context, event *rstypes.HeaderedEvent) (string, error) {
	userID, err := b.rsAPI.QueryUserIDForSender(ctx, event.RoomID(), event.SenderID())
	if err != nil || userID == nil {
		return "", err
	}
	return userID.String(), nil
}

func (b *RelationBundler) ignored(userID string) bool {
	if b.ignores == nil {
		return false
	}
	_, ok := b.ignores.List[userID]
	return ok
}

// StripRelations removes aggregations from events served without bundles.
func StripRelations(clients []synctypes.ClientEvent) error {
	for i := range clients {
		if err := stripRelations(&clients[i]); err != nil {
			return err
		}
	}
	return nil
}

// stripRelations removes aggregations so that those received from another
// server, or computed for another user, are never served.
func stripRelations(client *synctypes.ClientEvent) error {
	if !gjson.GetBytes(client.Unsigned, `m\.relations`).Exists() {
		return nil
	}
	unsigned, err := sjson.DeleteBytes(client.Unsigned, `m\.relations`)
	if err != nil {
		return err
	}
	client.Unsigned = unsigned
	return nil
}

// isRedacted also recognizes events redacted in storage, which are loaded
// with stripped content and redacted_because but without the redacted flag.
func isRedacted(event *rstypes.HeaderedEvent) bool {
	return event.Redacted() || gjson.GetBytes(event.Unsigned(), "redacted_because").Exists()
}

func unsignedOrEmpty(unsigned []byte) []byte {
	if len(unsigned) == 0 {
		return []byte(`{}`)
	}
	return unsigned
}
