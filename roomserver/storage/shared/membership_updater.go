package shared

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"codefloe.com/pat-s/gomatrixserverlib"

	"codefloe.com/pat-s/zendrite/roomserver/storage/tables"
	"codefloe.com/pat-s/zendrite/roomserver/types"
)

type MembershipUpdater struct {
	transaction
	d             *Database
	roomNID       types.RoomNID
	targetUserNID types.EventStateKeyNID
	oldMembership tables.MembershipState
	// oldEventNID is the membership event NID currently recorded for the
	// target, or 0 if the row was only just inserted and is not backed by a
	// real membership event yet (i.e. the user has no prior membership in this
	// room incarnation).
	oldEventNID types.EventNID
	// oldForgotten is the forgotten flag currently recorded for the target.
	oldForgotten bool
}

func NewMembershipUpdater(
	ctx context.Context, d *Database, txn *sql.Tx, roomID, targetUserID string,
	targetLocal bool, roomVersion gomatrixserverlib.RoomVersion,
) (*MembershipUpdater, error) {
	var roomNID types.RoomNID
	var targetUserNID types.EventStateKeyNID
	var err error
	err = d.Writer.Do(d.DB, txn, func(txn *sql.Tx) error {
		roomNID, err = d.assignRoomNID(ctx, txn, roomID, roomVersion)
		if err != nil {
			return err
		}
		targetUserNID, err = d.assignStateKeyNID(ctx, txn, targetUserID)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return d.membershipUpdaterTxn(ctx, txn, roomNID, targetUserNID, targetLocal)
}

func (d *Database) membershipUpdaterTxn(
	ctx context.Context,
	txn *sql.Tx,
	roomNID types.RoomNID,
	targetUserNID types.EventStateKeyNID,
	targetLocal bool,
) (*MembershipUpdater, error) {
	err := d.Writer.Do(d.DB, txn, func(txn *sql.Tx) error {
		if err := d.MembershipTable.InsertMembership(ctx, txn, roomNID, targetUserNID, targetLocal); err != nil {
			return fmt.Errorf("d.MembershipTable.InsertMembership: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("u.d.Writer.Do: %w", err)
	}

	membership, err := d.MembershipTable.SelectMembershipForUpdate(ctx, txn, roomNID, targetUserNID)
	if err != nil {
		return nil, err
	}

	// Also load the event NID and forgotten flag backing the current row.
	// SelectMembershipFromRoomAndTarget filters out rows with event_nid 0, so
	// ErrNoRows here means the row was only just inserted and has no prior
	// membership event — callers use this to tell a genuine local membership
	// from a "ghost" learned purely from federated state.
	oldEventNID, _, oldForgotten, err := d.MembershipTable.SelectMembershipFromRoomAndTarget(ctx, txn, roomNID, targetUserNID)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}

	return &MembershipUpdater{
		transaction{ctx, txn}, d, roomNID, targetUserNID, membership, oldEventNID, oldForgotten,
	}, nil
}

// IsInvite implements types.MembershipUpdater.
func (u *MembershipUpdater) IsInvite() bool {
	return u.oldMembership == tables.MembershipStateInvite
}

// IsJoin implements types.MembershipUpdater.
func (u *MembershipUpdater) IsJoin() bool {
	return u.oldMembership == tables.MembershipStateJoin
}

// IsLeave implements types.MembershipUpdater.
func (u *MembershipUpdater) IsLeave() bool {
	return u.oldMembership == tables.MembershipStateLeaveOrBan
}

// IsKnock implements types.MembershipUpdater.
func (u *MembershipUpdater) IsKnock() bool {
	return u.oldMembership == tables.MembershipStateKnock
}

// HasMembershipEvent reports whether the target already has a membership row
// backed by a real membership event in this room. It is false for a row that
// was only just inserted (and so has no prior local membership), which is how
// a "ghost" membership learned purely from federated state is recognized.
func (u *MembershipUpdater) HasMembershipEvent() bool {
	return u.oldEventNID != 0
}

// OldForgotten returns the forgotten flag currently recorded for the target.
func (u *MembershipUpdater) OldForgotten() bool {
	return u.oldForgotten
}

func (u *MembershipUpdater) Delete() error {
	if _, err := u.d.InvitesTable.UpdateInviteRetired(u.ctx, u.txn, u.roomNID, u.targetUserNID); err != nil {
		return err
	}
	return u.d.MembershipTable.DeleteMembership(u.ctx, u.txn, u.roomNID, u.targetUserNID)
}

// Update applies the given membership transition. If forget is true the
// membership row is also marked as forgotten in the same transaction. The
// caller decides whether to set forget; the storage layer doesn't gate it.
func (u *MembershipUpdater) Update(newMembership tables.MembershipState, event *types.Event, forget bool) (bool, []string, error) {
	var inserted bool    // Did the query result in a membership change?
	var retired []string // Did we retire any updates in the process?
	return inserted, retired, u.d.Writer.Do(u.d.DB, u.txn, func(txn *sql.Tx) error {
		senderUserNID, err := u.d.assignStateKeyNID(u.ctx, u.txn, string(event.SenderID()))
		if err != nil {
			return fmt.Errorf("u.d.AssignStateKeyNID: %w", err)
		}
		inserted, err = u.d.MembershipTable.UpdateMembership(u.ctx, u.txn, u.roomNID, u.targetUserNID, senderUserNID, newMembership, event.EventNID, forget)
		if err != nil {
			return fmt.Errorf("u.d.MembershipTable.UpdateMembership: %w", err)
		}
		if !inserted {
			return nil
		}
		switch {
		case u.oldMembership != tables.MembershipStateInvite && newMembership == tables.MembershipStateInvite:
			inserted, err = u.d.InvitesTable.InsertInviteEvent(
				u.ctx, u.txn, event.EventID(), u.roomNID, u.targetUserNID, senderUserNID, event.JSON(),
			)
			if err != nil {
				return fmt.Errorf("u.d.InvitesTable.InsertInviteEvent: %w", err)
			}
		case u.oldMembership == tables.MembershipStateInvite && newMembership != tables.MembershipStateInvite:
			retired, err = u.d.InvitesTable.UpdateInviteRetired(
				u.ctx, u.txn, u.roomNID, u.targetUserNID,
			)
			if err != nil {
				return fmt.Errorf("u.d.InvitesTables.UpdateInviteRetired: %w", err)
			}
		}
		return nil
	})
}
