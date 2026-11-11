# Failing closed on the state before an event

Status of the argument as of commit `79a378a0`. This is a case analysis over the
code, not a machine-checked proof. Section 6 lists what it does not cover.

Background and the incident are in
[state-before-and-state-hashes.md](state-before-and-state-hashes.md). This note
only asks: after the fixes, can Zendrite accept an event on a wrong state before
it?

## 1. Claim

**Safety (S).** Let E be an event that Zendrite stores as accepted (not
rejected, not soft-failed, not an outlier). Then E was checked by `Allowed()`
against a state before E that was computed from a complete set of inputs, or E
falls into one of the named exceptions in section 5.

"Complete" means: for every prev event P of E, the state after P is derived from
the full state events that P's accepted history implies, with no state event
silently omitted because it could not be fetched.

**What (S) does not say.** It does not say the state is _right_ when a peer lies
(section 6.1), and it does not say E is eventually stored (section 6.3).

## 2. Model

The input to the roomserver is `(E, kind, flags)`. The kinds that matter:

| Kind          | Meaning                                                     |
| ------------- | ----------------------------------------------------------- |
| `KindNew`     | a live event from `/send`, a local send, or a gap fill leaf |
| `KindOld`     | an event fetched to fill a gap or satisfy missing prevs     |
| `KindOutlier` | an event stored only as auth or state data, never accepted  |

Flags: `HasState` (the caller supplies the state before E), `SkipMissingEvents`
(local leaves), `SkipEventAuth`.

A processing attempt has one of three outcomes:

- **Stored accepted** — written with a state snapshot; visible to clients.
- **Stored rejected** — written, permanent. Resolution skips it, and its
  descendants cite it as a rejected auth event.
- **Unstored** — nothing written. A later event that cites E as a prev fetches
  it again.

The fixes make a class of failures end as _unstored_ instead of _accepted on
partial state_ or _rejected_.

## 3. The incident as a violation of (S)

Step 3 of the incident: `/state_ids` listed 107 events and one fetch was
refused, and the loop did `continue`, so the state after the prev event P was
built from 106. Steps 4 to 6: eight `KindOld` events were stored rejected for
"missing prev events", resolution then skipped their branch, and the new event
passed `Allowed()` against the damaged state. Two independent causes:

- **C1.** An unfetchable state event was dropped instead of failing the lookup.
- **C2.** A transient failure was turned into a permanent rejection, which then
  changed later resolution.

## 4. Case analysis of the paths that store an accepted event

Every accepted event reaches storage through `processRoomEvent`. Whether it
reaches storage accepted is decided by `isRejected`, which is set on the paths
below. The table covers each way the state before E is obtained.

| #   | Path                                          | How the state before E is obtained                                                  | Failure handling now                                                        | Honours (S)?                                 |
| --- | --------------------------------------------- | ----------------------------------------------------------------------------------- | --------------------------------------------------------------------------- | -------------------------------------------- |
| P1  | Create event                                  | none exists                                                                         | n/a                                                                         | yes, vacuous                                 |
| P2  | All prevs known locally                       | `QueryStateAfterEvents(prevs)`                                                      | `!PrevEventsExist` rejects; a database error fails the input (unstored)     | yes: local state of stored events only       |
| P3  | `missingPrev`, `KindNew`, servers known       | `processEventWithMissingState`: `/get_missing_events`, else `/state_ids` + `/event` | `errStateUnavailable` or a local failure: unstored. Anything else: rejected | yes for fetch failures (see lemma L1)        |
| P4  | `missingPrev`, `KindOld`                      | none; cannot be computed                                                            | `errStateUnavailable`: unstored (`processStateBefore`)                      | yes                                          |
| P5  | `missingPrev`, `KindNew`, `SkipMissingEvents` | none                                                                                | rejected                                                                    | yes (never accepted); but see 6.4            |
| P6  | `missingPrev`, no servers to ask              | none                                                                                | rejected                                                                    | yes (never accepted); but see 6.4            |
| P7  | `HasState` (send_join, state fallback)        | caller supplies the IDs; `EventsFromIDs`                                            | a missing ID is a hard error                                                | accepted on supplied state; see exception X1 |
| P8  | Partial-state room (MSC3706)                  | local state merged with the event's cited auth events                               | n/a                                                                         | exception X2                                 |
| P9  | State resolution cooldown                     | none                                                                                | unstored (`errMissingStateCooldown`)                                        | yes                                          |

### L1. The fetch paths cannot omit a state event silently

The state after P is built from `/state_ids` in two places
(`lookupMissingStateViaStateIDs` and `fetchAndStoreStateInChunks`). For each ID
listed that is not stored locally, `/event` is called. After the fix a failed
fetch wraps `errStateUnavailable` and aborts the lookup. The only IDs still
dropped are events that fail signature or format validation, which the receiver
would reject anyway and which a correct server never lists.

Claim: if the lookup returns success, every listed ID was either present or
fetched and valid, or failed validation. Reason: the loops have no other exit
that discards an ID; the old `if herr != nil { continue }` was the only one
(commits `bbe01da7`, `436b7823`). **This is the weakest link**: it depends on
reading both loops, and the two unit tests cover one shape each.

### L2. A transient failure never becomes a rejection

`processRoomEvent` returns early with the error when
`unavailableHere(ctx, err) || errors.Is(err, errStateUnavailable)`. Only other
errors set `isRejected`. `KindOld` events with `missingPrev` return
`errStateUnavailable` from `processStateBefore` instead of rejecting. The gap
filling loops (`fillGapDuringPartialState`, `processEventWithMissingState`) skip
a `KindOld` parent that returns `errStateUnavailable` and do not abort.

Consequence: step 4 to 5 of the incident (rejecting parents, then resolution
skipping the branch) cannot recur from a transient fetch failure.

Limit: which errors count as "other" is decided by `errors.As` on the error
chain. A remote failure that does not wrap `errStateUnavailable` and is not
`unavailableHere` (for example a malformed `/get_missing_events` reply) is still
treated as a reason to reject.

### L3. Unstored events are harmless to state

An unstored event adds nothing to the snapshot, the forward extremities or the
output stream. Therefore it cannot change what other events resolve against.
This follows from `processRoomEvent` returning before `calculateAndSetState` and
the database write on every unavailable path.

### L4. Local failures are retried, remote ones are not retried inline

`retryableInputError` retries a local failure (database, context) even when it
is wrapped inside `errStateUnavailable`, and does not retry a bare remote
unavailability. `unavailableHere` counts only local failures. A remote backoff
would otherwise hold the room's input queue for minutes.

## 5. Exceptions (S does not hold by design)

- **X1. `HasState` inputs.** The state before E is the list the caller supplies,
  and `EventsFromIDs` only checks the IDs exist locally. For `send_join` this is
  the join response; for the state fallback it is a `/state_ids` answer. Both
  are trusted as listed. Completeness holds only if the remote's list is
  complete (section 6.1).
- **X2. Partial-state rooms.** `processStateBefore` authorises against local
  state plus the event's cited auth events (`mergePartialStateAuth`), knowingly
  incomplete, until the background worker finishes. Events accepted in that
  window are checked against less than the true state before.

## 6. What is not proven

1. **Wrong but complete-looking answers.** A server that answers `/state_ids`
   with a complete-looking but wrong list is trusted. Only signed state
   commitments (state-before-and-state-hashes.md §4) would address this. Not
   needed for the incident, not decided.
2. **Completeness of the table in section 4.** The paths were found by reading
   `processRoomEvent`, `processStateBefore`, `input_missing.go` and
   `input_retry.go`. There is no mechanical check that no other caller of the
   store path exists. Obligation: list the callers of `storeEvent` /
   `calculateAndSetState`, and show each is in a row above.
3. **Liveness.** Nothing guarantees an unstored event is eventually stored. If
   the remote never serves the missing event, the gap persists and events built
   on it stay unstored. There is no bound and no alarm. Obligation: a bound on
   retries or a metric, and a test.
4. **Residual permanent rejections (P5, P6).** "Missing prev events and no other
   servers to ask" and `SkipMissingEvents` with missing prevs still reject
   permanently. These do not violate (S), since a rejected event is not
   accepted, but they have the same cost as C2: resolution skips the branch.
   Whether they should become unstored is undecided.
5. **Gap-fill abort (`7b4af5f9`).** The argument that a live event cannot reach
   the `missingPrev` rejection after a skipped `KindOld` parent rests on the
   first missing event being resolved with its state before any `KindOld` event
   is processed. No test skips a parent mid-loop.
6. **Rejected-set dependence.** Resolution skips rejected prevs. (S) does not
   cover a branch whose rejections were _correct_ but whose rejected events
   later become acceptable when a missing event arrives. That is the "hope it
   gets unrejected later" comment in `processRoomEvent`; there is no unrejection
   mechanism that I verified.
7. **Test coverage is by example.** Evidence today:
   - `TestMissingStateEventLeavesEventUnaccepted` (C1)
   - `TestUnavailableGrandparentLeavesChainUnrejected` (C2)
   - retry classification tests in `input_retry_test.go`
   - Complement `TestMissingStateEventUnavailableDoesNotAcceptEvent`

   No property test over gap shapes (number of prevs, which are missing, which
   fetches fail, in which order).

## 7. Related, but not part of (S)

`/messages` backfill: when the backfill at a backward extremity fails and
nothing is stored locally, `handleEmptyEventsSlice` now returns an empty page
instead of a 500. This is a client-facing bug found by Complement
(`TestFederationLateSameDepthEventPagination`), and it has no bearing on state
safety. Cost: a client cannot tell the end of reachable history from a failed
backfill. Complement has not been re-run since the change.

## 8. Next steps, in order of value

1. Enumerate the callers in 6.2 and finish the table.
2. Add the mid-loop skipped-parent test for 6.5.
3. Decide P5/P6 (6.4) and the liveness bound (6.3).
4. Property test over gap shapes with injected fetch failures (6.7).
