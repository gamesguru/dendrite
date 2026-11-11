# State before an event, state resets, and state commitments

This note covers how the Matrix spec defines the state before an event, how that
went wrong in a state reset on `den.nutra.tk`, what Zendrite changed, and a
room-version design (signed LtHash state commitments plus state-resolution rule
changes) that would make the remaining failure modes detectable or impossible.

Spec references are to the Matrix specification at
https://spec.matrix.org/latest/. The room-version fragments cited below are
under https://spec.matrix.org/latest/rooms/.

## 1. The definition

From `rooms/fragments/v2-state-res.txt` (room versions 2 to 12). A state map
takes `(event_type, state_key)` to an event ID.

- **State after E, written S′(E).**
  - If E is a message event: `S′(E) = S(E)`.
  - If E is a state event: `S′(E)` is `S(E)` with the entry for E's
    `(type, state_key)` replaced by E's event ID.
- **State before E, written S(E).** Over E's `prev_events` `{E₁, …, Eₘ}`:
  `S(E) = resolve({S′(E₁), …, S′(Eₘ)})`.

`resolve()` takes a set, so the order of `prev_events` does not matter. It is
not a merge or a XOR:

- A key that maps to the same event in every input is _unconflicted_ and kept as
  is.
- Every other value goes into the _conflicted state set_, which is ordered
  (power events by reverse topological power ordering over the auth DAG, the
  rest by mainline ordering) and replayed through iterative auth checks.

### Why partial inputs corrupt the result

- **Dropping a prev event** removes its branch from the resolution, so conflicts
  that branch would have raised never happen.
- **Dropping one entry from an S′(Eᵢ)** makes that key look absent there. With
  several branches that manufactures or hides a conflict; with one branch the
  key is simply missing from S(E).

Either way the result is not S(E), so spec check 5 (auth against the state
before the event, `server-server-api.txt:765`) runs against the wrong state.

### What the spec says about missing history

Nothing normative. Events are rejected only for failing auth against their
`auth_events` (check 4) or against the state before them (check 5); failing
against current state soft-fails them (check 6). Missing `prev_events` are not a
ground for rejection. The spec only says a server "may use the
`/get_missing_events` API" (`server-server-api.txt:1086`).

## 2. The `den.nutra.tk` incident

Room `!YRswYZcqh4FWoo1LLk:nutra.tk`, 2026-11-05 21:38:38 to 21:38:40 UTC. `den`
was running `ad7e3f8`.

1. `mdev.nutra.tk` pushed `$BDAbDYyb…`, which had five `prev_events`, some
   unknown to `den`.
2. `/get_missing_events` to `mdev` timed out, putting `mdev` in a 5-minute
   backoff.
3. For prev event `$PYhqik21…`, `/state_ids` listed 107 events, of which one was
   missing locally. Its `/event` fetch was refused by the backoff, and the fetch
   loop did `if herr != nil { continue }`, so S′ was built from 106 of 107
   events.
4. Eight older `m.room.encrypted` events fetched to fill the gap (`KindOld`)
   were stored as **rejected** with "has missing prev events".
5. Resolving the state before `$BDAb…` skipped two of its prev events because
   they were rejected ("Skipping state lookup for already-rejected prev_event").
6. `$BDAb…` was accepted against that state: "State reset detected (removing 1
   events)", state snapshot 6118 → 6153. The room was then purged.

### What Zendrite changed

These need no spec change.

- **No acceptance on partial state.** If any state or auth event listed by
  `/state_ids` cannot be fetched for a reason that may pass (timeout, backoff,
  5xx), the lookup fails with `errStateUnavailable`, in both the chunked and
  non-chunked fetch paths. Events that fail signature or format checks are still
  left out, as before. (`bbe01da7`, `436b7823`)
- **Unavailable is not rejected.** An event whose state could not be obtained is
  left unstored, like the existing database-outage path, so the next event in
  the room fetches it again as a missing prev event. This includes the case
  where every prev event was rejected and the `/state_ids` fallback for the
  event itself also fails.
- **Old events with missing prev events are not rejected.** `KindOld` events
  whose prev events are still missing return `errStateUnavailable`, and the
  gap-filling loops skip them rather than aborting. A rejection is permanent and
  later resolution skips the event's whole branch.
- **New events with missing prev events and no lookup** (for example a local
  leave with `SkipMissingEvents`) are still rejected as before.

A live event cannot fall through to the `missingPrev` rejection after a
transient failure: the first missing event returned by `/get_missing_events` is
resolved with its state before any `KindOld` event is processed, and that lookup
fails with `errStateUnavailable`. If a `KindOld` parent is skipped later, the
live event's own state lookup goes to federation and either gets complete state
or fails the same way; every success path clears `missingPrev`.

Tests, in `roomserver/internal/input/input_missing_state_unavailable_test.go`:

- `TestMissingStateEventLeavesEventUnaccepted`: one state event at the gap is
  unfetchable; fails on the old code with "accepted on partial state".
- `TestUnavailableGrandparentLeavesChainUnrejected`: `/get_missing_events`
  returns E1 but E1's parent E0 is unfetchable; fails on the old code with E1
  "stored as rejected".

Complement: `TestMissingStateEventUnavailableDoesNotAcceptEvent`.

### What these fixes cannot do

They only cover a fetch that _fails_. A server that answers `/state_ids` with a
complete-looking but wrong list (after its own state reset, a bug, or malice) is
still trusted, because nothing in the protocol lets the receiver check that a
list of event IDs is really S′(Eᵢ). Different servers can give different
answers, and the receiver cannot tell which is right.

The argument that these changes are sufficient, and what remains unproven, is in
[fail-closed-state-before.md](fail-closed-state-before.md).

## 3. Why "state at the deepest parent" does not help

Using only the state after the deepest prev event instead of resolving over all
of them:

- **`depth` is set by the sender and unverified.** "Deepest" means whichever
  branch the sender says. A banned or demoted user could cite a pre-ban branch
  with a large `depth`.
- **Other branches' changes are lost.** A ban or power-level change on a sibling
  branch never reaches the merged state, and branches never converge.
- **It does not avoid fetching.** If the deepest parent is the unknown one, its
  state still has to be fetched.

Requiring every prev event is not the fragile part; the receiver only needs
S′(Eᵢ), which `/state_ids` can supply. The fragile part is that the answer
cannot be verified.

## 4. Signed state commitments (new room version)

### 4.1 LtHash

A homomorphic hash of a set, from Bellare and Micciancio's incremental hashing,
as used in Facebook's LtHash:

- `h(x)` expands an event ID `x` with an extendable-output hash (for example
  BLAKE3) to a vector of `n = 1024` integers mod 2¹⁶ (2 KiB).
- `L(S) = Σ_{x∈S} h(x)`, component-wise mod 2¹⁶.
- `L(S ∪ {x}) = L(S) + h(x)` and `L(S \ {x}) = L(S) − h(x)`.

The state is hashed as the **set of its event IDs**; the key of each entry comes
from the event itself. Order does not matter, so implementations need not agree
on a sort order or tree shape, and every state transition is O(1):

```text
L(S′(E)) = L(S(E))                                       if E is a message
L(S′(E)) = L(S(E)) − h(E.replaces_state) + h(E.event_id)  if E is a state event
```

with `h(null) = 0`.

### 4.2 New PDU fields

Both are covered by the content hash and signature.

- `state_commit`: `BLAKE3-256(L(S(E)))`, 32 bytes. Servers store the full 2 KiB
  `L` with each state snapshot.
- `replaces_state` (state events only): the event ID at E's `(type, state_key)`
  in S(E), or `null` if there was none. Today this exists only as an unsigned
  hint in `unsigned`. It has to be signed so that the state transition can be
  undone during backward verification (§4.5).

### 4.3 New auth rules

- **5a.** Reject E if `E.state_commit ≠ BLAKE3-256(L(S(E)))`, where S(E) is
  computed by the receiver.
- **5b.** Reject a state event E if `E.replaces_state` is not the event at E's
  key in S(E) (or `null` when the key is absent).

Soundness: if every accepted event passed 5a, the commitment inside any accepted
prev event is correct, by induction from the trust anchor (§4.5).

### 4.4 Receiving an event E

**Step A: obtain S(Eᵢ) for each prev event Eᵢ.**

- If Eᵢ is accepted locally, `L(S(Eᵢ))` is known and verified.
- Otherwise, fetch Eᵢ (for its `state_commit` and `replaces_state`), ask any
  server for `/state_ids` at Eᵢ, and compute `L` over the returned IDs. This
  needs no event bodies.
  - If it matches `Eᵢ.state_commit`, the list is exactly the state Eᵢ's sender
    committed to.
  - If it does not match, discard that answer and ask another server.
  - If no server gives a matching list, E is **pending**: not accepted, not
    rejected, retried later.

**Step B: fetch the state event bodies** that resolution needs. If any fetch
fails, E stays pending: the list is known to be right, the contents are not yet
available.

**Step C: resolve and compare.** Compute `S(E) = resolve({S′(Eᵢ)})`.

- If `BLAKE3-256(L(S(E))) = E.state_commit`, run checks 4 to 6 as usual.
- Otherwise reject E (rule 5a).

Every outcome (match, mismatch, pending) depends only on data, never on which
server answered.

### 4.5 Trust anchor and backfill

A joining server has not seen the room from `m.room.create`, so induction starts
from the join:

- **Join anchor.** At join event J, verify that the `/send_join` state IDs hash
  to `J.state_commit`, and cross-check `J.state_commit` with several resident
  servers (the join server plus at least two others). S(J) is then the anchor.
- **Forward.** Events after J are verified by §4.4.
- **Backward (backfill).** For a parent P that is the only prev event of a
  verified child C (`C.prev_events == [P]`), `S(C) = S′(P)`, so

  ```text
  L(S(P)) = L(S(C)) − h(P.event_id) + h(P.replaces_state)   (P a state event)
  L(S(P)) = L(S(C))                                         (P a message)
  ```

  and `BLAKE3-256(L(S(P)))` must equal `P.state_commit`. That is O(1) per step
  with no fetches. A child with multiple prev events is a merge: its S′(Pᵢ)
  cannot be obtained by inverting S(C) from one parent. Verify each parent from
  its own single-prev chain, or mark it unverified and require the merge
  parents' S′ to resolve to the child's verified S(C) before accepting it.

Each backfilled event is then **verified**, **contradicted** (never used as a
state source), or **unverified** (a parent is missing; display only).

Faster joins (partial state) fit this: the full ID list is verified at join and
bodies are fetched lazily.

### 4.6 What commitments do and do not fix

They fix:

- **Partial or wrong `/state_ids` answers**: rejected by hash before use.
- **Servers that disagree**: only the answer matching the prev event's
  commitment counts.
- **Silent divergence**: a server whose state drifted (its own reset, an
  out-of-band purge or database edit) sees a mismatch on the next event from any
  peer, instead of spreading the reset.

They do not:

- **Supply missing data.** A verified list with unfetchable bodies is pending.
- **Say who is right** when an event's sender committed to bad state; that event
  is rejected, but the truth is still computed locally.
- **Stop legal-but-malicious resolution** (§5): every honest server computes the
  same result, so the commitment matches.
- **Forgive implementation bugs.** A server whose resolution differs gets all of
  its events rejected. That is correct (it would otherwise be splitting the room
  silently), but every implementation must resolve identically.

## 5. Backdated power events

### 5.1 History is immutable

An event ID hashes the event's `prev_events`, so the DAG only points backwards
and no event can become an ancestor of an existing one. S(E) depends only on E's
ancestors, and whether an ancestor was rejected depends only on its own
ancestors. Soft-failure is per-server, but soft-failed events "participate in
state resolution as normal" (`server-server-api.txt:906`). So S(E) and
`E.state_commit` are fixed once E exists.

A "backdated" event B citing an old event P is a sibling branch, not an
ancestor:

```text
P ── X ── Y ── E ── … ── M   (prev_events: [main tip, B])
 \                      /
  B (backdated ban) ───
```

- X, Y, E, and their commitments are unchanged, whether received live or
  backfilled later.
- B is verified against its own ancestry (P).
- B affects state only at the merge M and its descendants; M commits to the
  state including B, and honest servers compute the same.

### 5.2 The real attack: retroactive effect at the merge

B cannot change S(E), but it can cancel E's _effect_ from M onward. Conflicted
power events are ordered by Kahn's algorithm over the auth DAG, with ties broken
by `(sender power level, origin_server_ts, event_id)`. `B.auth_events` point at
power levels from before P, so B sorts early; it is applied first in the
iterative auth checks, and later main-branch state events (E) that fail against
it drop out of S(M). E stays accepted in history, but the room's state after M
behaves as if E never applied.

Worse, an admin demoted at D (on the main branch after P) can tie with D in the
auth DAG and set `B.origin_server_ts` earlier than D's, so B is applied before
D, for example demoting the user who demoted them, and D then fails. Room v12
(state resolution starting from an empty map, plus the conflicted state
subgraph, `rooms/v12.txt:249-262`) targets forked state resets but still breaks
ties on `origin_server_ts`, so it does not remove this.

### 5.3 Rule changes (new room version)

- **Revocation-first ordering.** Among conflicted power events, if X lowers the
  power level or membership of Y's sender, add an ordering edge X → Y before the
  topological sort. A demotion then always applies before concurrent actions by
  the demoted user. Break cycles (two equal-rank admins demoting each other on
  concurrent branches) by `event_id`, never by timestamp, or by keeping neither
  change.
- **Timestamp floor.** Require
  `origin_server_ts ≥ max(prev.origin_server_ts) − skew`, with a fixed allowance
  `skew` for clock drift. Without the allowance, honest servers with slow clocks
  get rejected. On its own this only stops B from claiming to be older than P.
- **Branch-age horizon.** At a merge M, a conflicted power event reachable from
  only one parent may not override power events on the other branch if its
  lowest common ancestor with that branch is more than K events deep from M. The
  bound must be defined from the DAG (depth below M), not from a server's view
  of the current tip, or servers will resolve M differently.

## 6. Two different problems

| Problem                        | Example                                         | Fixed by                                                           |
| ------------------------------ | ----------------------------------------------- | ------------------------------------------------------------------ |
| Silent divergence              | The 106-of-107 import; a peer's own state reset | Failing closed today; state commitments (§4) in a new room version |
| Legal-but-malicious resolution | A backdated power event winning at a merge      | Resolution rule changes (§5.3) only                                |
