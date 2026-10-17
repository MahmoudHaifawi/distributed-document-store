# M8 — Vector clocks + tombstones (causal reconciliation)

Status: design — pending review before implementation planning.
Milestone: M8 of the capstone roadmap (third "standout" distributed-systems feature; builds on M6/M7).
Workflow: work directly on `master`; canonical remote `distributed-document-store`.

## 1. Intent / why

M7 reconciles divergent replicas by last-write-wins on an integer `_version`. That is blind to
causality: it cannot tell a value that genuinely *supersedes* another from two values written
*concurrently*, and it has no way to represent a delete, so a replica that missed a delete keeps
serving the document (M7's documented gap).

M8 adds a **vector clock** to every document — one counter per node — so reconciliation can classify
two copies as "A descends from B" (keep A), "B descends from A" (keep B), or **concurrent** (neither
descends — a real conflict). On a concurrent conflict it **auto-resolves deterministically** (latest
wall-clock timestamp, node id as the final tiebreak) and merges the two clocks onto the winner, so
every read still returns a single document and the gateway/UI are unchanged. A **delete** becomes a
**tombstone** — a marker carrying its own vector clock — that propagates through replication and
read-repair exactly like any other write, which closes the delete-resurrection gap.

This is the Dynamo/Riak causality model. Being able to explain "we use vector clocks to distinguish
causal updates from true conflicts, and tombstones so a delete can't be undone by a stale replica" is
the standout outcome.

**Honest framing.** With a single primary coordinating each schema (M6) and static membership, truly
concurrent writes to one document are uncommon in normal operation — the primary serialises them. So
M8's causal machinery earns its keep three ways: (1) it makes **delete reconciliation correct now**
(tombstones); (2) it lets read-repair **heal a replica that missed a create** safely (a deleted doc is
now a tombstone, not an absent file, so "absent" unambiguously means "never had it"); and (3) it is the
correctness foundation that M9 (dynamic membership / multi-primary, where real concurrency arises)
requires. Concurrent-conflict resolution is demonstrated by tests that inject divergent clocks.

## 2. Goals / non-goals / anti-goals

### Goals
- A `VectorClock` (map node→counter) with `increment(node)`, `merge(other)` (pointwise max), and
  `compare(other)` → {EQUAL, DESCENDS, DESCENDED_BY, CONCURRENT}.
- Every document carries `_vclock` (and a coordination timestamp `_ts`); the coordinating primary
  increments its own entry on each create/update/delete.
- A pure **reconcile(a, b)** that returns the winner (the one that descends from the other; on
  CONCURRENT, the higher `_ts`, then the higher node id) with the **merged** clock, and is
  tombstone-aware (a tombstone is a normal participant).
- **Deletes are tombstones**: the delete writes a marker `{_deleted:true, _vclock, _version, _ts}` in
  place of the document file (it is not removed); the document is removed from indexes.
- **Reads skip tombstones**: read-by-id of a tombstoned id returns "Document not found"; read-all omits
  tombstones.
- **Read-repair is causal + tombstone-aware** (replaces M7's version-guarded repair): gather copies
  (including tombstones) from the replicas, reconcile to a winner, and repair every replica strictly
  dominated by it — including propagating a tombstone winner over a stale live copy (closes
  resurrection) and healing a replica that is genuinely absent from a live winner.
- Degrades cleanly: single node → the clock has one entry and reconciliation is trivial; existing
  behaviour (optimistic-lock `_version`, quorum, routing) is preserved.
- The existing suite stays green; new tests cover the clock algebra, reconcile (incl. concurrent and
  tombstone cases), tombstone reads, and causal read-repair.

### Non-goals (this milestone)
- **Sibling / application-level conflict resolution** (returning a set of concurrent versions): we
  auto-resolve to one so the read API stays single-document. Siblings are a future option.
- **Tombstone garbage collection** and **vector-clock pruning / size bounds** (clocks grow with the
  node set, tombstones accumulate) — documented follow-ups; out of scope here.
- **Dynamic membership / multi-primary** (M9) — M8 is built and tested under static membership; it is
  the foundation M9 builds on.
- Replacing the `_version` optimistic-lock the gateway UI uses — `_version` stays (the primary keeps
  bumping it); `_vclock` is the inter-replica causality mechanism, additional to it.
- Hinted handoff / anti-entropy (background convergence).

### Anti-goals (would make this a failure even if it "works")
- A reconcile that lets an **older** value overwrite a **newer** one, or **resurrects** a deleted
  document (the exact classes M7 fought).
- Breaking schema validation for normal documents, the single-node run, or M6/M7 routing/quorum.

## 3. Constraints

- Java 17 / Spring Boot 2.7.6; the M5 one-jar module; JDK 17.
- Builds on M6/M7: `RingRouter`, `Forwarder`, `ServiceTokens`, `QuorumReadService`, `WriteOperation`
  (create/update/apply/repair/delete), `JsonKeys.VERSION`, the per-node name identity.
- `Validators.validateDocument` validates a document against its schema. Reserved keys (`_version`, and
  the new `_vclock`, `_ts`, `_deleted`) must be ignored by validation; a **tombstone** has no schema
  fields, so tombstone writes/applies **bypass** `validateDocument`.
- No wire-protocol change except the added fields travelling inside the document JSON and the
  tombstone-aware repair body; peer calls keep the service JWT.
- Author every commit as Mahmoud Haifawi; no attribution trailers.

## 4. The design (decided "how")

### 4.1 The clock and the document shape
`VectorClock` wraps a `Map<String,Integer>` (node name → counter). Serialised as the `_vclock` JSON
object on the document. A document gains:
- `_vclock` — the vector clock.
- `_ts` — the coordinating node's wall-clock millis at the write (the concurrent-conflict tiebreak).
- `_deleted` — present and `true` only on a tombstone.
`_version` stays as-is (server-controlled integer, the gateway's optimistic lock).

### 4.2 Stamping (on the coordinating primary)
Under `WriteOperation`'s existing lock, keyed by the node's own name `self = Network.getSelf()` (or
`"localhost"` single-node):
- **create**: `_version=1`, `_vclock = {self:1}`, `_ts=now`.
- **update**: load old; keep its `_vclock`, `increment(self)`; `_version = old+1`; `_ts=now`.
- **delete**: write a **tombstone** `{_deleted:true, _version=old+1, _vclock=old.increment(self), _ts=now}`
  in place of the file (do not remove it); fire `onDelete` (drop from indexes). The optimistic
  `_version==expected` check is kept for the client delete.
Replicas receive the coordinator-stamped doc/tombstone and reconcile it (4.4).

### 4.3 Reads skip tombstones
`ReadService.fetchById`: if the stored doc has `_deleted==true`, return `ERROR "Document not found"`.
`fetchAll`: omit tombstoned documents. (Indexes already exclude deleted docs via `onDelete`.) The
quorum read (`QuorumReadService`) gathers tombstones too so it can reconcile and repair them, but what
it returns to the client is not-found when the reconciled winner is a tombstone.

### 4.4 Causal apply + reconcile
A pure `Reconcile.winner(JSONObject a, JSONObject b)`:
1. Compare `_vclock`s. If one DESCENDS the other → that one wins, unchanged.
2. If CONCURRENT → winner = higher `_ts`; tie on `_ts` → higher coordinating node id; the winner's
   `_vclock` is replaced by `merge(a,b)` so it now dominates both (no re-conflict).
3. EQUAL → either (identical).
A tombstone is an ordinary participant: it wins or loses by the same rules.

The internal replication apply (`applyUpdate`/`applyDelete`, used by the primary's fan-out and by
read-repair) becomes **reconcile-and-store**: load the local copy (if any), `Reconcile.winner(local,
incoming)`, and write the winner only if it differs from the local copy (never write an older/ dominated
value). This is idempotent and safe under divergence. Normal primary-coordinated writes always descend
from the replica's copy, so they always apply.

### 4.5 Read-repair (causal, tombstone-aware)
`QuorumReadService` gathers each replica's copy *including tombstones* (the internal local-read returns
the tombstone, with its `_vclock`/`_deleted`). It reconciles all gathered copies to a single winner. It
returns not-found to the client if the winner is a tombstone, else the winner document. Read-repair then
sends the winner to every replica whose copy is strictly dominated by it (or absent), via a
**tombstone-aware** `repairDocument` that, under the lock, writes the winner iff the local copy is
strictly dominated by it (by `_vclock`, falling back to the concurrent tiebreak) — this now covers:
a stale live copy upgraded; a stale live copy **tombstoned** (delete propagated — no resurrection); an
absent replica **healed** from a live winner (safe, because a deleted doc is a tombstone, not absent).

### 4.6 Config & demo
No new env. Demo (5 nodes, RF=3, QUORUM): delete a document; take a replica down before the delete so
it misses the tombstone; bring it back and re-wire; a read now returns not-found and the stale replica
is tombstoned (the delete can no longer be undone) — the concrete fix over M7. RUN.md documents vector
clocks, tombstones, and that concurrent conflicts are auto-resolved (not surfaced as siblings) and that
tombstones are not yet garbage-collected.

## 5. What's left to the builder
- Exact `VectorClock` storage/serialisation and the `compare` implementation; the reserved-key handling
  in `Validators` (ignore `_version`/`_vclock`/`_ts`/`_deleted`; tombstone writes bypass validation).
- Whether the normal fan-out apply reuses the same reconcile-and-store path as read-repair (preferred:
  one causal apply) or keeps a fast path for the descends case.
- Tombstone representation details (how `fetchAll`/index interaction skips them) and the quorum-read
  "winner is a tombstone → not-found" wiring.
- Test design: clock algebra (descends/concurrent/merge), reconcile (concurrent tiebreak, tombstone vs
  live, tombstone vs tombstone), tombstone reads, and the causal read-repair cases (upgrade, delete
  propagation / no resurrection, missing-copy heal), via the M4b/M6/M7 `HttpServer` stub-peer pattern.

The implementation plan (superpowers:writing-plans) follows approval of this document.
