# M7 — Tunable quorum reads/writes + read-repair

Status: design — pending review before implementation planning.
Milestone: M7 of the capstone roadmap (second "standout" distributed-systems feature; builds on M6).
Workflow: work directly on `master`; canonical remote `distributed-document-store`.

## 1. Intent / why

M6 partitions each schema onto a preference list of N replicas, but the replication is all-or-nothing
in intent and consistency-blind in practice: a write is sent to every replica and reported SUCCESS on
the coordinator's **local** apply regardless of whether the replicas actually persisted it, and a read
is served from a **single** replica with no freshness check and no healing. So a client can write, hit
a replica that missed the update, and read stale data forever.

M7 adds **tunable quorum consistency** on top of M6's preference list — the Dynamo/Cassandra model, and
the single most recognizable distributed-systems concept to be able to explain:

- A write is acknowledged to the client only when **W** replicas have persisted it.
- A read gathers from **R** replicas, returns the freshest value (by the existing integer `_version`),
  and **read-repairs** any queried replica it finds behind.
- **W** and **R** are tunable via consistency levels `ONE` / `QUORUM` / `ALL`; choosing `W + R > N`
  guarantees a read sees the latest acknowledged write (read-your-writes / strong consistency), while
  lower levels trade consistency for latency and availability. Default is `QUORUM` for both.

Conflict resolution is **last-write-wins by `_version`** (the server-assigned integer from M2). Causal
conflict detection (vector clocks) is M8; M7 deliberately uses the simpler version comparison.

## 2. Goals / non-goals / anti-goals

### Goals
- Consistency levels `ONE`/`QUORUM`/`ALL` mapping to a replica count over N (the schema's replica
  count): `ONE`=1, `QUORUM`=⌊N/2⌋+1, `ALL`=N. Configured by `WRITE_CONSISTENCY` / `READ_CONSISTENCY`
  (default `QUORUM`), read once like the other ring config.
- **Quorum writes** (create / update / delete / schema-create): the coordinator (the schema's primary)
  applies locally and replicates to the other replicas, then returns SUCCESS iff at least **W** replicas
  (counting the coordinator) acknowledged; otherwise an explicit "write quorum not met (X/W)" error. The
  write is still sent to all replicas for durability; acknowledged-but-under-quorum data is not rolled
  back (it heals via read-repair).
- **Quorum read + read-repair for read-by-id**: the coordinating replica gathers the document from **R**
  replicas (itself + others via an internal local-read), returns the copy with the highest `_version`,
  and upgrades any queried replica that returned a strictly lower version of the present document.
- Degrades cleanly: single node (N=1) → every level is 1 → today's behavior; `ONE`/`ONE` reproduces
  M6's fire-and-serve behavior.
- The existing suite stays green; new tests cover the quorum math, the write verdict, and read-repair.

### Non-goals (this milestone)
- **Tombstones / delete reconciliation.** Without a tombstone, a replica that missed a delete is
  indistinguishable from one that never had the document, so "repairing" it would resurrect deleted
  data. M7 therefore scopes read-repair to **version upgrades of a present document** and does **not**
  create a document on a replica that reports "not found" (so a missed delete is never resurrected, and
  a genuinely missing copy is simply left for a later re-write / future anti-entropy). Tombstones +
  full missing-copy repair are a documented follow-up (anti-entropy / with M8).
- **Hinted handoff** and **anti-entropy / Merkle-tree repair** (background convergence) — later.
- **Quorum on read-all and index-query** — those stay M6 single-replica reads (per-document
  reconciliation across replicas and index divergence are large separate problems). Documented.
- **Per-request consistency override** (e.g. an `X-Consistency` header per call) — the configured
  per-node default is the core; a per-request override is a clean optional extension, deferred.
- **Vector clocks / causal consistency** — M8.

### Anti-goals (would make this a failure even if it "works")
- Reporting a write SUCCESS that fewer than W replicas actually persisted (the exact bug M7 fixes).
- Read-repair that **resurrects deleted documents** or corrupts a newer value with an older one.
- Breaking the single-node run or M6's routing/sharding.

## 3. Constraints

- Java 17 / Spring Boot 2.7.6; the M5 one-jar role-based module; build with JDK 17.
- Builds on M6: `RingRouter.replicasFor/primaryFor/isPrimary/isReplica/otherReplicas`, `Forwarder`,
  `Broadcaster` (awaits all tasks, isolates per-task failure), the `_version` field (`JsonKeys.VERSION`).
- No change to the gateway↔node or inter-node wire protocol except an added internal local-read endpoint
  and the read-repair internal call; peer calls keep using the service JWT.
- Author every commit as Mahmoud Haifawi; no attribution trailers.

## 4. The design (decided "how")

### 4.1 Consistency levels and quorum counts
A new `ConsistencyConfig` (Node util): `WRITE_CONSISTENCY` / `READ_CONSISTENCY` env, each `ONE`/`QUORUM`/
`ALL`, default `QUORUM`. A `Quorum.required(level, n)` maps a level + replica count to a required count:
`ONE`→1, `QUORUM`→`n/2 + 1`, `ALL`→`n`. `n` for a given schema is `RingRouter.replicasFor(db,schema).size()`
(already clamped to cluster size), so a 3-replica schema has `QUORUM`=2, a single node has every level=1.

### 4.2 Quorum writes
The write coordinator is the schema's primary (M6 already forwards there). After it applies locally and
calls `WriteService.broadcast*` (which, via `Broadcaster`, returns one `Response` per other replica), it
computes an **ack count** = 1 (its own successful local apply) + the number of replica responses whose
type is SUCCESS, and compares to `W = Quorum.required(WRITE_CONSISTENCY, N)`:
- `acks ≥ W` → the client sees SUCCESS (with the normal result, e.g. the new version).
- `acks < W` → the client sees `ERROR "write quorum not met (acks/W)"`.
This is a change only to how the coordinator *summarizes* the existing local+broadcast responses; the
fan-out set (M6's `otherReplicas`) and the local apply are unchanged. Applies to create/update/delete
and schema-create. (If the coordinator's own local apply fails — e.g. a version conflict — that remains
the controlling error as today, before any quorum check.)

### 4.3 Quorum read + read-repair (read-by-id only)
`ReadController.fetchById`, when this node is a replica (today it serves purely locally), instead
coordinates a quorum read:
1. Build the replica set `RingRouter.replicasFor(db,schema)` and read the document from **R** of them:
   this node locally, plus others via an **internal local-read** — the same `/user/read/document`
   endpoint called with the service (INTERNAL) token, where `fetchById` branches on `isInternal()` to
   serve the raw local document (or "not found") with its `_version` **without** coordinating a quorum
   (this avoids recursion; INTERNAL already carries `ROLE_USER`, so no new endpoint or security rule is
   needed).
2. Among the responses, pick the **present** document with the highest `_version` (the winner). Return it.
   If fewer than R replicas respond, return the best available (and note degraded read) rather than fail,
   unless zero replicas have it → the normal not-found/forward behavior.
3. **Read-repair:** for every *queried* replica whose returned version is strictly lower than the winner
   (including a replica that returned an older present copy), send the winning document to it via the
   existing internal replication path (`applyUpdate`, verbatim, which sets the version). A replica that
   returned "not found" is **not** written to (no resurrection — §2 non-goals). Repair is best-effort and
   does not change the value returned to the client.

A non-replica entry node still forwards `fetchById` to `primaryFor` (M6), which then coordinates the
quorum read. `read-all` and `index-query` keep M6 single-replica behavior.

### 4.4 Configuration & demo
`WRITE_CONSISTENCY` / `READ_CONSISTENCY` default `QUORUM`; set on every node + coordinator in
`docker-compose.yml` (same shared `x-cluster-env` block as `REPLICATION_FACTOR`). Demo (5 nodes, RF=3,
QUORUM = W=R=2): a write succeeds with one replica down (2 of 3 ack); then take a replica down, update a
document, bring it back, and a read heals it (its stale copy is upgraded to the latest version). RUN.md
documents the levels and the `W + R > N` guarantee.

## 5. What's left to the builder
- Exact placement of the quorum verdict helper (a small `Quorum` util used by the write controller and
  the read coordinator); the internal local-read is `fetchById`'s `isInternal()` branch (§4.3).
- The read-coordination order (which R replicas to query — self first, then preference-list order) and
  whether read-repair is synchronous within the request (default: yes, bounded to the R queried
  replicas) or dispatched; sync is simpler and testable, async is a noted refinement.
- Test design: quorum math (`ONE`/`QUORUM`/`ALL` over N=1,2,3,5), the write verdict (acks ≥/<W via stub
  replicas), and read-repair (a stale replica is upgraded; a "not found" replica is not resurrected) via
  the M4b/M6 `HttpServer` stub-peer pattern.

The implementation plan (superpowers:writing-plans) follows approval of this document.
