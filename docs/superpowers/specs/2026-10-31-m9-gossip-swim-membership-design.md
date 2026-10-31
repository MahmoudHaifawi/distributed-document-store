# M9 — Gossip / SWIM dynamic membership (+ bounded state handoff)

Status: design — pending review before implementation planning.
Milestone: M9 of the capstone roadmap (fourth "standout" distributed-systems feature; builds on M6/M7/M8).
Workflow: work directly on `master`; canonical remote `distributed-document-store`.

## 1. Intent / why

Through M8 the cluster is **statically wired**: the coordinator (`MeshWiringRunner`, coordinator role)
reads `CLUSTER_NODES` once at start-up and POSTs each data node its peer list (`/network/add/nodes`)
and identity (`/network/assign/self`). From then on membership never changes. A node that dies stays
in every peer's `HashRing`; M7 quorum tolerates it for *reads/writes*, but the ring still lists it,
placement never adapts, and a restarted node serves stale local data until an operator runs
`docker compose restart bootstrappingnode` to re-wire the one-shot mesh. There is no failure
detection and no way to add or remove a node at run time.

M9 makes the cluster **self-organising**. Each data node runs the **SWIM** protocol (Scalable
Weakly-consistent Infection-style Process Group Membership): it periodically probes a random peer,
uses *indirect* probes through other peers to avoid false positives, and marks an unresponsive peer
`SUSPECT` then `DEAD`. Membership changes (joins, voluntary leaves, suspicions, deaths) are
disseminated **by gossip** — piggybacked on the probe messages — so the whole cluster converges on
the live member set in O(log N) rounds with no central authority. Each node's `HashRing` is rebuilt
from the live set as it changes, so M6 placement, M7 quorum and M8 causal reconcile all adapt
automatically. A **bounded state handoff** moves the data a newly-responsible node now owns to it, so
a joined node actually serves its share instead of waiting to be healed one read at a time.

Being able to say "membership is a SWIM group disseminated by infection-style gossip with incarnation
numbers for refutation; the consistent-hash ring reconfigures itself on every change and the affected
node pulls its new key range, reconciled by vector clock" is the standout outcome. It is the natural
capstone of M6→M7→M8: M8's causal layer is exactly what makes moving data between nodes safe.

**Honest framing.** This is the AP, eventually-consistent membership model (Dynamo/SWIM/Serf), not a
consensus-based one (no Raft/Paxos): during a change different nodes briefly disagree on the ring, and
a network partition yields two rings that re-merge when it heals. That is the correct trade-off for
this system and is stated as such. The state handoff is a **one-shot, bounded** pull per membership
change, not continuous background anti-entropy — fast convergence after a change, with Merkle-tree
anti-entropy and hinted-handoff left as named follow-ups.

## 2. Goals / non-goals / anti-goals

### Goals
- A single **`MembershipTable`** component: the authoritative map `member → {state ∈ {ALIVE, SUSPECT,
  DEAD, LEFT}, incarnation:int, lastChange}`, mutated only under its own lock. `Network.nodes`/`ring`
  become a **projection** of its live members (ALIVE + SUSPECT), rebuilt atomically on every change —
  retiring M6's loose static mutation and the "ring rebuilt without lock / self-unset window" gaps.
- **SWIM failure detection** on each data node, on a period `T`: direct `PING` of one random live peer;
  on timeout, `PING-REQ` fanned to `k` random peers (indirect probe); if neither acks, the peer is
  marked `SUSPECT`. A `SUSPECT` becomes `DEAD` after a suspicion timeout unless refuted.
- **Incarnation numbers** for refutation: a node owns its own incarnation and is the only one that
  increments it; on learning it is `SUSPECT`, it bumps its incarnation and gossips `ALIVE`, overriding
  the suspicion. Merge precedence: higher incarnation wins; at equal incarnation, `DEAD`/`LEFT` >
  `SUSPECT` > `ALIVE`.
- **Gossip dissemination**: a bounded set of recent membership updates is piggybacked on every
  `PING`/`ACK`/`PING-REQ` body and merged by the precedence rule; each update is re-gossiped a bounded
  number of times (~`λ·log N`). No separate broadcast.
- **Join**: a node starts knowing only a configured **seed** set (`SEED_NODES`); it sends `JOIN(self)`
  to a seed, receives the current member list in the reply, adopts it, and its arrival gossips out. The
  coordinator becomes a **seed + client router**, no longer the membership authority.
- **Graceful leave**: a shutdown hook gossips `LEFT(self)` (a terminal, non-refutable state) so peers
  drop it from the ring promptly instead of waiting for the failure detector.
- **Automatic ring reconfiguration**: whenever the live set changes, every node rebuilds its ring;
  because hashing is consistent, only boundary keys move.
- **Bounded state handoff**: when a membership change makes this node a replica for a schema it is
  missing (or stale on), it pulls that schema's documents once from a current replica and applies each
  through M8 `WriteOperation.applyReplicated` (causal merge — safe, idempotent, never resurrects or
  regresses). One pass per change, not continuous.
- Degrades cleanly: a single node with no seeds/peers is ALIVE, owns everything, runs no probes
  (`RingRouter`'s single-node short-circuit is unchanged). All of M6/M7/M8 behaviour is preserved.
- The existing suite stays green; new tests cover the SWIM state machine, the gossip merge, join/leave,
  ring reconfiguration, and the handoff pull — via the established `com.sun.net.httpserver.HttpServer`
  stub-peer pattern and a directly-driven detector tick (no real timers in tests).

### Non-goals (this milestone)
- **Continuous background anti-entropy** (Merkle-tree tree-walk reconciliation between replicas) — the
  handoff here is triggered one-shot by a membership change; drift between changes still relies on M7/M8
  read-repair. Merkle anti-entropy is the next data-convergence milestone.
- **Hinted handoff** (buffering writes aimed at a down replica and replaying them on return).
- **Consensus membership / strong reconfiguration** (Raft/Paxos). Membership stays AP/eventually
  consistent; brief per-node ring disagreement during a change is expected.
- **Persistent membership across a full-cluster cold start** — on a total restart the cluster
  re-bootstraps from seeds; membership is in-memory.
- **UDP transport, piggyback compression, cryptographic gossip beyond the existing service JWT,
  multi-datacenter / rack awareness.**

### Anti-goals (would make this a failure even if it "works")
- A **false positive** that evicts a healthy node and triggers needless data movement — the indirect
  probe, the suspicion period, and incarnation refutation exist precisely to prevent this; a single
  dropped packet must not move data.
- **Membership flapping** (a node oscillating ALIVE/DEAD) churning the ring.
- Breaking the **single-node** run, or M6 routing / M7 quorum / M8 causal reconcile and tombstones.
- A partition that, on heal, **fails to re-converge** the two membership views, or a handoff that
  **overwrites newer data** with older (handoff must go through `applyReplicated`, never a raw write).

## 3. Constraints

- Java 17 / Spring Boot 2.7.6; the M5 one-jar module; build with `JAVA_HOME=C:\Program Files\Java\jdk-17`.
- Builds on M6 (`HashRing`, `RingRouter`, `RingConfig`, `Network`), M7 (`Forwarder`, `ServiceTokens`,
  quorum), M8 (`WriteOperation.applyReplicated`, `Reconcile`, vector clocks + tombstones). The ring and
  router already derive everything from membership — M9 changes *who maintains membership and how*, not
  how placement is computed.
- Transport is **HTTP**, consistent with the rest of the system: new INTERNAL endpoints under
  `/swim/**` (Bearer service JWT, gated like `/network/**`), peer calls via `Forwarder`/`RestTemplate`.
  (SWIM is classically UDP; HTTP is chosen for stack consistency and testability, and documented as the
  deliberate trade-off — higher per-probe cost, same protocol semantics.)
- A periodic detector loop requires scheduling: add `@EnableScheduling` (or a dedicated daemon thread)
  on the **node role only**; the detector's body is a `tick()` method callable directly from tests.
- `Node` stays immutable (name + port); incarnation/state live in the `MembershipTable`, keyed by node
  name, never on the `Node` model.
- Author every commit as Mahmoud Haifawi; no attribution trailers.

## 4. The design (decided "how")

### 4.1 MembershipTable — the single source of truth
A `@Service MembershipTable` holds `Map<String /*node name*/, Member>` where
`Member = {Node node, MemberState state, int incarnation, long lastChange}` and this node's own
`selfIncarnation`. All mutation is under one lock. Its merge method applies one gossiped update by the
precedence rule (§4.3) and returns whether anything changed; on any change it recomputes the live set
(ALIVE + SUSPECT) and calls into `Network` to **replace `nodes` and rebuild `ring` atomically**. This
subsumes the current `Network.setNodes/addNode/assignSelf` paths (kept as thin shims for the
coordinator seed bootstrap and single-node assignment). `SUSPECT` members stay in the ring (still
routed to, so a transient blip does not move data); only `DEAD`/`LEFT` are removed.

### 4.2 Failure detection loop (node role)
Every `T` = `SWIM_PERIOD_MS` (default 1000): choose one random `ALIVE` peer `M` (round-robin over a
shuffled list so every member is probed within a bounded number of periods). Send `PING(self, gossip)`
to `M` with timeout `SWIM_PING_TIMEOUT_MS` (default 500). If `ACK` arrives, `M` is alive. Otherwise send
`PING-REQ(M, gossip)` to `k` = `SWIM_INDIRECT_K` (default 3) other random `ALIVE` peers; each relays a
`PING` to `M` and returns whether it got an `ACK`. If any indirect `ACK` returns, `M` is alive;
otherwise mark `M` `SUSPECT` locally and gossip it. The loop body is `tick()`; randomness and clock are
injected so tests drive it deterministically.

### 4.3 States, incarnation, and the merge rule
States: `ALIVE`, `SUSPECT`, `DEAD`, `LEFT`. Each member carries an `incarnation`. Updates (gossiped or
local) merge into a member by:
1. higher `incarnation` wins outright;
2. at equal `incarnation`, state precedence `LEFT = DEAD > SUSPECT > ALIVE` wins (a stronger "bad"
   state is never overwritten by a weaker one at the same incarnation).
Refutation: when a node sees an update that says *it itself* is `SUSPECT`/`DEAD` at incarnation `i`, it
sets `selfIncarnation = i + 1` and gossips `ALIVE(self, i+1)`, which dominates. Only the owning node
ever raises its own incarnation. A `SUSPECT(M, i)` not refuted within `SWIM_SUSPECT_TIMEOUT_MS`
(default 5000) is promoted locally to `DEAD(M, i)` and gossiped. `LEFT` is terminal and non-refutable
(graceful departure).

### 4.4 Gossip (piggyback dissemination)
No standalone broadcast. The `MembershipTable` keeps a small queue of recent updates, each with a
`retransmit` counter initialised to `ceil(λ · log2(N+1))` (λ via `SWIM_GOSSIP_LAMBDA`, default 3).
Every outgoing `PING`/`PING-REQ`/`ACK` body carries up to `SWIM_GOSSIP_MAX` (default 6) updates with the
highest remaining counters; each send decrements them; at zero an update drops out. Incoming bodies are
merged by §4.3; genuinely new/changed updates are (re)enqueued. This is infection-style dissemination:
expected convergence O(log N) periods, each update sent O(log N) times.

### 4.5 Join and leave
**Seeds:** `SEED_NODES` (comma-separated `host:port`, default = the coordinator, or the first
`CLUSTER_NODES` entry for backward-compatible docker-compose). **Join:** on start-up a node with
`GOSSIP_ENABLED=true` sends `POST /swim/join` (body: self as a `Member` at incarnation 0) to a seed; the
seed merges it, replies with its **full member list**; the joiner adopts the list (marking itself
ALIVE) and begins probing. Its arrival propagates by gossip. **Leave:** a Spring `@PreDestroy`/shutdown
hook sends `LEFT(self)` to a few peers (best-effort) so the ring drops it immediately; if the process is
killed instead, the detector takes the SUSPECT→DEAD path. **Coordinator:** `MeshWiringRunner` is
replaced (under `GOSSIP_ENABLED`) by seed behaviour — it answers `/swim/join`, holds a membership view
for `/cluster` and client routing, and refreshes that view by pulling a data node's live members; it
does **not** run the failure detector. With `GOSSIP_ENABLED=false` the old one-shot mesh wiring remains
as a fallback, so nothing that works today breaks.

### 4.6 Ring reconfiguration + bounded state handoff
Every membership change rebuilds the ring (§4.1). After a rebuild, a `HandoffService` (node role)
computes the schemas for which **this node is newly a replica** (it is in `replicasFor` now but has no
local data / is behind). It cannot know schemas it has never held, so it asks a current replica for the
relevant schema list via a new INTERNAL endpoint (`GET /swim/schemas` → the `db.schema` keys this node
holds); for each target schema it picks a current replica from the ring (not self) and pulls that
schema's documents through the existing internal read path, applying each via
`WriteOperation.applyReplicated` — so a pulled copy is causally merged, never clobbering newer local
data, and tombstones are carried (no resurrection). The handoff is **one-shot per change**, size-bounded
by the schemas actually moving, and idempotent (re-running converges to the same state). It is the part
most reasonably deferred: if descoped, a joined node still converges via M7/M8 read-repair, only slower
— the plan will isolate it as the last sub-phase.

### 4.7 Config & demo
New env (all with safe defaults; no change required for the existing single-node run):
`GOSSIP_ENABLED` (true), `SEED_NODES`, `SWIM_PERIOD_MS` (1000), `SWIM_PING_TIMEOUT_MS` (500),
`SWIM_INDIRECT_K` (3), `SWIM_SUSPECT_TIMEOUT_MS` (5000), `SWIM_GOSSIP_LAMBDA` (3), `SWIM_GOSSIP_MAX` (6).
docker-compose keeps `CLUSTER_NODES` as the seed source. **Demo:** 5 nodes, RF=3, QUORUM. (1) `docker
stop Node3` — within a few seconds the survivors probe → indirect-probe → SUSPECT → DEAD, gossip it out,
and the ring reconfigures with **no coordinator restart**; reads/writes continue on quorum; the M4c
`/cluster` topology view shows it down. (2) Start a new `Node5` — it joins via a seed, is gossiped in,
pulls the schemas it now owns, and serves its share. RUN.md documents SWIM, gossip, incarnation
refutation, the AP trade-off (brief divergence during change; partitions re-merge on heal), and that
continuous anti-entropy and hinted-handoff are future work.

## 5. What's left to the builder
- Exact `MembershipTable` storage, lock discipline, and the `Network` projection hand-off (replace the
  static mutation while keeping `RingRouter`'s reads lock-free via the `volatile` ring snapshot).
- The `/swim/**` request/response DTOs (ping/ping-req/ack/join/leave + the piggybacked update list) and
  wiring them INTERNAL in `SecurityConfig` (antMatcher `/swim/**` → `hasRole("INTERNAL")`), mirroring
  `/network/**`.
- The detector's randomness/clock injection so `tick()` is deterministic in tests; whether scheduling is
  `@Scheduled` on a node-profile bean or a dedicated daemon thread.
- The seed-join reply shape and the coordinator's seed/refresh behaviour and `GOSSIP_ENABLED=false`
  fallback to `MeshWiringRunner`.
- Handoff specifics: the `/swim/schemas` listing, which replica to pull from, batching, and that every
  applied doc goes through `applyReplicated` (never a raw write), tombstones included.
- Test design: SWIM state machine (alive→suspect→dead, indirect-ack cancels suspicion, incarnation
  refutation); gossip merge precedence (incarnation vs state, re-transmit decay); join adopts member
  list + rebuilds ring; leave drops from ring; handoff pulls + `applyReplicated` merges (and does not
  resurrect a tombstoned doc); single-node still runs no probes — all via the `HttpServer` stub-peer
  pattern and a directly-driven `tick()`.

Likely plan decomposition (for the planning step, not binding): **M9.1** `MembershipTable` + SWIM
detection core + `/swim/ping`,`/ping-req`,`/ack` (unit-testable state machine); **M9.2** gossip
piggyback merge + join/leave + ring reconfiguration + coordinator-as-seed + `GOSSIP_ENABLED` fallback;
**M9.3** bounded state handoff (`/swim/schemas`, pull + `applyReplicated`) + RUN.md + validation.

The implementation plan (superpowers:writing-plans) follows approval of this document.
