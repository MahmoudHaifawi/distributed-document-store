# M9 — Gossip / SWIM dynamic membership Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the coordinator's one-shot static mesh wiring with a self-organising SWIM membership group — nodes detect failures, gossip membership changes with incarnation-based refutation, reconfigure the consistent-hash ring automatically, join/leave at run time, and hand off the key range a newly-responsible node now owns.

**Architecture:** A single locked `MembershipTable` is the source of truth; `Network.nodes`/`ring` become its projection. A node-role detector `tick()` does SWIM direct + indirect probing over INTERNAL `/swim/**` HTTP endpoints; membership deltas are piggybacked and merged by an incarnation/state precedence rule (gossip). Join is seed-based (coordinator becomes seed + router). On every membership change the ring rebuilds and a `HandoffService` pulls newly-owned schemas (raw, tombstones included) and applies them through M8 `WriteOperation.applyReplicated`. Everything degrades to the existing single-node behaviour, and `GOSSIP_ENABLED=false` restores the old mesh wiring.

**Tech Stack:** Java 17, Spring Boot 2.7.6 (one-jar `app` module), Spring Security (JWT/INTERNAL role), org.json, `com.sun.net.httpserver.HttpServer` stub peers for tests.

**Spec:** docs/superpowers/specs/2026-10-31-m9-gossip-swim-membership-design.md

## Global Constraints

- Build/test with `JAVA_HOME=C:\Program Files\Java\jdk-17`; run from repo root (`mvn -q -B -ntp -pl Node test`, full reactor `mvn -q -B -ntp verify`).
- Work directly on `master`; push to `origin` (`distributed-document-store`). Author EVERY commit as `Mahmoud Haifawi <75939511+MahmoudHaifawi@users.noreply.github.com>`; **no `Co-Authored-By` / attribution trailer of any kind** in any commit message.
- New SWIM code lives in `com.database.atypon.Node.utils.swim` (node module). `Node` model stays immutable (name+port); incarnation/state live only in `MembershipTable`.
- Preserve M6 routing, M7 quorum, M8 causal reconcile + tombstones, and the single-node run. `RingRouter` reads the ring lock-free via the `volatile Network.ring` snapshot — keep it that way (swap a new snapshot, never mutate in place).
- New `/swim/**` endpoints are INTERNAL (`SecurityConfig`: `antMatchers("/swim/**").hasRole("INTERNAL")`), peer calls carry `ServiceTokens.bearer()`.
- Config via env with safe defaults, mirroring `RingConfig.intFromEnv`: `GOSSIP_ENABLED`(true), `SEED_NODES`(derived from `CLUSTER_NODES`), `SWIM_PERIOD_MS`(1000), `SWIM_PING_TIMEOUT_MS`(500), `SWIM_INDIRECT_K`(3), `SWIM_SUSPECT_TIMEOUT_MS`(5000), `SWIM_GOSSIP_LAMBDA`(3), `SWIM_GOSSIP_MAX`(6).
- Handoff and read-repair MUST go through `WriteOperation.applyReplicated` — never a raw write — so a pulled copy is causally merged and a tombstone is never resurrected.
- Tests never use real timers: the detector body is `tick(clock, rng)` driven directly; the scheduler is a thin wrapper.

## Review Focus

- **Single dropped probe must not move data.** One missed `PING` with a successful indirect `PING-REQ` leaves the peer ALIVE and the ring unchanged. → Task 3 test `indirectAckCancelsSuspicion`.
- **Gossip merge is order-independent and deterministic.** The same set of updates about one member, applied in any arrival order, yields the same final state (incarnation wins; equal-incarnation `LEFT=DEAD > SUSPECT > ALIVE`). → Task 2 tests `mergeIsHigherIncarnationWins` / `mergeStatePrecedenceAtEqualIncarnation` / `mergeIsOrderIndependent`.
- **Self-refutation.** A node told it is `SUSPECT`/`DEAD` at incarnation i re-enters as `ALIVE` at i+1 and that dominates. → Task 2 test `selfSuspicionIsRefutedWithHigherIncarnation`.
- **Ring snapshot is never torn.** A read of `Network.ring` during a reconfiguration returns either the old or the new complete ring, never a half-built one. → Task 2 test `ringProjectionSwapsAtomically` (asserts a new `HashRing` instance is published, not mutated).
- **Handoff cannot resurrect or regress.** Pulling a schema whose source holds a tombstone leaves the doc deleted on the receiver; pulling an older copy does not overwrite a newer local one. → Task 7 tests `handoffCarriesTombstone` / `handoffDoesNotRegressNewerLocal`.
- **Degrades to single node.** No seeds/peers → ALIVE, owns everything, detector runs no probes, no handoff. → Task 3 `tickWithNoPeersDoesNothing`, Task 7 `singleNodeDoesNoHandoff`.

---

## Task 1: SWIM config + member value types

**Files:**
- Create: `Node/.../utils/swim/SwimConfig.java`, `Node/.../utils/swim/MemberState.java`, `Node/.../utils/swim/Member.java`
- Test: `Node/.../utils/swim/SwimConfigTest.java`

**Interfaces:**
- Produces: `enum MemberState { ALIVE, SUSPECT, DEAD, LEFT }` with `boolean isLive()` (ALIVE||SUSPECT) and `int precedence()` (ALIVE=0, SUSPECT=1, DEAD=2, LEFT=2). `final class Member { Node node; MemberState state; int incarnation; long lastChange; }` (immutable with `withState(...)`/`withIncarnation(...)` copies). `SwimConfig` static getters: `boolean gossipEnabled()`, `List<Node> seeds()`, `long periodMs()`, `long pingTimeoutMs()`, `int indirectK()`, `long suspectTimeoutMs()`, `int gossipLambda()`, `int gossipMax()` — each `*FromEnv` with default + blank→default + invalid→`IllegalStateException`, mirroring `RingConfig.intFromEnv`. `seeds()` parses `SEED_NODES` as `host:port,...`; blank → parse `CLUSTER_NODES`; blank → empty list.

- [ ] **Step 1: Write failing tests** — `SwimConfigTest`: defaults when env unset; blank→default; invalid int→`IllegalStateException`; `seeds()` parses `a:8080,b:8080`; `seeds()` falls back to `CLUSTER_NODES`; `MemberState.precedence()`/`isLive()` values. (Env read via a seam: `SwimConfig` reads a package-private `Function<String,String> env` defaulting to `System::getenv`, settable in tests — do not rely on mutating the process environment.)
- [ ] **Step 2: Run tests, verify they fail** — `mvn -q -B -ntp -pl Node test -Dtest=SwimConfigTest` → FAIL (classes missing).
- [ ] **Step 3: Implement** the three types per the Interfaces block.
- [ ] **Step 4: Run tests, verify pass.**
- [ ] **Step 5: Commit** — `git add Node/.../utils/swim/SwimConfig.java MemberState.java Member.java SwimConfigTest.java` → `feat(m9): SWIM config + member value types`.

## Task 2: MembershipTable — merge rule, refutation, ring projection

**Files:**
- Create: `Node/.../utils/swim/MembershipTable.java`
- Modify: `Node/.../model/Network.java` (add a package-visible `setLiveMembers(List<Node>)` that replaces `nodes` and rebuilds `ring` under the peers lock, publishing a fresh `HashRing` snapshot; keep `setNodes/addNode/assignSelf` as shims that delegate)
- Test: `Node/.../utils/swim/MembershipTableTest.java`

**Interfaces:**
- Consumes: `Member`, `MemberState`, `SwimConfig`, `Network.setLiveMembers`, `RingConfig.vnodesPerNode`.
- Produces: `@Service MembershipTable` (holds `Map<String,Member>` + `selfName` + `selfIncarnation`, all mutation under one `synchronized`/lock). Methods: `void initSelf(Node self)`; `boolean merge(Member update)` (applies the precedence rule, returns whether state changed, refutes self); `List<Member> snapshot()`; `List<Node> liveNodes()` (state.isLive()); `Member get(String name)`; `List<Member> drainGossip(int max)` + `void enqueue(Member)` (Task 4 fills retransmit — here just the store); `void markSuspect(String name)`/`promoteExpiredSuspectsToDead(long now)`. On any change that alters the live set, call `Network.setLiveMembers(liveNodes())`.

Merge rule (the one algorithm worth pinning):
```
given incoming u for member m (existing e):
  if e == null: store u
  else if u.incarnation > e.incarnation: store u
  else if u.incarnation == e.incarnation && u.state.precedence() > e.state.precedence(): store u
  else: ignore
if the (possibly new) record targets SELF and its state != ALIVE:
  selfIncarnation = max(selfIncarnation, record.incarnation) + 1
  store ALIVE(self, selfIncarnation)   // refutation dominates
recompute live set; if changed -> Network.setLiveMembers(liveNodes())
```

- [ ] **Step 1: Write failing tests** — `MembershipTableTest`: `mergeIsHigherIncarnationWins`; `mergeStatePrecedenceAtEqualIncarnation` (DEAD beats ALIVE at equal inc; ALIVE does not beat DEAD); `mergeIsOrderIndependent` (shuffle a fixed update set → same final map); `selfSuspicionIsRefutedWithHigherIncarnation` (merge SUSPECT(self,2) → table holds ALIVE(self,3)); `deadAndLeftLeaveTheLiveSet`; `liveSetIsAlivePlusSuspect`; `ringProjectionSwapsAtomically` (capture `Network.ring` reference before/after a membership change → different instances, both internally complete). Drive `Network` directly (set self, merge peers, assert `Network.ring.physicalNodeCount()`).
- [ ] **Step 2: Run, verify fail.**
- [ ] **Step 3: Implement** `MembershipTable` + `Network.setLiveMembers`.
- [ ] **Step 4: Run, verify pass**, then `mvn -q -B -ntp -pl Node test` (whole module still green — `Network` change must not break M6/M7/M8).
- [ ] **Step 5: Commit** — `feat(m9): MembershipTable with incarnation merge + atomic ring projection`.

## Task 3: SWIM failure detector + /swim ping endpoints

**Files:**
- Create: `Node/.../utils/swim/SwimDetector.java` (holds `tick(long now, IntUnaryOperator rng)`), `Node/.../utils/swim/SwimClient.java` (peer calls via `RestTemplate`+`ServiceTokens.bearer()`), `Node/.../controllers/SwimController.java`, DTOs `Node/.../utils/swim/dto/{PingMessage,AckMessage,PingReqMessage}.java`
- Modify: `Node/.../security/SecurityConfig.java` (`antMatchers("/swim/**").hasRole("INTERNAL")`)
- Test: `Node/.../utils/swim/SwimDetectorTest.java`

**Interfaces:**
- Consumes: `MembershipTable`, `SwimConfig`, `SwimClient`, `ServiceTokens`.
- Produces: `SwimController` endpoints (all INTERNAL): `POST /swim/ping` → `AckMessage`; `POST /swim/ping-req` (body names target M) → relays a `/swim/ping` to M, returns `AckMessage{acked:boolean}`; (`/swim/ack` is the ping response shape, not a separate endpoint). `SwimDetector.tick(now, rng)`: pick one live peer (rng over a shuffled live list), direct ping (timeout `pingTimeoutMs`); on failure, ping-req to `indirectK` other live peers; if no direct/indirect ack → `membership.markSuspect(M)`. Separately `promoteExpiredSuspectsToDead(now)` runs each tick. `SwimClient.ping(target)`/`pingReq(via,target)` return booleans; a thrown/timed-out call = false.

- [ ] **Step 1: Write failing tests** — `SwimDetectorTest` with `HttpServer` stub peers: `directAckKeepsAlive`; `indirectAckCancelsSuspicion` (direct stub 500/timeout, one indirect stub acks → peer stays ALIVE, ring unchanged); `noAckMarksSuspect`; `suspectTimesOutToDead` (advance injected `now` past `suspectTimeoutMs` → DEAD, dropped from live set); `tickWithNoPeersDoesNothing`. Assert via `MembershipTable.get(name).state` and `Network.ring`.
- [ ] **Step 2: Run, verify fail.**
- [ ] **Step 3: Implement** detector, client, controller, DTOs, security rule.
- [ ] **Step 4: Run, verify pass**, then whole-module `mvn -q -B -ntp -pl Node test`.
- [ ] **Step 5: Commit** — `feat(m9): SWIM failure detector (direct + indirect probe) over internal endpoints`.

## Task 4: Gossip piggyback dissemination

**Files:**
- Modify: `Node/.../utils/swim/MembershipTable.java` (retransmit counters), `SwimDetector.java` + `SwimClient.java` + DTOs (carry `List<MemberUpdate> gossip`), `SwimController.java` (merge inbound gossip, attach outbound)
- Create: `Node/.../utils/swim/dto/MemberUpdate.java` (wire form of a `Member`: name, port, state, incarnation)
- Test: `Node/.../utils/swim/GossipTest.java`

**Interfaces:**
- Produces: `MembershipTable.enqueue(Member)` sets `retransmit = ceil(gossipLambda * log2(liveCount+1))`; `drainGossip(max)` returns the ≤`gossipMax` updates with the highest remaining counters and decrements them (drop at 0). Every `PING`/`PING-REQ`/`ACK` body carries `drainGossip(gossipMax)`; every inbound body is `merge`d update-by-update, and any update that changed state is `enqueue`d for re-gossip.

- [ ] **Step 1: Write failing tests** — `GossipTest`: `updateIsReGossipedBoundedTimes` (an enqueued update drains at most `ceil(λ·log2(N+1))` times then disappears); `drainReturnsHighestCounterFirstCappedAtMax`; `inboundGossipMergesByPrecedence` (a `PingMessage` carrying `DEAD(x,5)` drives table to DEAD(x,5)); `changedUpdatesAreReEnqueued`. Pure where possible; one stub-peer round-trip to prove piggyback attach/merge.
- [ ] **Step 2: Run, verify fail.**
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run, verify pass** + whole-module green.
- [ ] **Step 5: Commit** — `feat(m9): infection-style gossip piggybacked on SWIM probes`.

## Task 5: Join, leave, and the node-role scheduler

**Files:**
- Create: `Node/.../utils/swim/SwimBootstrap.java` (`@Component @Profile("node")`: on `ApplicationReadyEvent` join via seed if `gossipEnabled()`; `@PreDestroy` gossip `LEFT(self)`), `Node/.../utils/swim/SwimScheduler.java` (`@Scheduled(fixedRateString)` → `detector.tick(...)`, `@Profile("node")`)
- Modify: `app` module config to add `@EnableScheduling` on the node role configuration only; `SwimController` add `POST /swim/join` (merge joiner, return full `List<MemberUpdate>`) and `POST /swim/leave`
- Test: `Node/.../utils/swim/JoinLeaveTest.java`

**Interfaces:**
- Consumes: `MembershipTable`, `SwimConfig.seeds()`, `SwimClient`.
- Produces: `/swim/join` returns the seed's whole membership snapshot; joiner adopts it (merges each, marks self ALIVE@0) and rebuilds the ring. `/swim/leave` merges `LEFT(sender)`. `SwimBootstrap.join()` tries seeds in order until one answers; no seed/`gossipEnabled()==false` → stays single-node (no throw).

- [ ] **Step 1: Write failing tests** — `JoinLeaveTest` (stub seed peer): `joinAdoptsSeedMembershipAndRebuildsRing`; `joinWithNoReachableSeedStaysSingleNode`; `leaveMarksLeftAndDropsFromRing`; `gossipDisabledSkipsJoin`.
- [ ] **Step 2: Run, verify fail.**
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run, verify pass** + whole-module green.
- [ ] **Step 5: Commit** — `feat(m9): seed-based join, graceful leave, node-role SWIM scheduler`.

## Task 6: Coordinator as seed + GOSSIP_ENABLED fallback

**Files:**
- Modify: `BootstrappingNode/.../MeshWiringRunner.java` (run only when `GOSSIP_ENABLED=false`), add `BootstrappingNode/.../SwimSeedController.java` (`POST /swim/join` holding a coordinator-side member view) + a light refresh that pulls a data node's `/network/get/nodes` for `/cluster` + client routing
- Modify: `BootstrappingNode/.../security/SecurityConfig.java` (`/swim/**` INTERNAL)
- Test: `BootstrappingNode/.../SwimSeedControllerTest.java`

**Interfaces:**
- Produces: coordinator answers `/swim/join` with its current view and merges the joiner; when `GOSSIP_ENABLED=false`, `MeshWiringRunner` still performs the legacy one-shot wiring (unchanged behaviour); when true, it is inert.

- [ ] **Step 1: Write failing tests** — `SwimSeedControllerTest`: `joinReturnsSeedViewAndRegistersJoiner`; `meshWiringRunsOnlyWhenGossipDisabled` (property drives the `@ConditionalOnProperty`/guard).
- [ ] **Step 2: Run, verify fail.**
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run, verify pass**; `mvn -q -B -ntp -pl BootstrappingNode test`.
- [ ] **Step 5: Commit** — `feat(m9): coordinator becomes a SWIM seed; legacy mesh wiring behind GOSSIP_ENABLED=false`.

## Task 7: Bounded state handoff on membership change

**Files:**
- Create: `Node/.../utils/swim/HandoffService.java`, controller endpoints in a new `Node/.../controllers/HandoffController.java`: `GET /swim/schemas` (the `db.schema` keys this node holds) and `GET /swim/pull?database&schema` (RAW docs **including tombstones** — NOT `ReadService.fetchAll`, which hides them)
- Modify: `MembershipTable`/`Network` change path to fire a `MembershipChanged` callback the `HandoffService` subscribes to (node role)
- Test: `Node/.../utils/swim/HandoffServiceTest.java`

**Interfaces:**
- Consumes: `RingRouter.replicasFor/isReplica/otherReplicas`, `WriteOperation.applyReplicated`, a raw document reader.
- Produces: on a membership change, `HandoffService.onChange()` computes schemas this node is newly a replica for (query a current replica's `/swim/schemas`, keep those where `RingRouter.isReplica` is now true and local data is absent/behind), and for each pulls `/swim/pull` from one current replica (not self), applying every doc via `applyReplicated` (tombstones carried). One pass per change; idempotent. Single-node (`otherReplicas` empty) → no-op.

- [ ] **Step 1: Write failing tests** — `HandoffServiceTest` (stub source peer serving `/swim/schemas` + `/swim/pull`): `handoffPullsNewlyOwnedSchema` (receiver stored copy matches source after `onChange`); `handoffCarriesTombstone` (source holds a tombstone → receiver ends deleted, read-by-id not-found); `handoffDoesNotRegressNewerLocal` (receiver already has a newer vclock → unchanged); `singleNodeDoesNoHandoff`; `handoffIsIdempotent` (second `onChange` writes nothing new).
- [ ] **Step 2: Run, verify fail.**
- [ ] **Step 3: Implement**; `/swim/pull` reads raw files (reuse the storage path builder; do not route through tombstone-hiding reads).
- [ ] **Step 4: Run, verify pass** + whole-module green.
- [ ] **Step 5: Commit** — `feat(m9): bounded state handoff pulls newly-owned schemas, merged via applyReplicated`.

## Task 8: Docs + validation

**Files:**
- Modify: `RUN.md`, `docker-compose.yml` (add the SWIM env to the `x-cluster-env` anchor with defaults; `GOSSIP_ENABLED=true`)

- [ ] **Step 1: Full reactor** — `JAVA_HOME="C:\Program Files\Java\jdk-17" mvn -q -B -ntp verify` → all modules green (record counts).
- [ ] **Step 2: Update `RUN.md`** — a "Dynamic membership (SWIM + gossip)" section: failure detection (direct + indirect probe), gossip with incarnation refutation, automatic ring reconfiguration with no coordinator restart, seed-based join + graceful leave, bounded state handoff, and the honest AP notes (brief per-node ring divergence during a change; partitions re-merge on heal; continuous anti-entropy + hinted-handoff are future work). Replace the M7 note that a restarted node needs `docker compose restart bootstrappingnode` with the SWIM behaviour; keep a line on `GOSSIP_ENABLED=false` restoring the static mesh.
- [ ] **Step 3: Validation** — single-node run from the fat jar (`--role=node`, `GOSSIP_ENABLED` default): `/health` UP; create→read→update→delete→read-by-id = not-found (M8 preserved); confirm the detector logs no probes with no peers. Record observed output honestly; note Docker multi-node not run if the daemon is down (covered by stub tests).
- [ ] **Step 4: Commit** — `docs(m9): document SWIM dynamic membership + gossip`.

---

## Self-Review

**Spec coverage:** Goals map to tasks — MembershipTable+projection (T2), SWIM detection (T3), incarnation refutation (T2), gossip (T4), join/leave (T5), coordinator-seed + fallback (T6), ring reconfig (T2 projection, exercised through T3/T5), handoff (T7), config (T1), single-node degrade (T3/T7), docs+validation (T8). Non-goals (Merkle anti-entropy, hinted handoff, consensus, persistence, UDP) are not implemented. Anti-goals have Review-Focus tests (false-positive guard T3, flapping guard via suspicion+incarnation T2/T3, no-regress/no-resurrect handoff T7, single-node T3/T7, atomic ring swap T2).

**Step scan:** each code step names the type/signature; the only pinned algorithm bodies are the merge rule (T2) and the tick/gossip-decay shapes (T3/T4), which the signatures+tests do not fully determine. No "handle edge cases" placeholders.

**Type consistency:** `Member`/`MemberState`/`MemberUpdate` and `MembershipTable.{merge,snapshot,liveNodes,enqueue,drainGossip,markSuspect,promoteExpiredSuspectsToDead}` are named identically across T2–T7; `Network.setLiveMembers` is the single projection seam (T2) used by all change paths; handoff/read-repair both funnel through `WriteOperation.applyReplicated` (M8).

**Review Focus:** false-positive (one dropped probe), order-independent/deterministic gossip merge, self-refutation, atomic ring snapshot, no-resurrect/no-regress handoff, single-node degrade — each has a named owning test.

**Proportion:** plan is shorter than the spec; task bodies are signatures + test names + assertions, not transcribed code.
