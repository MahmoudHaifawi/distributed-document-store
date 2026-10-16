# M7 — Tunable Quorum + Read-Repair Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add tunable consistency levels (ONE/QUORUM/ALL) over M6's replicas: a write acks the client only when W replicas persist; a read-by-id gathers from R replicas, returns the freshest by `_version`, and read-repairs stale replicas.

**Architecture:** A pure `Quorum`/`ConsistencyConfig` maps a level + replica count to a required count. The write coordinator (M6 primary) summarizes its local apply + replica acks against W. The read coordinator (a replica) gathers the document from R replicas via internal local-reads, picks the highest `_version`, returns it, and upgrades present-but-stale replicas via the existing internal `applyUpdate`. Last-write-wins by `_version`; no tombstones, so a "not found" replica is never resurrected.

**Tech Stack:** Java 17, Spring Boot 2.7.6, the M5 one-jar module, JUnit 5 + AssertJ, JDK `com.sun.net.httpserver.HttpServer` stub peers (as in M4b/M6).

**Spec:** `docs/superpowers/specs/2026-10-16-m7-quorum-read-repair-design.md`

## Global Constraints

- Build with `JAVA_HOME=C:\Program Files\Java\jdk-17`; run the reactor from the repo root.
- Consistency levels: `ONE`/`QUORUM`/`ALL`, configured by `WRITE_CONSISTENCY` / `READ_CONSISTENCY` env, default `QUORUM`. Counts over the schema's replica count `n` (= `RingRouter.replicasFor(db,schema).size()`, already clamped): `ONE`=1, `QUORUM`=`n/2 + 1`, `ALL`=`n`; never exceed `n`, never below 1.
- Builds on M6: `RingRouter.{replicasFor,primaryFor,isPrimary,isReplica}`, `Forwarder`, `Broadcaster`, `ServiceTokens.bearer()`, the `_version` field (`JsonKeys.VERSION` = `"_version"`), `WriteService.applyUpdate` (internal verbatim update), `ReadService.fetchById` (returns the raw doc JSON as `content`, or ERROR "Document not found").
- Conflict resolution is last-write-wins by `_version`. Read-repair upgrades a present document on a replica that returned a strictly lower version; a replica that returned not-found is **never** written to (no delete resurrection).
- Quorum reads + repair apply to **read-by-id only**; `read-all` and `index-query` keep M6 single-replica behavior.
- Internal node-to-node calls (gather + repair) use `ServiceTokens.bearer()`; the target serves them locally (the INTERNAL branch), never re-coordinating.
- Author every commit as Mahmoud Haifawi; no attribution trailers. Work on `master`; remote `origin` = `distributed-document-store`.

## Review Focus

- **Single node / N=1:** every level resolves to 1; a write needs only its local apply, a read does no gather/repair — behavior identical to M6. → Task 1 + existing single-node tests stay green.
- **Write with fewer than W acks** (replicas down/error): the client must see "write quorum not met (X/W)", not a false SUCCESS. → Task 2 integration test.
- **Read with a stale replica** (older `_version`): the returned value is the freshest AND the stale replica is upgraded via internal `applyUpdate`. → Task 3 integration test.
- **Read with a not-found replica** (missed create or a delete): it must NOT be written to (no resurrection). → Task 3 unit + integration test.
- **Read with fewer than R replicas reachable:** return the best present value (degraded read) rather than hard-fail, as long as ≥1 replica has the document. → Task 3 test.

---

## Task 1: `ConsistencyLevel` + `ConsistencyConfig` + `Quorum.required` (pure)

**Files:**
- Create: `Node/src/main/java/com/database/atypon/Node/utils/quorum/ConsistencyLevel.java`
- Create: `Node/src/main/java/com/database/atypon/Node/utils/quorum/ConsistencyConfig.java`
- Create: `Node/src/main/java/com/database/atypon/Node/utils/quorum/Quorum.java`
- Test: `Node/src/test/java/com/database/atypon/Node/utils/quorum/QuorumTest.java`

**Interfaces:**
- Produces:
  - `enum ConsistencyLevel { ONE, QUORUM, ALL }`.
  - `static ConsistencyLevel ConsistencyConfig.writeConsistency()` — env `WRITE_CONSISTENCY`, default `QUORUM`.
  - `static ConsistencyLevel ConsistencyConfig.readConsistency()` — env `READ_CONSISTENCY`, default `QUORUM`.
  - `static int Quorum.required(ConsistencyLevel level, int n)` — `ONE`→1, `QUORUM`→`Math.max(1,n)/2 + 1`, `ALL`→`Math.max(1,n)`; result clamped to `[1, Math.max(1,n)]`.
  - `static int Quorum.ackCount(boolean localSuccess, java.util.List<Response> replicaResponses)` — `(localSuccess?1:0)` + number of `replicaResponses` whose `getResponseType()==SUCCESS`.

- [ ] **Step 1: Write the failing tests** `QuorumTest`:

```java
@Test void requiredCountsPerLevel() {
    assertThat(Quorum.required(ConsistencyLevel.ONE, 3)).isEqualTo(1);
    assertThat(Quorum.required(ConsistencyLevel.QUORUM, 3)).isEqualTo(2);   // majority of 3
    assertThat(Quorum.required(ConsistencyLevel.QUORUM, 5)).isEqualTo(3);
    assertThat(Quorum.required(ConsistencyLevel.ALL, 3)).isEqualTo(3);
}
@Test void everyLevelIsOneForASingleReplica() {
    for (ConsistencyLevel l : ConsistencyLevel.values())
        assertThat(Quorum.required(l, 1)).isEqualTo(1);
}
@Test void ackCountCountsLocalPlusSuccessfulReplicas() {
    List<Response> r = List.of(new Response(ResponseType.SUCCESS,"ok"),
                               new Response(ResponseType.ERROR,"down"));
    assertThat(Quorum.ackCount(true, r)).isEqualTo(2);     // local + 1 success
    assertThat(Quorum.ackCount(false, r)).isEqualTo(1);
}
@Test void configDefaultsToQuorumWhenEnvUnset() {
    assertThat(ConsistencyConfig.writeConsistency()).isEqualTo(ConsistencyLevel.QUORUM);
    assertThat(ConsistencyConfig.readConsistency()).isEqualTo(ConsistencyLevel.QUORUM);
}
```

- [ ] **Step 2: Run to verify they fail** — `... -pl Node test -Dtest=QuorumTest` → FAIL (classes missing).

- [ ] **Step 3: Implement** the enum; `ConsistencyConfig` (env read, blank → `QUORUM`, parse case-insensitively; an unrecognized non-blank value throws `IllegalStateException`); `Quorum.required` and `Quorum.ackCount` with the signatures above.

- [ ] **Step 4: Run to verify they pass** — `... -pl Node test -Dtest=QuorumTest` → PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add Node/src/main/java/com/database/atypon/Node/utils/quorum/ Node/src/test/java/com/database/atypon/Node/utils/quorum/
git commit -m "feat(m7): consistency levels + quorum count/ack helpers"
```

---

## Task 2: Quorum write verdict

Make a write ack the client only when W replicas persisted.

**Files:**
- Modify: `Node/src/main/java/com/database/atypon/Node/controllers/write/WriteController.java`
- Test: `Node/src/test/java/com/database/atypon/Node/controllers/write/WriteQuorumTest.java`

**Interfaces:**
- Consumes: Task 1 `Quorum.required`, `Quorum.ackCount`, `ConsistencyConfig.writeConsistency()`; M6 `RingRouter.replicasFor`.
- Produces: a private `Vector<Response> WriteController.withWriteQuorum(String db, String schema, Response localResult, List<Response> replicaResponses)` — returns `[localResult, ...replicaResponses]`, but when `localResult` is SUCCESS and `Quorum.ackCount(true, replicaResponses) < Quorum.required(ConsistencyConfig.writeConsistency(), RingRouter.replicasFor(db,schema).size())`, replaces element 0 with `new Response(ERROR, "write quorum not met (" + acks + "/" + w + ")")`. When `localResult` is not SUCCESS, returns `[localResult, ...replicaResponses]` unchanged (the local error is controlling).

- [ ] **Step 1: Write the failing test** `WriteQuorumTest` (pure, drives the verdict via a tiny subclass or by extracting the verdict to a package-visible static). Preferred: extract the decision to `Quorum.writeClientResult(boolean localSuccess, Response localResult, List<Response> replicaResponses, int w) -> Response` and test that, then have `withWriteQuorum` call it with the per-schema `w`.

```java
@Test void successWhenAcksMeetW() {
    Response local = new Response(ResponseType.SUCCESS, "ok", 2);
    List<Response> replicas = List.of(new Response(ResponseType.SUCCESS,"ok"), new Response(ResponseType.ERROR,"down"));
    // acks = 2, w = 2
    assertThat(Quorum.writeClientResult(true, local, replicas, 2).getResponseType()).isEqualTo(ResponseType.SUCCESS);
}
@Test void errorWhenAcksBelowW() {
    Response local = new Response(ResponseType.SUCCESS, "ok", 2);
    List<Response> replicas = List.of(new Response(ResponseType.ERROR,"down"), new Response(ResponseType.ERROR,"down"));
    // acks = 1, w = 2
    Response r = Quorum.writeClientResult(true, local, replicas, 2);
    assertThat(r.getResponseType()).isEqualTo(ResponseType.ERROR);
    assertThat(r.getMessage()).contains("quorum not met");
}
@Test void localFailureIsControlling() {
    Response local = new Response(ResponseType.ERROR, "Version conflict");
    assertThat(Quorum.writeClientResult(false, local, List.of(), 2)).isSameAs(local);
}
```
(Add `Quorum.writeClientResult` to the Task-1 `Quorum` class; it is the pure core of `withWriteQuorum`.)

- [ ] **Step 2: Run to verify it fails** — `... -pl Node test -Dtest=WriteQuorumTest` → FAIL.

- [ ] **Step 3: Implement** `Quorum.writeClientResult` and `WriteController.withWriteQuorum`, then route all four origin-path writes through it: `createSchema`, `createDocument`, `updateDocument`, `deleteDocument` each build `responses = withWriteQuorum(db, schema, localResult, broadcastResponses)` instead of appending the broadcast responses directly. (The forward-to-primary and internal branches are unchanged; the broadcast still targets M6's `otherReplicas`.)

- [ ] **Step 4: Run to verify it passes** — `... -pl Node test -Dtest=WriteQuorumTest` → PASS. Then the whole module (`... -pl Node test`) — single-node writes still SUCCEED (N=1 → W=1, local apply is the one ack), so the existing controller/integration tests stay green.

- [ ] **Step 5: Write the integration test** `WriteQuorumIntegrationTest` (`@SpringBootTest(RANDOM_PORT)`, `@TestPropertySource(app.jwt.secret=…)`; default QUORUM). Two `HttpServer` stub peers; set `Network.self` + `Network.nodes=[stub1,stub2]`, `rebuildRing()`; pick a schema whose primary is self (scan). With N=3, W=2: both stubs ACK a document-create → client SUCCESS; one stub returns ERROR → still 2 acks → SUCCESS; both stubs return ERROR → 1 ack < 2 → client response is ERROR "write quorum not met".

- [ ] **Step 6: Run to verify it passes**, then `... -pl Node test` green.

- [ ] **Step 7: Commit**

```bash
git add Node/src/main/java/com/database/atypon/Node/ Node/src/test/java/com/database/atypon/Node/controllers/write/WriteQuorumTest.java Node/src/test/java/com/database/atypon/Node/controllers/write/WriteQuorumIntegrationTest.java
git commit -m "feat(m7): write acks the client only when W replicas persist"
```

---

## Task 3: Quorum read + read-repair (read-by-id)

**Files:**
- Create: `Node/src/main/java/com/database/atypon/Node/utils/quorum/ReplicaDoc.java` (small record/class)
- Modify: `Node/src/main/java/com/database/atypon/Node/utils/quorum/Quorum.java` (add `pickFreshest`)
- Create: `Node/src/main/java/com/database/atypon/Node/services/read/QuorumReadService.java`
- Modify: `Node/src/main/java/com/database/atypon/Node/controllers/read/ReadController.java`
- Test: `Node/src/test/java/com/database/atypon/Node/utils/quorum/PickFreshestTest.java`
- Test: `Node/src/test/java/com/database/atypon/Node/controllers/read/ReadRepairIntegrationTest.java`

**Interfaces:**
- Consumes: Task 1 `Quorum.required`, `ConsistencyConfig.readConsistency()`; M6 `RingRouter.replicasFor`, `ServiceTokens.bearer()`, `ReadService.fetchById`, `WriteService.applyUpdate`, `JsonKeys.VERSION`.
- Produces:
  - `ReplicaDoc` — `{ String node; boolean present; int version; String content; }` (one per queried replica; `version` from the doc's `_version`, default 1; `present=false` for not-found).
  - `static ReplicaDoc Quorum.pickFreshest(List<ReplicaDoc> docs)` — the present doc with the highest `version`, or `null` if none present. (Ties: any one; versions are single-primary-assigned so ties don't diverge.)
  - `Response QuorumReadService.readById(String db, String schema, String id)` — coordinates the quorum read (gather R, pick freshest, repair stale-present, return the freshest doc or ERROR "Document not found").

- [ ] **Step 1: Write the failing pick/repair test** `PickFreshestTest`:

```java
@Test void picksHighestPresentVersion() {
    ReplicaDoc r = Quorum.pickFreshest(List.of(
        new ReplicaDoc("A", true, 1, "{\"_version\":1}"),
        new ReplicaDoc("B", true, 3, "{\"_version\":3}"),
        new ReplicaDoc("C", false, 0, null)));
    assertThat(r.node).isEqualTo("B");
    assertThat(r.version).isEqualTo(3);
}
@Test void returnsNullWhenNonePresent() {
    assertThat(Quorum.pickFreshest(List.of(new ReplicaDoc("A", false, 0, null)))).isNull();
}
@Test void repairTargetsArePresentReplicasBelowWinner() {
    // the service repairs docs that are present AND version < winner; a not-found replica is excluded
    List<ReplicaDoc> docs = List.of(
        new ReplicaDoc("A", true, 1, "{\"_version\":1}"),
        new ReplicaDoc("B", true, 3, "{\"_version\":3}"),
        new ReplicaDoc("C", false, 0, null));
    ReplicaDoc winner = Quorum.pickFreshest(docs);
    List<ReplicaDoc> repair = docs.stream().filter(d -> d.present && d.version < winner.version).collect(toList());
    assertThat(repair).extracting(d -> d.node).containsExactly("A");   // not C (not-found, no resurrection)
}
```

- [ ] **Step 2: Run to verify it fails** — `... -pl Node test -Dtest=PickFreshestTest` → FAIL.

- [ ] **Step 3: Implement** `ReplicaDoc`, `Quorum.pickFreshest`, and `QuorumReadService`:
  - `readById`: `n = RingRouter.replicasFor(db,schema).size()`, `r = Quorum.required(ConsistencyConfig.readConsistency(), n)`. Gather into `List<ReplicaDoc>` from **all** replicas: self via `readService.fetchById` (parse `_version` from its `content`; not-found → `present=false`), and every other replica via `GET <peer>/user/read/document?id=…&databaseName=…&schemaName=…` with header `Authorization: ServiceTokens.bearer()` (an unreachable peer is simply omitted from the gathered list). `r` governs availability, not which replicas are queried: if fewer than `r` replicas respond at all, the read is degraded (still returned if any present; see below) — reading all replicas means the gathered set always overlaps the `W` that acked a write, so `W + R > N` read-your-writes holds. `winner = Quorum.pickFreshest(gathered)`; if null (none present) → `ERROR "Document not found"`. Read-repair: for every gathered `ReplicaDoc` that is `present && version < winner.version`, send the winner doc — self via `writeService.applyUpdate(db,schema,id, winnerDocAsMap)`, a peer via `POST <peer>/write/document/update?database=…&schema=…&id=…` body = winner `content`, header `ServiceTokens.bearer()`. A `present=false` replica is never written to. Return the winner as `new Response(SUCCESS, "Document fetched successfully", winner.content)`.
  - `ReadController.fetchById`: after validation, add `if (isInternal()) return readService.fetchById(id, databaseName, schemaName);` (the internal local-read used by the coordinator — reuses `WriteController`'s `isInternal()` pattern via `SecurityUtils`). Keep the M6 `!isReplica → forward to primaryFor`. Replace the final local serve with `return quorumReadService.readById(databaseName, schemaName, id);`.

- [ ] **Step 4: Run to verify it passes** — `... -pl Node test -Dtest=PickFreshestTest` → PASS. Then `... -pl Node test` — single-node (N=1, R=1, no peers) returns the local doc with no gather/repair, so `NodeSecuredApiIntegrationTest`'s read steps stay green.

- [ ] **Step 5: Write the integration test** `ReadRepairIntegrationTest` (`@SpringBootTest(RANDOM_PORT)`, HttpServer stubs, default QUORUM so R=2 with N=3). Self holds the document at `_version` 3; set `Network.nodes=[staleStub, missingStub]` so both are replicas of the test schema. `staleStub`'s `/user/read/document` returns the doc at `_version` 1; `missingStub`'s returns ERROR not-found. A client `GET /user/read/document` returns the `_version` 3 doc; assert `staleStub` received a `POST /write/document/update?...` carrying the `_version` 3 doc (repaired), and `missingStub` received **no** write (not resurrected). Also a degraded case: with one stub unreachable but self present, the read still returns self's doc.

- [ ] **Step 6: Run to verify it passes**, then `... -pl Node test` green.

- [ ] **Step 7: Commit**

```bash
git add Node/src/main/java/com/database/atypon/Node/ Node/src/test/java/com/database/atypon/Node/
git commit -m "feat(m7): quorum read-by-id with last-write-wins read-repair"
```

---

## Task 4: Config wiring, demo, validation, docs

**Files:**
- Modify: `docker-compose.yml` (consistency env in `x-cluster-env`)
- Modify: `RUN.md`

- [ ] **Step 1: Full reactor build + test** — `JAVA_HOME="C:\Program Files\Java\jdk-17" mvn -q -B -ntp verify` → all modules green (report counts).

- [ ] **Step 2: Add consistency env** to `docker-compose.yml`'s `x-cluster-env` anchor: `WRITE_CONSISTENCY: "QUORUM"` and `READ_CONSISTENCY: "QUORUM"` (so every node + the coordinator share them). Validate `docker compose config` → exits 0.

- [ ] **Step 3: Update `RUN.md`** — add a "Consistency" note: levels `ONE`/`QUORUM`/`ALL` via `WRITE_CONSISTENCY`/`READ_CONSISTENCY` (default `QUORUM`); a write needs W replicas to ack, a read gathers R and read-repairs stale ones; `W + R > N` gives read-your-writes; the 5-node RF=3 demo runs W=R=2, so a write survives one replica down and a read heals a replica that missed an update.

- [ ] **Step 4: Validation** — confirm the single-node local run still works (`java -jar app/target/nosql-db.jar --role=node` from a dir with `data/info.json` → `/health` UP; a write + read-by-id succeed, since N=1 → W=R=1). Record it. (Multi-node quorum behavior is covered by the Task-2/3 stub integration tests; the 5-node Docker run is the user demo — note if the daemon is unavailable.)

- [ ] **Step 5: Commit**

```bash
git add docker-compose.yml RUN.md
git commit -m "build(m7): default QUORUM consistency in the demo + run docs"
```
