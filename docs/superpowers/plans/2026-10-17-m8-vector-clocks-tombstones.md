# M8 — Vector Clocks + Tombstones Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give every document a vector clock so reconciliation can tell causal updates from true conflicts, make deletes tombstones that propagate, and heal replicas causally — closing M7's delete-resurrection gap.

**Architecture:** A pure `VectorClock` (node→counter) + `Reconcile.winner(a,b)` (descends → newer; concurrent → latest `_ts`, canonical-clock tiebreak, merged clock); tombstone-aware. The coordinating primary stamps `_vclock`/`_ts` on create/update and writes a tombstone on delete. A single causal `applyReplicated` (reconcile-and-store) backs replication AND read-repair; the quorum read reconciles all copies (including tombstones) and returns not-found when the winner is a tombstone.

**Tech Stack:** Java 17, Spring Boot 2.7.6, the M5 one-jar module, org.json, JUnit 5 + AssertJ, JDK `com.sun.net.httpserver.HttpServer` stub peers (M4b/M6/M7).

**Spec:** `docs/superpowers/specs/2026-10-17-m8-vector-clocks-tombstones-design.md`

## Global Constraints

- Build with `JAVA_HOME=C:\Program Files\Java\jdk-17`; run the reactor from the repo root.
- Reserved document keys (ignored by schema validation): `_version`, `_vclock`, `_ts`, `_deleted`. A **tombstone** (`_deleted:true`, no schema fields) bypasses `Validators.validateDocument`.
- The coordinating node's identity for a clock entry is its node name: `Network.getSelf()!=null ? getSelf().getName() : "localhost"`.
- Conflict resolution is deterministic auto-resolve (NOT siblings): on CONCURRENT, winner = higher `_ts`; tie → the lexicographically greater canonical `_vclock` string; the winner's clock is the pointwise `merge` of the two.
- Deletes are tombstones (the file is overwritten with the marker, never removed); reads return not-found for a tombstone; tombstones propagate via replication + read-repair.
- `_version` (the gateway's optimistic lock) is preserved; `_vclock` is additional.
- Builds on M6/M7: `RingRouter`, `Forwarder`, `ServiceTokens`, `QuorumReadService`, `WriteOperation`, `WriteController`, the INTERNAL-only `/write/document/repair` (handler + mvcMatchers guard — keep both).
- Out of scope: sibling resolution, tombstone GC, vclock pruning, dynamic membership (M9), hinted handoff/anti-entropy.
- Author every commit as Mahmoud Haifawi; no attribution trailers. Work on `master`; remote `origin` = `distributed-document-store`.

## Review Focus

- **Single node (one clock entry):** create/update/delete stamp a 1-entry clock; a read-by-id of a tombstone returns not-found; no gather/repair — behavior otherwise as M6/M7. → Task 1 + Task 4 tests.
- **Concurrent conflict (neither clock descends):** resolved deterministically (higher `_ts`, then canonical-clock tiebreak) with a merged clock, so re-reading does not re-conflict. → Task 2 tests.
- **A replica that missed a DELETE (stale live copy):** read-repair must send it the tombstone winner so it stops returning the document (no resurrection). → Task 4 integration test.
- **A replica genuinely absent (missed a create):** read-repair heals it from a live winner — safe now because a deleted doc is a tombstone, not an absent file. → Task 4 integration test (this REPLACES M7's "absent is never written" assertion).
- **Tombstone vs live reconciliation / the winner is a tombstone:** the quorum read returns not-found; a live copy that the tombstone does NOT dominate (concurrent, live wins) stays live. → Task 2 + Task 4 tests.

---

## Task 1: `VectorClock` + reserved keys

**Files:**
- Create: `Node/src/main/java/com/database/atypon/Node/utils/vclock/VectorClock.java`
- Modify: `Node/src/main/java/com/database/atypon/Node/utils/JsonKeys.java` (add VCLOCK/TS/DELETED)
- Test: `Node/src/test/java/com/database/atypon/Node/utils/vclock/VectorClockTest.java`

**Interfaces:**
- Produces:
  - `VectorClock(java.util.Map<String,Integer>)` and `VectorClock()` (empty).
  - `static VectorClock VectorClock.fromJson(org.json.JSONObject vclockOrNull)` (null/absent → empty).
  - `org.json.JSONObject VectorClock.toJson()`.
  - `VectorClock VectorClock.increment(String node)` — returns a NEW clock with `node` one higher.
  - `VectorClock VectorClock.merge(VectorClock other)` — NEW clock, pointwise max.
  - `enum VectorClock.Ordering { EQUAL, DESCENDS, DESCENDED_BY, CONCURRENT }`.
  - `Ordering VectorClock.compare(VectorClock other)` — `this` vs `other` (DESCENDS = this strictly dominates; CONCURRENT = neither dominates).
  - `String VectorClock.canonical()` — deterministic string of entries sorted by node (for the concurrent tiebreak).
  - `JsonKeys.VCLOCK="_vclock"`, `JsonKeys.TS="_ts"`, `JsonKeys.DELETED="_deleted"`.

- [ ] **Step 1: Write the failing tests** `VectorClockTest`:

```java
@Test void incrementRaisesOneEntry() {
    VectorClock c = new VectorClock().increment("A").increment("A").increment("B");
    assertThat(c.toJson().getInt("A")).isEqualTo(2);
    assertThat(c.toJson().getInt("B")).isEqualTo(1);
}
@Test void mergeIsPointwiseMax() {
    VectorClock a = VectorClock.fromJson(new JSONObject("{\"A\":2,\"B\":1}"));
    VectorClock b = VectorClock.fromJson(new JSONObject("{\"A\":1,\"B\":3,\"C\":1}"));
    JSONObject m = a.merge(b).toJson();
    assertThat(m.getInt("A")).isEqualTo(2); assertThat(m.getInt("B")).isEqualTo(3); assertThat(m.getInt("C")).isEqualTo(1);
}
@Test void compareClassifiesOrdering() {
    VectorClock a1 = VectorClock.fromJson(new JSONObject("{\"A\":1}"));
    VectorClock a2 = VectorClock.fromJson(new JSONObject("{\"A\":2}"));
    assertThat(a2.compare(a1)).isEqualTo(VectorClock.Ordering.DESCENDS);
    assertThat(a1.compare(a2)).isEqualTo(VectorClock.Ordering.DESCENDED_BY);
    assertThat(a1.compare(a1)).isEqualTo(VectorClock.Ordering.EQUAL);
    VectorClock x = VectorClock.fromJson(new JSONObject("{\"A\":1,\"B\":1}"));
    VectorClock y = VectorClock.fromJson(new JSONObject("{\"A\":2}"));   // A higher, B lower -> neither dominates
    assertThat(x.compare(y)).isEqualTo(VectorClock.Ordering.CONCURRENT);
}
@Test void fromJsonToJsonRoundTrips() { /* empty + non-empty round-trip equal maps */ }
@Test void canonicalIsSortedAndDeterministic() { /* same entries in any insertion order -> same canonical() */ }
```

- [ ] **Step 2: Run to verify fail** — `... -pl Node test -Dtest=VectorClockTest` → FAIL (classes/keys missing).
- [ ] **Step 3: Implement** `VectorClock` (a `TreeMap<String,Integer>` internally for canonical ordering; `compare` by scanning the union of keys: track whether `this` has any entry greater and `other` has any entry greater → both → CONCURRENT, only this → DESCENDS, only other → DESCENDED_BY, neither → EQUAL) and the three `JsonKeys`.
- [ ] **Step 4: Run to verify pass** — PASS (5 tests).
- [ ] **Step 5: Commit**

```bash
git add Node/src/main/java/com/database/atypon/Node/utils/vclock/ Node/src/main/java/com/database/atypon/Node/utils/JsonKeys.java Node/src/test/java/com/database/atypon/Node/utils/vclock/
git commit -m "feat(m8): vector clock (increment/merge/compare) + reserved doc keys"
```

---

## Task 2: `Reconcile` (pure, tombstone-aware)

**Files:**
- Create: `Node/src/main/java/com/database/atypon/Node/utils/vclock/Reconcile.java`
- Test: `Node/src/test/java/com/database/atypon/Node/utils/vclock/ReconcileTest.java`

**Interfaces:**
- Consumes: Task 1 `VectorClock`, `JsonKeys.{VCLOCK,TS,DELETED}`.
- Produces:
  - `static boolean Reconcile.isTombstone(JSONObject doc)` = `doc != null && doc.optBoolean(DELETED, false)`.
  - `static JSONObject Reconcile.winner(JSONObject a, JSONObject b)` — null-safe (a null → b; b null → a). Reads each clock via `VectorClock.fromJson(doc.optJSONObject(VCLOCK))`. By `a.clock.compare(b.clock)`: EQUAL/DESCENDS → a; DESCENDED_BY → b; CONCURRENT → the one with the higher `_ts` (`optLong(TS,0)`), tie → the one whose `clock.canonical()` is lexicographically greater; the returned winner carries `_vclock = a.clock.merge(b.clock).toJson()` (so it dominates both). A tombstone is an ordinary participant — no special-casing by `_deleted`.

- [ ] **Step 1: Write the failing tests** `ReconcileTest`:

```java
@Test void descendsKeepsNewer() {
    JSONObject a = doc("{\"_vclock\":{\"A\":1},\"_ts\":10,\"n\":1}");
    JSONObject b = doc("{\"_vclock\":{\"A\":2},\"_ts\":5,\"n\":2}");   // b descends a, even with lower _ts
    assertThat(Reconcile.winner(a,b).getInt("n")).isEqualTo(2);
}
@Test void concurrentResolvesByTimestampThenCanonical() {
    JSONObject a = doc("{\"_vclock\":{\"A\":1},\"_ts\":10,\"n\":1}");
    JSONObject b = doc("{\"_vclock\":{\"B\":1},\"_ts\":20,\"n\":2}");  // concurrent; b later
    JSONObject w = Reconcile.winner(a,b);
    assertThat(w.getInt("n")).isEqualTo(2);
    assertThat(w.getJSONObject("_vclock").getInt("A")).isEqualTo(1);   // merged clock
    assertThat(w.getJSONObject("_vclock").getInt("B")).isEqualTo(1);
}
@Test void tombstoneWinsOrLosesByClock() {
    JSONObject live = doc("{\"_vclock\":{\"A\":1},\"_ts\":10,\"n\":1}");
    JSONObject tomb = doc("{\"_vclock\":{\"A\":2},\"_ts\":11,\"_deleted\":true}");  // tomb descends live
    assertThat(Reconcile.isTombstone(Reconcile.winner(live, tomb))).isTrue();
}
@Test void nullOperandReturnsTheOther() {
    assertThat(Reconcile.winner(null, doc("{\"_vclock\":{\"A\":1},\"n\":1}")).getInt("n")).isEqualTo(1);
}
```
(`doc(String)` = `new JSONObject(String)`, local helper.)

- [ ] **Step 2: Run to verify fail** → FAIL.
- [ ] **Step 3: Implement** `Reconcile` per the Interfaces contract.
- [ ] **Step 4: Run to verify pass** → PASS.
- [ ] **Step 5: Commit**

```bash
git add Node/src/main/java/com/database/atypon/Node/utils/vclock/Reconcile.java Node/src/test/java/com/database/atypon/Node/utils/vclock/ReconcileTest.java
git commit -m "feat(m8): causal reconcile (descends/concurrent tiebreak, tombstone-aware)"
```

---

## Task 3: Stamp clocks, tombstone deletes, unify replication as `applyReplicated`

**Files:**
- Modify: `Node/src/main/java/com/database/atypon/Node/operations/write/WriteOperation.java`
- Modify: `Node/src/main/java/com/database/atypon/Node/controllers/write/WriteController.java` (delete replicates the tombstone)
- Modify: `Node/src/main/java/com/database/atypon/Node/utils/Validators.java` (ignore reserved keys — confirm/extend)
- Test: `Node/src/test/java/com/database/atypon/Node/operations/write/WriteOperationVClockTest.java`

**Interfaces:**
- Consumes: Task 1 `VectorClock`, Task 2 `Reconcile`, `JsonKeys.{VERSION,VCLOCK,TS,DELETED}`.
- Produces:
  - `WriteOperation` stamps on the origin path: create → `_vclock={self:1}`, `_ts=now`; update → `_vclock = fromJson(old._vclock).increment(self)`, `_ts=now`; delete → writes a tombstone `{_deleted:true, _version=old+1, _vclock=fromJson(old._vclock).increment(self), _ts=now}` (keeps the version check; bypasses `validateDocument`; fires `onDelete`) and returns the tombstone JSON string in `Response.content`.
  - `Response WriteOperation.applyReplicated(String db, String schema, String id, JSONObject incoming)` — under the lock: load local (absent→null); `winner = Reconcile.winner(local, incoming)`; if `winner` content differs from local, write it (bypass `validateDocument` when `winner` is a tombstone), and fire `onDelete` if the winner is a tombstone else `onUpdate`. `applyUpdate` and `applyDelete` delegate to it. SUCCESS always; content says whether it applied.
  - `private static String WriteOperation.selfName()` = `Network.getSelf()!=null ? Network.getSelf().getName() : "localhost"`.

- [ ] **Step 1: Write the failing test** `WriteOperationVClockTest` (uses the real `WriteOperation` bean + a temp db/schema, like the existing `WriteOperation*Test`):

```java
@Test void createStampsAOneEntryClock() { /* create -> stored doc has _vclock {self:1}, _ts>0, _version 1 */ }
@Test void updateIncrementsSelfInTheClock() { /* after update, _vclock[self]==2, _version 2 */ }
@Test void deleteLeavesATombstoneNotAnAbsentFile() {
    // delete -> the document FILE still exists and is a tombstone (_deleted:true), _version bumped, clock incremented
}
@Test void applyReplicatedKeepsTheDominatingCopy() {
    // local at {A:1}; applyReplicated an incoming at {A:2} -> stored becomes incoming; an incoming at {A:1} (dominated) -> unchanged
}
@Test void applyReplicatedTombstoneOverLiveTombstonesIt() {
    // local live {A:1}; applyReplicated a tombstone {A:2,_deleted} -> stored is a tombstone
}
```

- [ ] **Step 2: Run to verify fail** → `... -pl Node test -Dtest=WriteOperationVClockTest` → FAIL.
- [ ] **Step 3: Implement** the stamping, the tombstone delete, `applyReplicated` (and delegate `applyUpdate`/`applyDelete` to it), `selfName()`, and confirm `Validators.validateDocument` ignores the reserved keys (extend it if it rejects unknown keys). In `WriteController.deleteDocument` (origin path), after the local tombstone SUCCESS, replicate the **tombstone** to the other replicas via the update-replication path (`writeService.broadcastUpdate(db, schema, id, <tombstone map from the delete Response content>)`) instead of `broadcastDelete`, so peers `applyReplicated` the tombstone.
- [ ] **Step 4: Run to verify pass** → `-Dtest=WriteOperationVClockTest` PASS. Then `... -pl Node test`: existing write/replication/quorum tests stay green EXCEPT ones that asserted a delete removes the file or that `applyUpdate` is a blind upsert — update those minimally to the tombstone/reconcile behavior (note which in the commit). Single-node create/update/delete still work (one-entry clock).
- [ ] **Step 5: Commit**

```bash
git add Node/src/main/java/com/database/atypon/Node/ Node/src/test/java/com/database/atypon/Node/operations/write/WriteOperationVClockTest.java
git commit -m "feat(m8): stamp vector clocks, tombstone deletes, causal applyReplicated"
```

---

## Task 4: Reads skip tombstones; causal, tombstone-aware read-repair

**Files:**
- Modify: `Node/src/main/java/com/database/atypon/Node/services/read/ReadService.java` (fetchAll skips tombstones)
- Modify: `Node/src/main/java/com/database/atypon/Node/services/read/QuorumReadService.java` (reconcile via `Reconcile`; winner-tombstone → not-found; repair via causal apply)
- Modify: `Node/src/main/java/com/database/atypon/Node/operations/write/WriteOperation.java` (`repairDocument` becomes a thin delegate to `applyReplicated` — drop its own `validateDocument`/version-guard, since reconcile handles it and a tombstone winner must not be schema-validated)
- Modify: the existing `ReadRepairIntegrationTest` (its "absent is never written" assertion changes — see below)
- Modify: the existing `WriteOperationRepairTest` (M7 asserted repair skips an absent file and compares by `_version`; under M8 repair reconciles by clock — absent + live winner now HEALS, a tombstone winner tombstones a dominated live copy, a dominating/concurrent-losing winner skips; update those assertions)
- Test: `Node/src/test/java/com/database/atypon/Node/controllers/read/ReadRepairVClockTest.java` (new cases)

**Interfaces:**
- Consumes: Task 2 `Reconcile`, Task 3 `WriteOperation.applyReplicated`; M7 `QuorumReadService` gather/repair structure, `ServiceTokens.bearer()`.
- Produces: `QuorumReadService.readById` returns `ERROR "Document not found"` when the reconciled winner is a tombstone (`Reconcile.isTombstone`), else the winner document; repairs every replica whose copy is strictly dominated by the winner (or absent) by sending the winner to the INTERNAL `/write/document/repair` (now backed by `applyReplicated`). `ReadService.fetchById` stays RAW (returns a tombstone's content, so the internal gather sees it); `ReadService.fetchAll` omits tombstones for the client.

- [ ] **Step 1: Write the failing tests.**
  - In `ReadRepairVClockTest` (stub peers, default QUORUM, same pattern as `ReadRepairIntegrationTest`):
    - `clientReadOfATombstonedDocReturnsNotFound` — self holds a tombstone → `GET /user/read/document` returns ERROR "Document not found".
    - `readRepairPropagatesATombstoneToAReplicaThatMissedTheDelete` — self holds a tombstone (clock {self:2}); staleStub holds the live doc at the older clock ({self:1}); a client read returns not-found AND staleStub receives a `/write/document/repair` carrying the tombstone (so it stops returning the doc). (This is the concrete M7-gap fix.)
    - `readRepairHealsAReplicaThatMissedACreate` — self holds a live doc; absentStub returns not-found; a client read returns the doc AND absentStub receives the live winner via `/write/document/repair` (healed). 
  - Update `ReadRepairIntegrationTest`: the old `missingStub receives NO write` expectation is now the opposite (absent is healed); re-point that assertion to the new M8 behavior, and keep the stale-older-copy repair case (now classified by clock, not version).
- [ ] **Step 2: Run to verify fail** → FAIL (reconcile/tombstone wiring missing).
- [ ] **Step 3: Implement** `QuorumReadService` reconcile via `Reconcile.winner` across the gathered copies (gather already returns raw docs incl tombstones); winner-tombstone → not-found; repair the dominated/absent replicas via the repair endpoint. Point `WriteOperation.repairDocument` at `applyReplicated` (so repair is causal + tombstone-aware + heals absent). Make `ReadService.fetchAll` skip `_deleted` docs.
- [ ] **Step 4: Run to verify pass** → `-Dtest=ReadRepairVClockTest,ReadRepairIntegrationTest` PASS, then `... -pl Node test` green (single-node read-by-id of a tombstone returns not-found via the N=1 reconcile path).
- [ ] **Step 5: Commit**

```bash
git add Node/src/main/java/com/database/atypon/Node/ Node/src/test/java/com/database/atypon/Node/
git commit -m "feat(m8): tombstone-aware causal read-repair; reads hide tombstones"
```

---

## Task 5: Docs + validation

**Files:**
- Modify: `RUN.md` (vector clocks + tombstones note)

- [ ] **Step 1: Full reactor build + test** — `JAVA_HOME="C:\Program Files\Java\jdk-17" mvn -q -B -ntp verify` → all modules green (report counts).
- [ ] **Step 2: Update `RUN.md`** — a "Conflict resolution and deletes" note: each document carries a vector clock; reconciliation keeps the causally newer copy and **auto-resolves** genuinely concurrent writes deterministically (latest timestamp) rather than surfacing siblings; a delete writes a tombstone that propagates, so a replica that missed a delete is repaired to the tombstone (the delete can't be undone); tombstones are not yet garbage-collected. No new env vars.
- [ ] **Step 3: Validation** — confirm the single-node local run still works: `java -jar app/target/nosql-db.jar --role=node` from a dir with `data/info.json` → `/health` UP; a create, read-by-id, update, delete, then read-by-id returns not-found (tombstone). Record it. (Multi-node causal behavior is covered by the stub integration tests; Docker is the user demo — note if the daemon is down.)
- [ ] **Step 4: Commit + validation note** — if only RUN.md changed:

```bash
git add RUN.md
git commit -m "docs(m8): document vector-clock reconciliation + tombstones"
```
