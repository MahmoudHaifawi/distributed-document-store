# M6 — Consistent Hashing + Virtual Nodes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Partition schemas across the cluster with a consistent-hash ring + virtual nodes — each schema lives on a preference list of N replicas — replacing round-robin affinity + full replication.

**Architecture:** A pure `HashRing` maps a schema key to an ordered preference list of N physical nodes via virtual nodes. Every data node builds the same ring from its membership (`Network.nodes ∪ self`). `RingRouter` turns the ring into routing decisions (am I the primary / a replica; who are the other replicas; who to forward to). Writes replicate to the schema's replicas only (primary coordinates); reads/index-queries serve on a replica or forward to one; DB/user metadata stays fully broadcast.

**Tech Stack:** Java 17, Spring Boot 2.7.6, the M5 one-jar role-based module, JUnit 5 + AssertJ, JDK `com.sun.net.httpserver.HttpServer` for stub-peer tests (mirrors M4b).

**Spec:** `docs/superpowers/specs/2026-10-16-m6-consistent-hashing-vnodes-design.md`

## Global Constraints

- Build with `JAVA_HOME=C:\Program Files\Java\jdk-17`; run the reactor from the repo root (`mvn -q -B -ntp ...`).
- Shard granularity is **by schema**; the schema key is the string `"<database>.<schema>"`.
- Replication factor default **3** (`REPLICATION_FACTOR` env); virtual nodes per node default **150** (`VNODES_PER_NODE` env); RF is clamped to the physical node count.
- Membership reaches a node via `Network.nodes` (peers) + `Network.self`, wired by the coordinator at start-up; the ring is `Network.nodes ∪ {self}`.
- Cluster-wide metadata (database create, user add) stays fully broadcast; only schema data + indexes are sharded.
- No HTTP/JSON wire-protocol change except added internal forwarding; peer calls use the service JWT.
- Out of scope: quorum/read-repair/hinted-handoff (M7), dynamic membership/rebalancing (M9), shard-by-document.
- Author every commit as Mahmoud Haifawi; no attribution trailers. Work on `master`; remote `origin` = `distributed-document-store`.

## Review Focus

- **Cluster smaller than RF** (single node, or 3 nodes with RF=3): preference list must clamp to the physical node count and terminate — never loop forever or return duplicates. → Task 1 tests.
- **Ring wrap-around**: a key hashing past the last vnode must wrap to the first; a key landing exactly on a vnode boundary resolves deterministically. → Task 1 tests.
- **A write/read/query arriving at a non-replica** of the schema: must forward, never apply/serve locally (else silent data loss or a wrong/empty answer). → Task 3 & 4 stub-forward tests.
- **update/delete/schema-create arriving at a non-primary**: must forward to the primary (today only document-create forwards; the others assume local data). → Task 3 tests.
- **A replica unreachable during write fan-out**: the local write still succeeds and the failure is surfaced per-replica, not turned into a total failure. → Task 3 test (fan-out uses `Broadcaster`, which already isolates per-task failures).

---

## Task 1: `HashRing` + `RingConfig` (pure)

**Files:**
- Create: `Node/src/main/java/com/database/atypon/Node/utils/ring/HashRing.java`
- Create: `Node/src/main/java/com/database/atypon/Node/utils/ring/RingConfig.java`
- Test: `Node/src/test/java/com/database/atypon/Node/utils/ring/HashRingTest.java`

**Interfaces:**
- Produces:
  - `HashRing(List<Node> nodes, int vnodesPerNode)` — builds a ring placing `vnodesPerNode` virtual nodes per physical node.
  - `List<Node> HashRing.preferenceList(String key, int replicationFactor)` — distinct physical nodes walking clockwise from `hash(key)`, size `min(replicationFactor, physicalNodeCount())`.
  - `Node HashRing.primary(String key)` — `preferenceList(key, 1).get(0)`.
  - `int HashRing.physicalNodeCount()`.
  - `static long HashRing.hash(String s)` — first 8 bytes of the MD5 of `s`, big-endian, as a long (stable across JVMs).
  - `static int RingConfig.replicationFactor()` — `REPLICATION_FACTOR` env or 3.
  - `static int RingConfig.vnodesPerNode()` — `VNODES_PER_NODE` env or 150.
- Consumes: `com.database.atypon.Node.model.Node` (`getName()`, `getPort()`).

- [ ] **Step 1: Write the failing tests** `HashRingTest` (AssertJ). Build nodes with `new Node("Node"+i, "8080")`.

```java
@Test void preferenceListHasRequestedSizeOfDistinctNodes() {
    HashRing ring = new HashRing(nodes(5), 150);
    List<Node> pref = ring.preferenceList("shop.products", 3);
    assertThat(pref).hasSize(3);
    assertThat(pref.stream().map(Node::getName).distinct().count()).isEqualTo(3);
}
@Test void replicationFactorClampsToClusterSize() {           // Review Focus: cluster < RF
    assertThat(new HashRing(nodes(2), 150).preferenceList("k", 3)).hasSize(2);
    assertThat(new HashRing(nodes(1), 150).preferenceList("k", 3)).hasSize(1);
}
@Test void placementIsDeterministicRegardlessOfNodeInputOrder() {
    List<Node> a = List.of(n("A"), n("B"), n("C")), b = List.of(n("C"), n("A"), n("B"));
    assertThat(names(new HashRing(a,150).preferenceList("shop.products",2)))
        .isEqualTo(names(new HashRing(b,150).preferenceList("shop.products",2)));
}
@Test void keyPastLastVnodeWrapsToRingStart() { /* assert a large-hash key still returns a node, no exception */ }
@Test void addingANodeMovesAboutOneNthOfKeys() {              // the consistent-hashing property
    HashRing before = new HashRing(nodes(4), 150);
    HashRing after  = new HashRing(nodes(5), 150);
    int moved = 0, keys = 5000;
    for (int i=0;i<keys;i++){ String k="s"+i;
        if(!before.primary(k).getName().equals(after.primary(k).getName())) moved++; }
    assertThat((double) moved/keys).isLessThan(2.0/5);        // ~1/5 expected, well under naive-modulo ~4/5
}
@Test void configDefaultsWhenEnvUnset() {
    assertThat(RingConfig.replicationFactor()).isEqualTo(3);
    assertThat(RingConfig.vnodesPerNode()).isEqualTo(150);
}
```
(Helpers `nodes(n)`, `n(name)`, `names(list)` are local to the test.)

- [ ] **Step 2: Run to verify they fail**

Run: `JAVA_HOME="C:\Program Files\Java\jdk-17" mvn -q -B -ntp -pl Node test -Dtest=HashRingTest`
Expected: FAIL to compile (`HashRing`/`RingConfig` missing).

- [ ] **Step 3: Implement `RingConfig`** (two static env reads with the defaults above) and **`HashRing`**: a `TreeMap<Long, Node>` of `hash(name+"#"+v)` → node for each vnode; `preferenceList` does `tailMap(hash(key)).values()` then the ring's head, collecting distinct nodes until `min(rf, count)`. `hash` via `MessageDigest.getInstance("MD5")`.

- [ ] **Step 4: Run to verify they pass**

Run: `JAVA_HOME="C:\Program Files\Java\jdk-17" mvn -q -B -ntp -pl Node test -Dtest=HashRingTest`
Expected: PASS (6 tests).

- [ ] **Step 5: Commit**

```bash
git add Node/src/main/java/com/database/atypon/Node/utils/ring/ Node/src/test/java/com/database/atypon/Node/utils/ring/
git commit -m "feat(m6): consistent-hash ring with virtual nodes + preference list"
```

---

## Task 2: Membership → ring wiring + `RingRouter`; `checkAffinity` via ring

**Files:**
- Modify: `Node/src/main/java/com/database/atypon/Node/model/Network.java` (hold + rebuild the ring)
- Create: `Node/src/main/java/com/database/atypon/Node/utils/ring/RingRouter.java`
- Modify: `Node/src/main/java/com/database/atypon/Node/utils/AffinityLoadBalancer.java` (`checkAffinity` uses the ring)
- Test: `Node/src/test/java/com/database/atypon/Node/utils/ring/RingRouterTest.java`

**Interfaces:**
- Consumes: Task 1 `HashRing`, `RingConfig`.
- Produces (all static, key = `database + "." + schema`, `rf = min(RingConfig.replicationFactor(), ring.physicalNodeCount())`):
  - `List<Node> RingRouter.replicasFor(String database, String schema)`
  - `Node RingRouter.primaryFor(String database, String schema)`
  - `boolean RingRouter.isPrimary(String database, String schema)` — self is the primary; **true when `Network.nodes` is empty** (single node owns all).
  - `boolean RingRouter.isReplica(String database, String schema)` — self is in the preference list; **true when `Network.nodes` is empty**.
  - `List<Node> RingRouter.otherReplicas(String database, String schema)` — preference list minus self (by name); **empty when `Network.nodes` is empty**.
  - `Network.ring` (`static volatile HashRing`) + `static void Network.rebuildRing()` (builds from `nodes ∪ self` with `RingConfig.vnodesPerNode()`), called at the end of `setNodes` and `assignSelf`.

- [ ] **Step 1: Write the failing tests** `RingRouterTest`. Set static membership directly, e.g. `Network.nodes = new ArrayList<>(List.of(n("Node1"),n("Node2"),n("Node3"),n("Node4"))); Network.self = n("Node0"); Network.rebuildRing();`

```java
@Test void singleNodeOwnsEverything() {
    Network.nodes = Collections.synchronizedList(new ArrayList<>()); Network.self = n("Node0"); Network.rebuildRing();
    assertThat(RingRouter.isPrimary("shop","products")).isTrue();
    assertThat(RingRouter.isReplica("shop","products")).isTrue();
    assertThat(RingRouter.otherReplicas("shop","products")).isEmpty();
}
@Test void replicasMatchRingPreferenceListAndExcludeSelfInOtherReplicas() {
    // with 5 nodes RF=3: replicasFor has 3; otherReplicas = those minus self
    List<Node> replicas = RingRouter.replicasFor("shop","products");
    assertThat(replicas).hasSize(3);
    assertThat(RingRouter.otherReplicas("shop","products"))
        .allSatisfy(nd -> assertThat(nd.getName()).isNotEqualTo(Network.self.getName()));
}
@Test void isReplicaTrueIffSelfInPreferenceList() { /* pick a schema whose replicas include self and one whose don't; assert */ }
```

- [ ] **Step 2: Run to verify they fail**

Run: `... -pl Node test -Dtest=RingRouterTest` → FAIL (`RingRouter` / `Network.ring` missing).

- [ ] **Step 3: Implement** `Network.ring` + `rebuildRing()` (call it in `setNodes` and `assignSelf`), `RingRouter` (the five methods above, with the empty-`nodes` short-circuits), and rewrite `AffinityLoadBalancer.checkAffinity(db, schema)` to: single-node short-circuit (keep) → else `RingRouter.isPrimary` ? `SUCCESS` : `ERROR` with `RingRouter.primaryFor(...)` as content. Leave `assignAffinity` in place for now — Task 3 removes it together with its last caller (`WriteService.createSchema`), so deleting it here would break the build.

- [ ] **Step 4: Run to verify they pass**

Run: `... -pl Node test -Dtest=RingRouterTest` → PASS. Then the whole module: `... -pl Node test` → all green (checkAffinity's single-node path keeps the existing controller/integration tests passing).

- [ ] **Step 5: Commit**

```bash
git add Node/src/main/java/com/database/atypon/Node/model/Network.java Node/src/main/java/com/database/atypon/Node/utils/
git commit -m "feat(m6): build the ring from membership; RingRouter routing decisions; checkAffinity via ring"
```

---

## Task 3: Writes replicate to replicas; schema/update/delete route to primary; drop affinity file

**Files:**
- Modify: `Node/src/main/java/com/database/atypon/Node/services/write/WriteService.java` (fan-out to replicas; drop affinity write)
- Create: `Node/src/main/java/com/database/atypon/Node/utils/Forwarder.java` (shared forward helper)
- Modify: `Node/src/main/java/com/database/atypon/Node/controllers/write/WriteController.java` (route schema/update/delete to primary)
- Test: `Node/src/test/java/com/database/atypon/Node/services/write/WriteFanoutTest.java`
- Test: `Node/src/test/java/com/database/atypon/Node/controllers/write/WriteForwardIntegrationTest.java`

**Interfaces:**
- Consumes: Task 2 `RingRouter`.
- Produces: `static Vector<Response> Forwarder.forwardForResponses(Node target, String pathWithQuery, Object body)` — POSTs `body` (nullable) to `target.getURL()+pathWithQuery` with `Authorization: Bearer <SecurityUtils.currentToken()>`, parses the node's `Vector<Response>` (generalizes the current `WriteController.forwardRequest`).

- [ ] **Step 1: Write the fan-out unit test** `WriteFanoutTest`: set `Network` to 5 nodes + self, `rebuildRing()`, and assert the peers a schema write fans out to equal `RingRouter.otherReplicas(db,schema)`.

```java
@Test void documentBroadcastTargetsOtherReplicasNotAllPeers() {
    // arrange 5-node membership; pick a schema; expected = RingRouter.otherReplicas(db, schema)
    List<String> fannedOut = namesOf(new WriteService(noopWriteOp()).replicaTargets("shop","products"));
    assertThat(fannedOut).isEqualTo(namesOf(RingRouter.otherReplicas("shop","products")));
}
```
(Expose a package-visible `List<Node> WriteService.replicaTargets(db, schema)` = `RingRouter.otherReplicas(db, schema)`, used by all four broadcast methods, so the fan-out set is unit-testable without HTTP.)

- [ ] **Step 2: Run to verify it fails** → `... -pl Node test -Dtest=WriteFanoutTest` → FAIL (`replicaTargets` missing / still iterates `Network.nodes`).

- [ ] **Step 3: Implement**: in `WriteService`, add `replicaTargets(db,schema)` returning `RingRouter.otherReplicas(db,schema)`, and change `broadcastSchema/Document/Update/Delete` to iterate it instead of `Network.nodes`; delete `writeNodeAffinity` and the `assignAffinity`/affinity-write in `createSchema` (now just `writeOperation.createSchema`). Add `Forwarder.forwardForResponses`. In `WriteController`, add a routing gate at the top of the **origin** path of `createSchema`, `updateDocument`, `deleteDocument`: `if (!RingRouter.isPrimary(database, schema)) return forward via Forwarder to RingRouter.primaryFor(...)` with that endpoint's path+query+body; `createDocument` keeps its existing `checkAffinity` forward (now ring-backed).

- [ ] **Step 4: Run to verify it passes** → `... -pl Node test -Dtest=WriteFanoutTest` → PASS.

- [ ] **Step 5: Write the forward integration test** `WriteForwardIntegrationTest` (`@SpringBootTest(RANDOM_PORT)`, `@ActiveProfiles` none needed — it is the Node module; `@TestPropertySource(app.jwt.secret=...)`). Start an `HttpServer` stub as the peer; set `Network.self` and `Network.nodes=[stubNode]`, `rebuildRing()`; find a schema whose `RingRouter.primaryFor` is the stub (scan `"s"+i`); POST a document-create with a USER JWT; assert the stub recorded the forwarded `POST /write/document/new?...` (proving a non-primary forwards, not applies locally).

```java
@Test void nonPrimaryForwardsWriteToPrimary() {
    String schema = schemaWhosePrimaryIsStub();   // scans until RingRouter.primaryFor("db",s) == stub
    rest.postForObject(base()+"/write/document/new?database=db&schema="+schema, authed(docJson), String.class);
    assertThat(stub.lastPath()).isEqualTo("/write/document/new?database=db&schema="+schema);
}
```

- [ ] **Step 6: Run to verify it passes**, then the whole module → `... -pl Node test` green.

- [ ] **Step 7: Commit**

```bash
git add Node/src/main/java/com/database/atypon/Node/ Node/src/test/java/com/database/atypon/Node/
git commit -m "feat(m6): replicate writes to the schema's replicas; route schema/update/delete to the primary"
```

---

## Task 4: Reads + index queries forward to a replica; index build/drop to replicas

**Files:**
- Modify: `Node/src/main/java/com/database/atypon/Node/controllers/read/ReadController.java`
- Modify: `Node/src/main/java/com/database/atypon/Node/controllers/index/IndexController.java`
- Test: `Node/src/test/java/com/database/atypon/Node/controllers/read/ReadForwardIntegrationTest.java`
- Test: `Node/src/test/java/com/database/atypon/Node/controllers/index/IndexFanoutTest.java`

**Interfaces:**
- Consumes: Task 2 `RingRouter`, Task 3 `Forwarder` (add `static Response Forwarder.forwardForResponse(Node target, String pathWithQuery, Object body, boolean post)` for single-`Response` read/query endpoints).

- [ ] **Step 1: Write the index fan-out unit test** `IndexFanoutTest`: 5-node membership; assert the index create/drop fan-out target set equals `RingRouter.otherReplicas(db,schema)` (expose `List<Node> IndexController.replicaTargets(db,schema)` = `RingRouter.otherReplicas`).

- [ ] **Step 2: Run to verify it fails** → FAIL (`replicaTargets` missing / iterates `Network.nodes`).

- [ ] **Step 3: Implement**:
  - `ReadController.fetchById`/`fetchAll`: at the top, `if (!RingRouter.isReplica(databaseName, schemaName)) return Forwarder.forwardForResponse(RingRouter.primaryFor(...), <same path+query>, null, false);` else serve locally.
  - `IndexController.query`: same read-forward using the body's `database`/`schema`.
  - `IndexController.createIndex`/`dropIndex`: origin (admin) path — `if (!RingRouter.isPrimary(db,schema)) return Forwarder.forwardForResponses(primaryFor, <path+query>, null);` else run locally + fan out to `replicaTargets(db,schema)` (not `Network.nodes`); internal path unchanged (local only).

- [ ] **Step 4: Run to verify it passes** → `... -pl Node test -Dtest=IndexFanoutTest` → PASS.

- [ ] **Step 5: Write the read-forward integration test** `ReadForwardIntegrationTest` (same stub pattern as Task 3): a node that is **not** a replica of a crafted schema must forward `GET /user/read/document?...` to a replica; assert the stub recorded it.

- [ ] **Step 6: Run to verify it passes**, then whole module → `... -pl Node test` green.

- [ ] **Step 7: Commit**

```bash
git add Node/src/main/java/com/database/atypon/Node/controllers/ Node/src/test/java/com/database/atypon/Node/controllers/
git commit -m "feat(m6): route reads/index-queries to a replica; build indexes on the schema's replicas"
```

---

## Task 5: Config wiring, 5-node demo, validation, docs

**Files:**
- Modify: `docker-compose.yml` (5 data nodes, `REPLICATION_FACTOR`)
- Modify: `RUN.md` (replication factor + sharding note)
- Modify: `docs/superpowers/specs/...` only if a decision changed (else none)

**Interfaces:** none new.

- [ ] **Step 1: Full reactor build + test**

Run: `JAVA_HOME="C:\Program Files\Java\jdk-17" mvn -q -B -ntp verify`
Expected: all modules green (Node incl. the new ring/router/forward tests; others unchanged).

- [ ] **Step 2: Bump the demo cluster** in `docker-compose.yml` to **five** `node`-role services (`Node0`..`Node4`, all `SPRING_PROFILES_ACTIVE=node`), set `CLUSTER_NODES: "Node0:8080,Node1:8080,Node2:8080,Node3:8080,Node4:8080"` and `REPLICATION_FACTOR: "3"` on the coordinator and each node (ring + RF must match cluster-wide), `depends_on` updated. Validate: `docker compose config` → no errors.

- [ ] **Step 3: Update `RUN.md`** — add that schemas are sharded by a hash ring onto `REPLICATION_FACTOR` (default 3) replicas, that the compose demo runs 5 nodes so each schema lives on 3 of 5, and how to see it (create several schemas; each is served from its replicas).

- [ ] **Step 4: Live validation (best-effort).** If the Docker daemon is up: `docker compose up --build -d`, wait, then create a database + several schemas via the gateway, and confirm reads/queries succeed (the entry node forwards to replicas). If Docker is unavailable, record that multi-node behavior is covered by the ring unit tests + the two stub-forward integration tests + `docker compose config`, and that the 5-node run is the user-facing demo. Either way, confirm the **single-node** local run still works: `java -jar app/target/nosql-db.jar --role=node` → `/health` UP, gateway login → dashboard (RF clamps to 1).

- [ ] **Step 5: Commit + push**

```bash
git add docker-compose.yml RUN.md
git commit -m "build(m6): 5-node sharding demo (RF=3) + run docs"
git push origin master
```
