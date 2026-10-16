# M6 — Consistent hashing + virtual nodes (schema-level sharding)

Status: design — pending review before implementation planning.
Milestone: M6 of the capstone roadmap (first of the "standout" distributed-systems features).
Workflow: work directly on `master`; the canonical remote is `distributed-document-store`.

## 1. Intent / why

Today the cluster **full-replicates everything**: every data node holds a copy of every document, and a
schema's single "owner" (round-robin `assignAffinity` = schemaCount % nodeCount, persisted in an affinity
file) coordinates writes and broadcasts them to **all** peers. That is not how a real distributed store
behaves, and it is the one thing that most undercuts this project in a distributed-systems interview.

M6 replaces that with **consistent hashing + virtual nodes**: a hash ring deterministically places each
**schema** (collection) on a **preference list of N nodes** (the replication factor), with virtual nodes
for even load distribution. Data is genuinely partitioned — with a cluster larger than N, a given schema
lives on only N of the nodes, not all — and placement is computed identically on every node from the
shared membership, so there is no central placement authority and no stored affinity file. This is the
core mechanism behind Dynamo/Cassandra/Riak, and being able to walk an interviewer through "a hash ring
with virtual nodes places each collection on its N replicas, and adding a node moves only ~1/n of the
collections" is the standout outcome.

**Sharding granularity: by schema (collection), decided.** Each whole schema — its documents *and* its
B+-tree indexes — lives on its N replicas. A query still reaches one node that holds the complete index,
so the entire M1 indexing engine is reused unchanged and there is no distributed-query/scatter-gather
work. Sharding *by document* (rows of one schema spread across the cluster, forcing scatter-gather reads
and partial per-node indexes) is the more extreme Dynamo model; it is a **documented future extension**,
not M6.

## 2. Goals / non-goals / anti-goals

### Goals
- A `HashRing` that maps schema keys to an ordered **preference list** of N physical nodes using virtual
  nodes, computed deterministically from the current membership on every node (same ring everywhere).
- Writes (schema create, document create/update/delete) replicate to **only the schema's N replicas**,
  not all peers; the **primary** (first node on the preference list) is the single coordinator per schema,
  preserving the M2 version/ordering semantics.
- Reads and index queries are served by a replica of the target schema; a node that is not a replica
  forwards to one.
- Index build/drop runs on exactly the schema's replicas.
- Replication factor and virtual-node count are configurable; the design degrades cleanly to the existing
  behaviour when N ≥ cluster size (every node is a replica) and to a single node locally.
- A way to **demonstrate the minimal-movement property** (adding a node remaps only ~1/n of schemas),
  since static membership means it is otherwise invisible.
- The existing test suite stays green; new tests cover the ring and the routing.

### Non-goals (this milestone)
- **Dynamic membership** (nodes joining/leaving at runtime, live rebalancing/data migration) — that is M9
  (gossip/SWIM). M6's ring is built from the static `CLUSTER_NODES` membership at start-up, exactly as the
  mesh is wired today.
- **Quorum reads/writes, read-repair, hinted handoff** — that is M7. M6 writes synchronously to the
  available replicas and reads from one; failure handling is best-effort (see §5).
- Sharding by document / distributed query execution (future extension).
- Re-partitioning or migrating data that already exists under the old affinity scheme (demo data is
  recreated; no migration tool).

### Anti-goals (would make this a failure even if it "works")
- Each node having to know a *different* ring, or a central service being consulted on every request —
  placement must be local and deterministic.
- A design that only ever behaves like full replication (e.g. demo cluster size ≤ N), so sharding is never
  actually exercised or visible.
- Breaking the single-node local run or the per-schema B+-tree index/query path.

## 3. Constraints

- Java 17 / Spring Boot 2.7.6; the M5 one-jar, role-based structure; build with `JAVA_HOME` = JDK 17.
- Membership reaches a data node exactly as today: the coordinator wires `Network.nodes` (peers) and
  `Network.self` at start-up via `/network/add/nodes` and `/network/assign/self`. The ring is built from
  `Network.nodes ∪ {self}`.
- No change to the HTTP/JSON wire protocol between gateway and nodes except where routing requires it;
  internal peer calls keep using the service JWT.
- Author every commit as Mahmoud Haifawi; no attribution trailers.

## 4. The design (decided "how")

### 4.1 The ring
A new `HashRing` (data-node util) built from a list of physical nodes:
- Each physical node contributes **V virtual nodes** (`VNODES_PER_NODE`, env-configurable, default 150).
  Each vnode's ring position = a stable 64-bit hash of `"<nodeName>#<i>"` (MD5 of the string, first 8
  bytes as a long — stable across JVMs and well-distributed, unlike `String.hashCode`).
- The ring is a sorted map (ring position → physical node). For a key, hash `"<database>.<schema>"` the
  same way, then walk clockwise (tailMap, wrapping) collecting **distinct physical nodes** until N are
  found — that ordered list is the **preference list**; element 0 is the **primary**.
- `N` = `REPLICATION_FACTOR` (env, default 3), clamped to the number of physical nodes (so a 3-node
  cluster with default N behaves like full replication; a 5-node cluster shards 3-of-5).
- The ring is rebuilt whenever membership is (re)assigned (`/network/*`), from the same inputs on every
  node, so all nodes agree.

### 4.2 Placement replaces affinity
`HashRing.primaryFor(db, schema)` and `HashRing.replicasFor(db, schema)` replace
`AffinityLoadBalancer.assignAffinity`/`checkAffinity` and the per-schema **affinity file is removed** —
placement is now computed, not stored. `checkAffinity`'s single-node short-circuit is preserved (a lone
node is the primary and sole replica of everything).

### 4.3 Write path (schema create, document create/update/delete)
The data node that receives the request (the user's pinned entry node) is the router:
1. Compute `replicasFor(db, schema)` and `primary = primaryFor(db, schema)`.
2. If `self == primary`: apply locally (unchanged `WriteOperation`, keeping version checks), then
   **replicate to the other replicas only** (preference list minus self) via the internal endpoints — i.e.
   `WriteService`'s four fan-out loops iterate the **replica list** instead of `Network.nodes`.
3. If `self != primary`: forward the original request to the primary (reusing the existing
   affinity-forward mechanism — `WriteController.forwardRequest` with the caller's Bearer token), which
   then runs step 2.

This keeps a single coordinator per schema (primary) so M2/M3 optimistic-version semantics are unchanged;
only the fan-out set shrinks from "all peers" to "the N−1 other replicas".

### 4.4 Read path (read-by-id, read-all, index query)
The entry node computes `replicasFor(db, schema)`:
- If `self` is a replica → serve locally (unchanged read / index-query code).
- Else → forward the read to a replica (prefer the primary; fall back to the next replica) over the
  internal endpoint, return its response. This adds a read-forward analogous to the existing write-forward
  (today reads never forward because every node has every schema).

### 4.5 Index build/drop
`IndexController`'s create/drop fan-out iterates the schema's **replica list** instead of `Network.nodes`,
so exactly the nodes that hold the schema's data build/drop its B+-tree. Index query follows the read path
(§4.4).

### 4.6 What stays fully replicated
Cluster-wide **metadata** is not sharded: **database creation** and **user add** still broadcast to all
nodes (`AdminService` unchanged), because every node needs the database directory and the user registry
regardless of where schemas land.

### 4.7 Configuration & demo topology
- `REPLICATION_FACTOR` (default 3) and `VNODES_PER_NODE` (default 150), read once at start-up (same
  pattern as `CLUSTER_NODES`). Shared by all roles via the M5 profile config.
- **docker-compose demo bumped to 5 data nodes with RF=3**, so sharding is visible (each schema on 3 of
  5). Single-node local and the 3-node setup keep working (RF clamps).

### 4.8 Demonstrating minimal movement (static membership)
Because membership is static, add a small read-only **ring tool** to make the consistent-hashing property
tangible:
- A unit test that builds a ring of K nodes, assigns many schema keys, adds a (K+1)-th node, and asserts
  only ~1/(K+1) of keys change primary (the defining property of consistent hashing vs. naive modulo).
- Optional (nice-to-have, may defer): a gateway dashboard addition showing, per schema, its preference
  list ("shop.products → Node1, Node3, Node4"), built on the M4c topology view — strong live demo of "this
  collection lives on these nodes". Marked optional so it never blocks the core.

## 5. Failure handling in M6 (deliberately minimal)
Writes replicate synchronously to the available replicas (reusing `Broadcaster`, which already awaits all
tasks); if a replica is unreachable, its task returns an error that is logged/surfaced but does not roll
back the others (best-effort replication). Reads try the primary then fall back to another replica. True
quorum (W+R>N), read-repair, and hinted handoff are **M7** and explicitly out of scope here — M6's job is
placement and partitioned replication, not consensus.

## 6. What's left to the builder
- Exact hash implementation (MD5→long is specified; the builder writes it) and the `VNODES_PER_NODE`
  default's load-balance check via the movement test.
- Precise reuse of the existing forward mechanism for the new read-forward.
- Whether the optional dashboard placement view is included this milestone or deferred (default: defer if
  it adds risk; the ring tool/test is the required demonstration).
- Test design for the routing (where to assert primary/replica selection and forwarding).

The implementation plan (superpowers:writing-plans) follows approval of this document.
