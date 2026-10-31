# Running the cluster

The system is **one artifact** (`app/target/nosql-db.jar`) launched in a chosen **role**. Data nodes
still run as separate processes — that is what makes it a cluster — but the build and the launcher are
unified (one binary, many roles, like Cassandra or CockroachDB).

| Role          | Port  | Launch                                             |
|---------------|-------|----------------------------------------------------|
| `node`        | 8080  | data node — storage engine + B+-tree index         |
| `coordinator` | 8079  | user routing + SWIM seed + topology                 |
| `gateway`     | 8078  | Thymeleaf web UI, forwards to nodes                 |

Select the role with `--role=<node|coordinator|gateway>` (or `--spring.profiles.active=<role>`, or the
`SPRING_PROFILES_ACTIVE` env var). Launching with no role prints usage and exits.

Seeded admin account: **`mahmoud` / `admin`**.

---

## Option A — Docker (full cluster, recommended)

Requires Docker Desktop running. One image is built from source (the root `Dockerfile`) and run seven
times with different roles — no local Maven needed.

```bash
docker compose up --build
```

Then open http://localhost:8078/login and sign in. This runs **five** data nodes (`Node0`..`Node4`)
behind the coordinator and the gateway. Addressing is by compose service name: `CLUSTER_NODES` tells
the coordinator the data-node list (and seeds the nodes' membership, see
[Dynamic membership](#dynamic-membership-swim--gossip)), and `BOOTSTRAP_URL` tells the gateway where the
coordinator is.

### Sharding and replication

Schemas are **sharded by a consistent-hash ring** (virtual nodes) onto `REPLICATION_FACTOR` replicas
(default **3**). Each schema hashes to a position on the ring and lives on the next
`REPLICATION_FACTOR` distinct nodes; the first of them is its primary, which takes schema/update/delete
writes and replicates them to the others. Any node you hit as the entry point forwards a read or index
query to a replica that holds the schema, so clients never need to know where data lives.

The compose demo runs 5 nodes with `REPLICATION_FACTOR=3`, so **each schema lives on 3 of the 5
nodes** rather than on every node. To see it: sign in, create a database and several schemas, then read
and query them from the dashboard. Databases are created on every node, but each schema is served from
its own subset of three nodes, so the schema directories under `/app/data/<database>` differ from node
to node, e.g. `docker compose exec Node0 ls /app/data/<database>`.

These environment variables tune the ring and the consistency levels (all optional):

| Variable             | Default  | Meaning                                                                                                     |
|----------------------|----------|-------------------------------------------------------------------------------------------------------------|
| `REPLICATION_FACTOR` | `3`      | Number of nodes that hold a copy of each schema (clamped to cluster size)                                   |
| `VNODES_PER_NODE`    | `150`    | Virtual nodes each physical node places on the ring (smoother balance)                                      |
| `WRITE_CONSISTENCY`  | `QUORUM` | `ONE`/`QUORUM`/`ALL`: replicas that must persist a schema or document write before it is acknowledged (`W`) |
| `READ_CONSISTENCY`   | `QUORUM` | `ONE`/`QUORUM`/`ALL`: replicas a read by id expects to answer; stale ones are read-repaired (`R`)           |

Each node builds the ring from its own live membership view, which the nodes keep up to date among
themselves with SWIM and gossip (see [Dynamic membership](#dynamic-membership-swim--gossip)), so once
the views agree every node computes the same placement. `REPLICATION_FACTOR` and `VNODES_PER_NODE` are
read by each node itself and must match on every node; `docker-compose.yml` sets them once in a shared
`x-cluster-env` block. Changing either on a cluster that already holds data changes where schemas are
placed and existing data is not rebalanced, so set them before creating data.

### Consistency (quorum reads and writes)

Replication is **tunable**: you choose how many of a schema's `N` replicas (`N` = `REPLICATION_FACTOR`)
must take part in each write and read, with `WRITE_CONSISTENCY` (`W`) and `READ_CONSISTENCY` (`R`) from
the table above. Each is one of `ONE` (1 replica), `QUORUM` (a majority, `N/2 + 1`) or `ALL` (every
replica); both default to `QUORUM`.

- **Writes:** the schema's primary applies a schema or document write (create, update, delete) and
  copies it to the other replicas, and the client is told it succeeded only when at least `W` replicas
  (the primary included) persisted it. With fewer than `W`, the client gets a "write quorum not met"
  error instead of a false success; the copies that did persist are kept, not rolled back. Creating a
  database or a user is not gated by `W`.
- **Reads:** a read by document id asks the schema's replicas, reconciles the copies it gets by vector
  clock (see [Conflict resolution and deletes](#conflict-resolution-and-deletes)) into one winner, and
  answers with it. It then **read-repairs** every replica whose copy differs from the winner, a replica
  that has no copy included. `R` is how many replicas must have answered; fewer than `R` is logged as a
  degraded read but is still served from whatever was found.
- **Read-your-writes (create, update and delete):** if `W + R > N`, every read quorum overlaps every
  write quorum, so a read by id sees the latest acknowledged document create, update or delete.
  `QUORUM`/`QUORUM` satisfies this for any `N`.
- **Scope:** read-repair applies to read-by-id only.

The compose demo runs 5 nodes with `REPLICATION_FACTOR=3` and `WRITE_CONSISTENCY=READ_CONSISTENCY=QUORUM`,
so `W = R = 2` and `W + R = 4 > 3`. A document write therefore **survives one replica being down** (the
other two still meet the quorum), and a read **heals a replica that missed an update**. To see it:

1. Create a document, and note which nodes hold its schema (`docker compose exec Node0 ls /app/data/<database>`
   and likewise on the others).
2. Stop one of the schema's replicas other than the primary, e.g. `docker compose stop Node1`. The
   primary takes the write, so if you happen to stop it the update is rejected until the others drop it
   from their rings; try another replica.
3. Update the document. It is still acknowledged, because the remaining two replicas meet the quorum.
4. Bring the node back with `docker compose start Node1`. No coordinator restart is needed: the node
   rejoins the cluster through its seeds and the others add it back to their rings by themselves (see
   [Dynamic membership](#dynamic-membership-swim--gossip)). Use `stop`/`start`, not `down`, so the node
   keeps its data.
5. Read the document by id. The read returns the latest version and repairs the stale replica.

`ONE` favours availability and speed at the price of stale reads; `ALL` favours strength at the price of
write availability. On a single node (`N = 1`) every level collapses to 1, so the local run behaves as
before. The levels are read by each node itself and must match on every node; `docker-compose.yml` sets
them in the same shared `x-cluster-env` block.

### Conflict resolution and deletes

Every document carries a **vector clock** (`_vclock`: one counter per node, bumped by the node that
coordinates the write) and a coordination timestamp (`_ts`). These, and `_version` and `_deleted`, are
server-controlled keys that clients do not set. When two replicas hold different copies of a document,
reconciliation compares their clocks:

- If one copy **causally follows** the other (its clock has seen everything the other's has), the newer
  copy is kept, even if its wall-clock timestamp is lower.
- If neither does, the writes were genuinely **concurrent**. The cluster **auto-resolves** them
  deterministically in favour of the copy with the latest `_ts` (ties broken by the clock itself), and
  stamps the winner with the merged clock so it supersedes both. It does **not** surface the competing
  versions as siblings: a read always returns one document. When three or more copies are mutually
  concurrent, they are folded in node-name order, so every replica picks the same winner.

A **delete writes a tombstone**: the document file is kept, replaced by a marker (`_deleted: true`) that
carries its own vector clock, rather than being removed. The tombstone replicates and read-repairs like
any other write, and reads, listings and indexes treat the document as gone (a read by id answers
"Document not found"). So a replica that missed a delete is **repaired to the tombstone** on the next
read of that document, and the delete **cannot be undone** by a stale replica still holding the old copy.
A replica that has no copy at all is repaired too, so it cannot later accept a late copy of a document
that was already deleted. Read-repair never overwrites a replica's newer data: a replica applies a repair
through the same causal rules as replication, so a repair that races a newer write or delete is ignored.
For a replica whose copy already agrees, the content is left unchanged (on a concurrent conflict, only
the merged vector clock is written back).

To see this in the demo above, delete the document in step 3 instead of updating it. The stopped replica
misses the delete, so after step 4 the read in step 5 answers "Document not found" and tombstones that
replica, rather than letting it serve the deleted document.

Tombstones are **not yet garbage-collected**, so they accumulate on disk, and vector clocks grow with the
number of nodes that have written a document. No new environment variables are involved.

### Dynamic membership (SWIM + gossip)

The data nodes find each other, notice failures and re-shard on their own. The coordinator no longer
wires the mesh (its one-shot wiring is inert unless you set `GOSSIP_ENABLED=false`), so **no coordinator
restart is ever needed** when a node stops, crashes, restarts or joins; this replaces the old
`docker compose restart bootstrappingnode` step. Every node keeps a membership table (`ALIVE`, `SUSPECT`,
`DEAD` or `LEFT` per node, each with an *incarnation* number) and projects it onto its consistent-hash
ring: `ALIVE` and `SUSPECT` nodes are on the ring, `DEAD` and `LEFT` ones are not.

- **Failure detection (SWIM).** Every `SWIM_PERIOD_MS` each node probes one random live peer. It pings it
  directly and waits `SWIM_PING_TIMEOUT_MS`; if there is no ack it asks `SWIM_INDIRECT_K` other peers to
  ping it on its behalf, so one lossy link does not condemn a healthy node. Only if nobody gets an ack is
  the peer marked `SUSPECT`. A suspect has `SWIM_SUSPECT_TIMEOUT_MS` to prove it is alive; if it does not,
  it is declared `DEAD` and leaves the ring.
- **Gossip.** Membership changes ride on the protocol's own ping / ack / ping-req messages (no separate
  broadcast and no shared registry): each change is piggybacked on a bounded number of messages
  (`SWIM_GOSSIP_LAMBDA` sets how many sends, scaled by cluster size; `SWIM_GOSSIP_MAX` caps updates per
  message), which spreads it to the whole cluster in roughly log(N) probe periods. Competing claims about a
  node are resolved deterministically: the higher incarnation wins, and at an equal incarnation `DEAD` and
  `LEFT` beat `SUSPECT`, which beats `ALIVE`.
- **Refutation.** Only a node can raise its own incarnation. A node that hears it is suspected or dead
  (a slow node, a healed partition, a restart under the same name) re-announces itself `ALIVE` at a higher
  incarnation, which overrides the accusation everywhere and puts it back on the rings.
- **Automatic ring reconfiguration.** Whenever a node's live set changes, that node rebuilds its ring (one
  atomic swap), so routing, replica choice and quorums follow the live members. Stopping `Node1` removes it
  from every ring; starting it brings it back. Nothing outside the data nodes is involved.
- **Join and leave.** On start a node registers itself and asks its seeds (`SEED_NODES`, tried in order;
  the first that answers wins, a node skips itself) to admit it. The seed returns its whole member list,
  which the node adopts, and the cluster hears of the newcomer by gossip. With no reachable seed, the node
  simply stays a cluster of one, and later nodes find it through their own join. The coordinator is a
  seed too (`POST /swim/join`), answering with the nodes from `CLUSTER_NODES` plus any it has learned.
  On a graceful shutdown (`docker compose stop`) a node tells a few live peers it is `LEFT`, so its ring
  entry goes at once; a crash is found by the failure detector instead.
- **Bounded state handoff.** When a membership change makes a node a replica of a schema it does not hold,
  it pulls that schema (definition and every document, tombstones included) from a current replica that
  does, instead of waiting for reads to heal it document by document. The pulled copies are merged by
  vector clock through the same causal apply that replication and read-repair use, so a newer local write
  is never overwritten and a delete is not undone. It is bounded: one pass per membership change, only for
  the schemas that actually moved to this node, and a schema that could not be pulled is retried at the
  next change.

Environment variables (all optional except `NODE_NAME` in a multi-node deployment). They are read by each
data node, and the SWIM ones should be the same on every node:

| Variable                  | Default                         | Meaning                                                                                          |
|---------------------------|---------------------------------|--------------------------------------------------------------------------------------------------|
| `GOSSIP_ENABLED`          | `true`                          | SWIM membership on. Use the literal `true` / `false`; `1` or `yes` read as `false`                |
| `NODE_NAME`               | host name (logs a warning)      | This node's **stable** name, spelled as in `CLUSTER_NODES` (`Node0`..`Node4` in compose)          |
| `SEED_NODES`              | `CLUSTER_NODES`                 | `host:port,...` contacts a starting node tries to join through; compose lists the five nodes, then the coordinator (`bootstrappingnode:8079`) |
| `SWIM_PERIOD_MS`          | `1000`                          | Protocol period: one probe per node per period                                                    |
| `SWIM_PING_TIMEOUT_MS`    | `500`                           | How long a direct ping waits for its ack before the indirect probe                                |
| `SWIM_INDIRECT_K`         | `3`                             | Peers asked to probe a silent node indirectly                                                     |
| `SWIM_SUSPECT_TIMEOUT_MS` | `5000`                          | Time a suspect has to refute before it is declared `DEAD`                                         |
| `SWIM_GOSSIP_LAMBDA`      | `3`                             | Retransmit multiplier: each update is sent about `lambda * log2(N + 1)` times                     |
| `SWIM_GOSSIP_MAX`         | `6`                             | Most updates piggybacked on one message. Keep it at least `2`: a node's own `ALIVE` takes a slot, so `1` stops dissemination |

`NODE_NAME` matters because the ring is keyed by node name. Without it a node announces its host name,
which in Docker is the container id and changes whenever the container is recreated, so every virtual node
would move and the data already on its disk would be looked for on other nodes. `docker-compose.yml`
therefore sets `NODE_NAME` on each service (`Node0` on `Node0`, and so on) to exactly the name used in
`CLUSTER_NODES`; if you add a node, give it a unique, stable `NODE_NAME` too.

To see it in the compose demo, watch the membership events with `docker compose logs -f Node0` (lines
tagged `SWIM`), then `docker compose stop Node1` (graceful: the others drop it immediately), or
`docker compose kill Node1` (a crash: it goes `SUSPECT`, then `DEAD` after `SWIM_SUSPECT_TIMEOUT_MS`), and
`docker compose start Node1` to watch it come back. With `QUORUM` consistency, document writes survive
the one missing replica throughout; once the others drop the stopped node from their rings, the next
replica on the ring takes over as primary for its schemas. When it returns, it pulls the schemas it
owns from its peers.

**The honest AP notes.** Membership is eventually consistent, in the spirit of the rest of the system:

- During a change the nodes' rings briefly disagree: each node switches to the new live set when *it*
  hears of the change, roughly log(N) probe periods apart. In that window an entry node may route a read
  or write by an older ring. Replica copies can differ for a moment; quorums, vector-clock reconciliation
  and read-repair settle them.
- A network partition is tolerated, not prevented. Each side marks the other `DEAD` and keeps serving by
  its own ring (a write whose replicas sit across the cut can fail its quorum rather than succeed
  falsely), and concurrent writes are reconciled by the causal rules afterwards. When the partition
  heals the sides **re-merge** by themselves: nodes occasionally probe a node they hold `DEAD`, and a node
  that learns it was written off refutes it with a higher incarnation.
- `GOSSIP_ENABLED=false` restores the previous behaviour for every node: no failure detector, no join and
  no handoff, and the coordinator wires the mesh once from `CLUSTER_NODES` at startup (so the old
  caveats return: start the data nodes first, and re-wire with `docker compose restart bootstrappingnode`
  after bringing a restarted node back).

### Known limitations / future work

Deliberately out of scope for this milestone:

- **No continuous anti-entropy or hinted handoff.** Background Merkle-tree anti-entropy and hinted handoff
  are future work. The state handoff is one-shot per membership change, so drift *between* changes (a
  replica that missed a write while down, say) is repaired only by read-repair when the document is read.
- **The coordinator seed does not prune departed nodes.** It remembers every node it has learned, and
  reports the `CLUSTER_NODES` as alive whether or not they are, so a joiner may briefly carry stale `ALIVE`
  "ghost" entries until SWIM probes them and marks them `DEAD`. Nodes that join dynamically and are not in
  `CLUSTER_NODES` are known to the seed but do not appear in the coordinator's `/cluster` view or client
  routing, and the seed forgets learned joiners when it restarts.
- **Concurrent creates are not reconciled.** The causal (vector-clock) rules cover updates and deletes of
  a document that exists; two clients creating documents at the same moment can still race for the same
  id assignment.
- **Handoff moves schema data only.** A pulled schema is its definition and documents (tombstones
  included). Secondary indexes are not handed off, and users and the database registry are not copied to a
  brand-new node.
- **Tombstone garbage collection and vector-clock pruning** are still future work, so tombstones and clocks
  only grow.
- **Multi-node behaviour is tested in-process.** Detection, gossip, join / leave, ring reconfiguration and
  handoff are covered by automated tests that run the membership components against in-process peers over
  real HTTP; the compose cluster is the end-to-end demo to run yourself.

Helper scripts: `./run.sh` (= `docker compose up --build`), `./rebuild.sh` (rebuild + restart
detached), `./stop.sh` (= `docker compose down`).

---

## Option B — Local, single node (no Docker)

Requires JDK 17 (`JAVA_HOME` pointing at a JDK 17 — Spring Boot 2.7.x does not run on newer JDKs here).

```bash
# build the single jar (whole reactor) once:
mvn -q -B -DskipTests -pl app -am clean package     # -> app/target/nosql-db.jar
JAR="$(pwd)/app/target/nosql-db.jar"

# run each role in its own terminal:
( cd Node && java -jar "$JAR" --role=node )          # 8080  (reads ./data/info.json — the seed)
java -jar "$JAR" --role=coordinator                  # 8079
java -jar "$JAR" --role=gateway                      # 8078
```

The data node reads `./data/info.json` (the admin seed), so launch it from the `Node/` directory,
which holds that file. Defaults target a single local node (`CLUSTER_NODES=localhost:8080`,
`BOOTSTRAP_URL=http://localhost:8079`), so no environment variables are needed.

> Single node: with no peers the ring places every schema on this one node (`REPLICATION_FACTOR`
> clamps to 1), so all operations — including gateway document writes — work locally. SWIM is on by
> default but a lone node has nobody to join or probe, so it stays a cluster of one. Set `NODE_NAME` (for
> example `NODE_NAME=Node0`) to give it a stable name; without it the node announces its host name and
> logs a warning saying so.

---

## Demo: the B+-tree secondary index

1. Sign in at `/login` as `mahmoud` / `admin`.
2. Dashboard → **Index**.
3. **Create Index** on a field (e.g. database `shop`, schema `products`, field `price`) — builds a
   disk-paged B+-tree via a sorted bulk-load of the records.
4. **Query Index** — equality / range (`GT`, `GTE`, `LT`, `LTE`, `BETWEEN`) with `ASC`/`DESC` ordering
   and `limit`/`offset` pagination. Matching documents are resolved and shown in result order.

---

## Observability & benchmarking

**Metrics.** Every role (`node`, `coordinator`, `gateway`) serves Prometheus metrics at `/actuator/prometheus`
(public, no token; Actuator exposes only `health` and `prometheus`). Besides the usual JVM and HTTP timings
there are application meters for writes, reads, read-repair, reconcile outcomes, tombstones, the ring size, SWIM
membership / probes / gossip and state handoff (`db_*`); every meter is tagged with `role` and `node`. On a
local single node: `curl -s localhost:8080/actuator/prometheus`.

**Prometheus and Grafana (Docker).** `docker compose up --build` also starts both. Neither is needed by the
database.

| Service    | URL                       | Notes                                                                                     |
|------------|---------------------------|-------------------------------------------------------------------------------------------|
| Prometheus | http://localhost:9090     | scrapes all seven processes every 10 s (`monitoring/prometheus.yml`)                       |
| Grafana    | http://localhost:3000     | dashboard **NoSQL Cluster** opens with no login; sign in as `admin` / `admin` (demo default) to edit |

The dashboard is provisioned from `monitoring/grafana/`. Read it with the caveats in
[BENCHMARKS.md, section 5](BENCHMARKS.md#5-reading-the-dashboard-correctly) (per-node counters, "conflicts" means
concurrent writes only, `db_ring_size` for the node count).

**Benchmarking.** [BENCHMARKS.md](BENCHMARKS.md) describes the YCSB binding (`ycsb-binding/`, workloads A, B, C
and F in `ycsb-binding/workloads/`), how to run the load and run phases against one node or the Docker cluster,
the measured single-node results, and a demo sequence (load, kill a node, watch SWIM / ring / read-repair,
add a node, watch handoff). The multi-node run and the dashboard were not executed where that file was
written (no Docker daemon); the file says exactly what was measured and what was not.
