# Running the cluster

The system is **one artifact** (`app/target/nosql-db.jar`) launched in a chosen **role**. Data nodes
still run as separate processes — that is what makes it a cluster — but the build and the launcher are
unified (one binary, many roles, like Cassandra or CockroachDB).

| Role          | Port  | Launch                                             |
|---------------|-------|----------------------------------------------------|
| `node`        | 8080  | data node — storage engine + B+-tree index         |
| `coordinator` | 8079  | user routing + cluster mesh wiring + topology       |
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
the coordinator the data-node list, and `BOOTSTRAP_URL` tells the gateway where the coordinator is.

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

| Variable             | Default  | Meaning                                                                                     |
|----------------------|----------|---------------------------------------------------------------------------------------------|
| `REPLICATION_FACTOR` | `3`      | Number of nodes that hold a copy of each schema (clamped to cluster size)                   |
| `VNODES_PER_NODE`    | `150`    | Virtual nodes each physical node places on the ring (smoother balance)                      |
| `WRITE_CONSISTENCY`  | `QUORUM` | `ONE`/`QUORUM`/`ALL`: replicas that must persist a write before it is acknowledged (`W`)    |
| `READ_CONSISTENCY`   | `QUORUM` | `ONE`/`QUORUM`/`ALL`: replicas a read expects to answer; stale ones are read-repaired (`R`) |

Each node builds the ring from the cluster membership that the coordinator wires up from
`CLUSTER_NODES` (only the coordinator reads that variable), so every node computes the same
placement. `REPLICATION_FACTOR` and `VNODES_PER_NODE` are read by each node itself and must match on
every node; `docker-compose.yml` sets them once in a shared `x-cluster-env` block. Changing either on a
cluster that already holds data changes where schemas are placed and existing data is not rebalanced,
so set them before creating data.

### Consistency (quorum reads and writes)

Replication is **tunable**: you choose how many of a schema's `N` replicas (`N` = `REPLICATION_FACTOR`)
must take part in each write and read, with `WRITE_CONSISTENCY` (`W`) and `READ_CONSISTENCY` (`R`) from
the table above. Each is one of `ONE` (1 replica), `QUORUM` (a majority, `N/2 + 1`) or `ALL` (every
replica); both default to `QUORUM`.

- **Writes:** the schema's primary applies a write and copies it to the other replicas, and the client
  is told it succeeded only when at least `W` replicas (the primary included) persisted it. With fewer
  than `W`, the client gets a "write quorum not met" error instead of a false success; the copies that
  did persist are kept, not rolled back.
- **Reads:** a read by document id asks the schema's replicas and answers with the freshest copy
  (last-write-wins by document version). It then **read-repairs** every replica that held an older copy
  of it. `R` is how many replicas must have answered; fewer than `R` is logged as a degraded read but
  is still served from whatever was found.
- **Read-your-writes:** if `W + R > N`, every read quorum overlaps every write quorum, so a read sees the
  latest acknowledged write. `QUORUM`/`QUORUM` satisfies this for any `N`.

The compose demo runs 5 nodes with `REPLICATION_FACTOR=3` and `WRITE_CONSISTENCY=READ_CONSISTENCY=QUORUM`,
so `W = R = 2` and `W + R = 4 > 3`. A write therefore **survives one replica being down** (the other two
still meet the quorum), and a read **heals a replica that missed an update**. To see it: create a
document, stop one of its schema's replicas other than the primary (`docker compose stop Node1`; the
primary takes the write, so if you stop that one the update is rejected, try another), update the
document (still acknowledged), start the node again (`docker compose start Node1`) and read the document
back. The read returns the latest version and repairs the stale replica.

`ONE` favours availability and speed at the price of stale reads; `ALL` favours strength at the price of
write availability. On a single node (`N = 1`) every level collapses to 1, so the local run behaves as
before. The levels are read by each node itself and must match on every node; `docker-compose.yml` sets
them in the same shared `x-cluster-env` block.

> If the coordinator starts before the data nodes accept connections, its one-shot mesh wiring is
> skipped. Re-run it once the nodes are up: `docker compose restart bootstrappingnode`.

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
> clamps to 1), so all operations — including gateway document writes — work locally.

---

## Demo: the B+-tree secondary index

1. Sign in at `/login` as `mahmoud` / `admin`.
2. Dashboard → **Index**.
3. **Create Index** on a field (e.g. database `shop`, schema `products`, field `price`) — builds a
   disk-paged B+-tree via a sorted bulk-load of the records.
4. **Query Index** — equality / range (`GT`, `GTE`, `LT`, `LTE`, `BETWEEN`) with `ASC`/`DESC` ordering
   and `limit`/`offset` pagination. Matching documents are resolved and shown in result order.
