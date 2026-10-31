# Benchmarks (YCSB) and the observability demo

This file says how the store is benchmarked with [YCSB](https://github.com/brianfrankcooper/YCSB), what was
actually measured, and how to read the numbers. It describes **this** system on **one** machine. It is not a
comparison with any other database, and the numbers are not tuned.

## Status: what was measured and what was not

| Item | Status |
|---|---|
| YCSB load phase and workloads A, B, C, F against **one node** (1 000 records, 2 000 operations, 1 / 4 / 16 threads, 3 repetitions each) | **Measured**, results below |
| The same workloads against the **5-node Docker cluster** (RF 3, QUORUM) | **Not run.** The Docker daemon was down in the environment where this was written. The procedure is documented, the results table for it is empty. |
| `docker build` of the image (it now copies `ycsb-binding/`, see the Dockerfile) | **Not run.** Only a Maven-level check of the build context was done. Do a real `docker build` before the multi-node run. |
| The Grafana dashboard rendering live data | **Not viewed.** No Prometheus/Grafana stack was started. The dashboard's queries were checked statically against the metric names, and the application meters were read from a real node's `/actuator/prometheus` during the single-node run (below). |
| The kill-a-node / add-a-node demo | **Documented only**, not executed. |

Everything under "Results" comes from the output files of YCSB's own `site.ycsb.Client`; nothing was
estimated or extrapolated.

---

## 1. How the benchmark drives the store

The `ycsb-binding` module is a YCSB `DB` implementation (`NoSqlDBClient`) that talks to a node or gateway
only through the public JSON API, so a run exercises the real HTTP path, the quorum, the replication and the
B+-tree index.

The store addresses a document by an integer id it assigns itself; YCSB addresses a record by a string key it
chooses (`user1234`). The binding keeps the YCSB key in an ordinary field, `ykey`, with a B+-tree index on it
(one database `ycsb`, one schema `ycsb`, `ykey` plus `field0`..`field9`, all `String`). Every operation is
therefore a **two-step** one:

| YCSB operation | HTTP calls the node sees | Check against the run |
|---|---|---|
| `INSERT` | 1: `POST /write/document/new` | load phase: 1 000 inserts |
| `READ` | 2: `POST /user/index/query` (key to id), `GET /user/read/document` (id to document) | workload C, 2 000 reads: the node counted 2 000 index queries and 2 000 document reads |
| `UPDATE` | 3: index query, document read, `POST /write/document/update` carrying the `_version` that was read. Each version conflict adds 2 more (re-read and re-post, no new index lookup) | workload A, 1 thread: 1 006 updates and 994 reads gave 2 000 queries, 2 000 reads, 1 006 update POSTs |
| `READ-MODIFY-WRITE` (workload F) | 5: a `READ` followed by an `UPDATE` | workload F, 1 thread: 3 006 queries, 3 006 reads, 1 006 update POSTs |

(Counts are the node's own `http_server_requests_seconds_count`, read before and after each run.) Each client
thread also logs in once (`POST /login`), and the first thread creates the database, schema and index
(`nosql.setup=true`).

Binding properties used here: `gateway.url`, `nosql.username`, `nosql.password`, `nosql.db`, `nosql.schema`,
`nosql.setup`, `nosql.update.retries` (default 3), `nosql.timeout.ms`. Do **not** pass `-p db=...`: YCSB uses
`db` for the binding's class name; the binding's database name is `nosql.db`.

### The workloads

Four property files are in [`ycsb-binding/workloads/`](ycsb-binding/workloads): standard YCSB `CoreWorkload`,
zipfian key choice, 10 fields of 100 bytes per record, an update rewrites one field, and HdrHistogram
measurements (microsecond percentiles; YCSB's default histogram only resolves whole milliseconds).

| File | Mix | Why it is in the set |
|---|---|---|
| `workloada` | 50% read, 50% update | update heavy: the version-conflict and write-path case |
| `workloadb` | 95% read, 5% update | read mostly |
| `workloadc` | 100% read | the read path alone (index lookup then read by id) |
| `workloadf` | 50% read, 50% read-modify-write | each RMW is a read then an update of the same key |

Record and operation counts are set on the command line (`-p recordcount=1000 -p operationcount=2000`); the
files carry the same values as defaults. Workloads D and E are not run.

---

## 2. How to run

### 2.1 Single node (this is what was measured)

Needs JDK 17 (`JAVA_HOME` must point at it: Spring Boot 2.7 does not run on newer JDKs here) and Maven.
Commands are for a POSIX shell; on Windows use Git Bash, and the classpath separator is `;` instead of `:`.

```bash
# 1. Build the app jar and the binding jar, and write the binding's runtime classpath to a file.
mvn -q -B -DskipTests -pl app -am package                 # -> app/target/nosql-db.jar
mvn -q -B -DskipTests -pl ycsb-binding package            # -> ycsb-binding/target/ycsb-binding-0.0.1-SNAPSHOT.jar
mvn -q -B -pl ycsb-binding dependency:build-classpath \
    -Dmdep.outputFile=/tmp/ycsb.cp -Dmdep.includeScope=runtime   # site.ycsb:core:0.17.0 + org.json + its jars

# 2. Start ONE node from a scratch directory (it keeps its data under ./data, and the benchmark writes a lot:
#    do not start it inside the repository). data/info.json is the seed with the admin account mahmoud / admin.
mkdir -p /tmp/bench-node/data && cp Node/data/info.json /tmp/bench-node/data/
JAR="$(pwd)/app/target/nosql-db.jar"
( cd /tmp/bench-node && NODE_NAME=Node0 java -Xmx1g -jar "$JAR" --role=node ) &
curl -s localhost:8080/health                            # -> UP

# 3. Load phase, once (4 client threads), then the workloads.
CP="ycsb-binding/target/ycsb-binding-0.0.1-SNAPSHOT.jar:$(cat /tmp/ycsb.cp)"
OPTS="-db com.database.atypon.ycsb.NoSqlDBClient -p gateway.url=http://localhost:8080 \
      -p nosql.username=mahmoud -p nosql.password=admin -p recordcount=1000 -s"

java -cp "$CP" site.ycsb.Client -load $OPTS -P ycsb-binding/workloads/workloada -threads 4

for W in a b c f; do for T in 1 4 16; do
  java -cp "$CP" site.ycsb.Client -t $OPTS -P ycsb-binding/workloads/workload$W \
       -p operationcount=2000 -threads $T > run-$W-t$T.txt
done; done
```

`-s` prints a status line every 10 s to stderr; the measurements are on stdout (`[READ]`, `[UPDATE]`,
`[READ-MODIFY-WRITE]`, `[OVERALL]`). The number to look at for failures is `[UPDATE], Return=ERROR, n` (and
`[READ], Return=ERROR`). Stop the node afterwards. Use a fresh data directory (or a different `-p
nosql.db=`) for every load: the store has no unique-key constraint, so loading twice creates duplicates.

While a run is going, `curl -s localhost:8080/actuator/prometheus` shows the node's own counters (this is how
the HTTP call counts above were taken).

### 2.2 Multi-node (5-node Docker cluster): documented, NOT run

None of this was executed (the Docker daemon was down). Treat every step as unverified.

1. **Build the image for real first.** `docker build -t nosql-db:latest .` (or just `docker compose build`).
   The Dockerfile gained `COPY ycsb-binding ycsb-binding` so that Maven can read all declared modules; that
   was only checked with `mvn validate` in a copy of the build context.
2. **Publish a data-node port.** `docker-compose.yml` publishes only the gateway (8078), and the binding speaks
   the node API (`/login`, `/write/...`, `/user/...`, `/admin/...`), so it must reach a data node. Either
   publish Node0's port with an override file, or run the binding inside the `db_cluster` network (2.3).
   The override below passes `docker compose config`:

   ```yaml
   # docker-compose.bench.yml  (create it next to docker-compose.yml; not checked in)
   services:
     Node0:
       ports:
         - "8080:8080"
   ```

   ```bash
   docker compose -f docker-compose.yml -f docker-compose.bench.yml up --build -d
   curl -s localhost:8080/health                         # -> UP
   ```

   Wait until the Grafana panel "Live nodes on the ring" says 5 (http://localhost:3000, dashboard "NoSQL
   Cluster", no login needed; Prometheus is on http://localhost:9090).
3. **Load and run exactly as in 2.1**, with the same `gateway.url=http://localhost:8080` (Node0 is the entry
   point; it forwards to the schema's replicas). Use a fresh database. Record the same table.
4. **Things to check, not assume.** The binding creates the database, schema and index through one node. That
   the index ends up on every replica of the `ycsb` schema is not verified here: after the load phase, run a
   few reads and look at `[READ]` NOT_FOUND / ERROR counts before trusting a run. Which node is the primary of
   the schema, and whether Node0 is one of its three replicas, changes the number of hops per operation; note
   it next to the results (`docker compose exec Node0 ls /app/data/ycsb`, and likewise on the others).

### 2.3 Running the binding inside the cluster network (alternative to publishing a port)

Attach a JRE container to the compose network (`docker network ls`; the name is `<project>_db_cluster`, for
example `nosql-database_db_cluster`) with the repository and the Maven repository mounted, and point
`gateway.url` at `http://Node0:8080`. The classpath file from step 1 holds host paths, so regenerate it
inside the container or mount `~/.m2` at the same path. Also untested.

---

## 3. Results: single node

**Setup.** One data node (`--role=node`, `NODE_NAME=Node0`, gossip on, no seeds: a cluster of one;
`db_ring_size` read 1 at the end of the run), JDK 17.0.14, `-Xmx1g`, fresh data directory, replication factor
and consistency clamped to 1 because there is one node. YCSB 0.17.0 `site.ycsb.Client` on JDK 17 on the **same
machine**: Intel Core i7-1065G7 (4 cores / 8 threads), 15.8 GB RAM, Windows 11. Client and node therefore share
the CPU. Commit under test: `3484aa9`. Run on 2026-10-31.

**Method.** Load: 1 000 records, 4 threads (one run). Then, for each of A, B, C, F and each of 1, 4 and 16
client threads: 2 000 operations over 1 000 records, zipfian keys, **3 repetitions** (the whole matrix was run three times in sequence, on the same
loaded data). Before the measured runs the node had served a small smoke test (50 records, 100 operations) and 500 discarded
warm-up reads. Throughput is the median of the 3 runs with the min-max range beside it; the percentiles are the median
of the three runs' own percentiles. Latency is measured by YCSB around the whole operation, so a `READ` includes
both of its HTTP calls. Operations that fail are counted under `Return=ERROR` and their latency goes into a
separate `*-FAILED` bucket, so the percentiles below are for successful operations only.

**Load phase** (1 000 inserts, 4 threads, one run): 75 ops/s; insert p50 38.7 ms, p95 81.1 ms, p99 102.5 ms; 0 failed.

**Workloads** (single node, 1 000 records, 2 000 operations per run):

| Workload | Threads | Throughput, ops/s: median (min-max) | READ p95 / p99, ms | UPDATE p95 / p99, ms | READ-MODIFY-WRITE p95 / p99, ms | FAILED updates / update ops (3 runs) |
|---|---|---|---|---|---|---|
| A | 1 | 51.5 (51.3-52.5) | 14.9 / 16.7 | 33.3 / 36.5 | - | 0 / 3032 |
| A | 4 | 139 (112-144) | 23.8 / 30.4 | 62.2 / 84.7 | - | 0 / 2950 |
| A | 16 | 98.5 (87.1-114) | 37.2 / 63.8 | 507 / 619 | - | **4** / 3046 |
| B | 1 | 84.5 (84.1-85.2) | 13.9 / 15.8 | 31.6 / 37.4 | - | 0 / 287 |
| B | 4 | 257 (245-288) | 22.7 / 30.1 | 44.4 / 67.3 | - | 0 / 302 |
| B | 16 | 310 (305-318) | 65.0 / 80.5 | 196 / 290 | - | 0 / 338 |
| C | 1 | 94.4 (92.5-95.0) | 13.2 / 15.0 | - | - | 0 / - |
| C | 4 | 277 (259-291) | 21.0 / 31.1 | - | - | 0 / - |
| C | 16 | 330 (321-333) | 78.8 / 115 | - | - | 0 / - |
| F | 1 | 42.9 (41.5-43.1) | 14.2 / 15.8 | 30.2 / 32.9 | 43.4 / 47.9 | 0 / 3005 |
| F | 4 | 98.4 (97.0-108) | 28.0 / 31.3 | 64.5 / 77.4 | 84.3 / 100 | 0 / 3000 |
| F | 16 | 103 (98.3-125) | 34.1 / 51.3 | 446 / 523 | 470 / 557 | 0 / 2956 |

Reading notes. In workload F, YCSB's `READ` line also contains the read half of every read-modify-write, and
`UPDATE` is the write half. No `READ` failed in any run. The throughput of a run is operations per second over
the whole mix, so A, B, C and F are not comparable as "which is faster" without looking at the mix. The spread
between repetitions is real: at 16 threads it reaches roughly +/-15%, so differences smaller than that between
two cells mean nothing.

**Multi-node (5 nodes, RF 3, QUORUM/QUORUM): NOT RUN.**

| Workload | Threads | Throughput | READ p95 / p99 | UPDATE p95 / p99 | FAILED |
|---|---|---|---|---|---|
| A, B, C, F | 1 / 4 / 16 | not run | not run | not run | not run |

### 3.1 Failed operations and the hot-key caveat

Every failure in the table is an `UPDATE` that kept hitting an optimistic version conflict. The store replaces
a whole document and checks `_version`; two threads updating the same key between one thread's read and its
write make the second write fail. Zipfian key choice concentrates operations on a few keys, so this happens
exactly when many threads write at once. The binding re-reads and retries, up to `nosql.update.retries`
(default 3) more times, then reports `ERROR`. The knob, swept on workload A at 16 threads (3 runs each; "retries
executed" is the node's update POSTs beyond one per update operation, i.e. conflicts that were retried):

| `nosql.update.retries` | FAILED updates per run (of about 1 000) | retries executed per run | Throughput, ops/s: median (range) |
|---|---|---|---|
| 0 | 28, 29, 32 | 0 | 139 (131-151) |
| 3 (default) | 2, 2, 0 | 44, 39, 47 | 98.5 (87-114) |
| 10 | 0, 0, 0 | 51, 42, 48 | 130 (119-134) |

So roughly 3% of first attempts conflict at 16 threads on this key distribution; the default retries turn that
into 0-0.2% failed updates, and 10 retries into none. Retries are not free (each one repeats a document read and
a write), but the throughput column is **not** evidence about the knob: the runs with 3 retries come from the main
matrix (each preceded by other workloads) while the 0 and 10 retries sets ran back to back on their own, and the
trend is not monotonic (139 at 0 retries, 98.5 at 3, 130 at 10). What is solid is the failure count. With 1 and 4 threads there were no failures at all (workload A at
4 threads still retried a handful of conflicts successfully). Report `FAILED` next to every throughput figure.
A single key hammered by many threads is far worse than this zipfian case (an 8-thread, one-key test during
development lost 25 of 80 updates at 3 retries); YCSB's own key spread is the benign case.

### 3.2 Where the time goes (the node's own view)

`http_server_requests_seconds` on the node, mean milliseconds per call, median of the 3 runs (this is time
inside the node's HTTP handler; the client adds a few milliseconds of its own):

| Run | Index query | Document read | Update POST | Client-side mean of the YCSB operation |
|---|---|---|---|---|
| C, 1 thread | 5.4 | 2.5 | - | READ 10.2 |
| C, 4 threads | 7.2 | 3.1 | - | - |
| C, 16 threads | 25.7 | 8.1 | - | READ 37.7 |
| A, 1 thread | 6.5 | 2.7 | 13.5 | UPDATE 26.3 |
| A, 4 threads | 10.3 | 2.5 | 22.5 | UPDATE 38.1 |
| A, 16 threads | 17.4 | 2.9 | 220 | UPDATE 251 |

---

## 4. What the numbers say about this system

These readings tie the measurements to the architecture. The causes in the second half of each point are my
reading of the code, consistent with the data, **not** the output of a profiler.

1. **A read is two HTTP round trips, an update is three.** That is the cost of mapping YCSB's string keys onto
   a store that addresses documents by integer id: first the B+-tree index (`ykey` to id), then the read by id.
   At one thread a `READ` takes about 10 ms, of which about 8 ms is inside the node (index query 5.4 ms,
   document read 2.5 ms); the index lookup is the larger share. A key-addressed store would do one call.
2. **Reads scale to the machine, not to the design.** From 1 to 4 threads, workload C goes from 94 to 277 ops/s
   (about 3x); at 16 threads it is 330, so the box is saturated. Client and node share 8 hardware threads here,
   so this says little about the node's own ceiling. The index query slows more than the by-id read as load
   rises (5.4 to 25.7 ms against 2.5 to 8.1 ms); every `Pager` method (the B+-tree's page access) is
   `synchronized`, so concurrent index lookups queue on one monitor.
3. **Writes are serialised per node, and it shows.** All creates, updates and deletes on a node run inside one
   `synchronized (this)` block in `WriteOperation`, and index maintenance for each write takes the index
   manager's single write lock, which also blocks index queries (a global `ReentrantReadWriteLock` that the code
   comments call adequate because writes are expected to be rare). The data fits that picture: an update POST costs about
   13.5 ms inside the node at 1 thread, which caps a node near 70 updates/s, and workload A at 4 threads
   reaches about that (for example 986 updates in 13.9 s in one run). Past that, more threads only lengthen the queue: at 16 threads
   the update POST averages 220 ms in the node, update p99 is 619 ms against 36 ms at one thread, and total
   throughput falls from 139 to 98.5 ops/s. Workload F tops out near 100 ops/s at both 4 and 16 threads, which is consistent
   with the same limit. A write-heavy YCSB mix is not what this write path is built for; B and C are closer to its
   intended use.
4. **Failures are conflicts, and they are visible.** There are no read failures, and update failures only
   appear when many threads write at once (section 3.1). The retry knob trades latency for success rate; it
   does not remove the conflict, it re-does the work.
5. **The single-node run leaves out what makes this a cluster.** With one node the replication factor and both
   consistency levels clamp to 1, so there are no quorum round trips, no replication traffic, no read-repair,
   no SWIM peers and no handoff (the node's `db_read_repair_total`, `db_reconcile_conflicts_total` and
   `db_handoff_*` meters all read 0, as they should). The numbers above are therefore a floor for the HTTP +
   index + storage path, not an estimate of cluster performance. In the 5-node, RF 3 compose cluster every
   update additionally goes to the schema's primary, which copies it to the other replicas over HTTP and waits
   for `W = 2` of 3 acknowledgements, and a read by id asks the replicas (`R = 2`), reconciles by vector clock
   and read-repairs a replica that differs; an entry node that is not a replica adds a forwarding hop. The
   expectation (a hypothesis for the not-yet-run experiment, not a result) is lower per-operation throughput
   and higher latency than the single-node figures, and no gain in write throughput for a benchmark with
   one schema, since all of that schema's writes pass through one primary. What the cluster buys is
   availability: a QUORUM write survives a down replica.
6. **Metrics matched the run.** At the end of the run the node reported `db_write_total{origin="local",
   type="create",result="success"} = 1050` (1 000 loaded + 50 from the smoke test), `db_read_total` split
   about evenly between `path="index"` (93 607) and `path="by_id"` (93 993), and `result="quorum_failed"` 0,
   which agrees with the two-step read. The two paths differ by 386, and that is exactly the 386 conflict retries
   counted independently from the update POSTs (each retry re-reads the document by id but does not repeat the
   index query).

Limits of this experiment: one machine shared by client and server; 1 000 records that fit in cache; 2 000
operations per run (a short run, and the zipfian sequence differs between repetitions); three repetitions;
no tuning of the JVM, Tomcat or the store; HTTP/1.1 from the JDK `HttpClient` with a login per thread. It
characterises this store's current design, nothing more.

---

## 5. Reading the dashboard correctly

The provisioned Grafana dashboard ("NoSQL Cluster") is built on the application's Micrometer meters plus Spring's
HTTP timings. A few semantics matter when you read it during a benchmark or the demo:

- **Per-node metrics.** Every meter carries `role` and `node` labels and counts what *that process* did. The
  write counters and `db_tombstone_written_total` are per node: a client write is counted on the entry node
  (`origin="forwarded"`) and on the primary (`origin="local"`), so the cluster panels filter `origin="local"`;
  a delete stores a tombstone on every replica, so the tombstone total is about N times the number of deletes.
- **"Conflicts" means concurrent writes only.** `db_reconcile_conflicts_total{outcome="concurrent"}` is the
  conflict panel. `outcome="descends"` fires on every routine replicated update or delete (about N-1 per client
  write): that is ordinary replication, **not** a conflict, and it is not charted as one. Note also that the
  optimistic `_version` conflicts behind the YCSB `FAILED` counts are rejected by the node's local apply before
  any quorum verdict, so `db_write_total` does not count them (the run above ended with `quorum_failed` at 0
  although updates had failed).
- **Membership gauges.** `db_swim_members{state}` gauges are taken independently and a member that has left is
  in none of them, so alive + suspect + dead is not the cluster size. Use `db_ring_size` for the number of nodes
  on the ring.
- **Latency percentiles** on the dashboard are computed from the HTTP histogram, per HTTP call, not per YCSB
  operation: a YCSB `READ` is two calls.
- **Scraping.** Prometheus scrapes all seven processes every 10 s, so panels lag the benchmark by up to that
  much; `rate()` windows add to it.

---

## 6. The demo sequence (documented, not run)

The point of the observability stack is to watch the cluster react while it is under load. Steps 1 to 6
describe what to do and what to look for; none of it was executed here (no Docker daemon), and what the
panels show is what the meters are defined to count, not something observed.

0. **Bring it up.** Follow 2.2 (build, override, `up`). Open Grafana at http://localhost:3000 (dashboard "NoSQL
   Cluster"; sign in as `admin` / `admin` only to edit) and Prometheus at http://localhost:9090/targets (7
   targets up). Ring size should read 5.
1. **Load.** Run the YCSB load phase. Watch "Client writes per second" (type `create`) and "Requests per
   second by URI".
2. **Put it under steady load.** Start a long run so the panels have something to show, for example workload A
   with `-threads 4 -p operationcount=1000000 -p maxexecutiontime=600`. Find the schema's replicas with
   `docker compose exec Node0 ls /app/data/ycsb` and likewise on the other nodes.
3. **Kill a node.** Pick a replica that is not the entry node (Node0): `docker compose kill Node3`. A crash
   rather than `stop`, so the failure detector has to find it. Watch:
   - "SWIM members by state": the node goes `suspect`, then `dead` after `SWIM_SUSPECT_TIMEOUT_MS` (5 s by
     default); "SWIM probes per second" shows indirect probes rising while peers try to reach it.
   - "Live nodes on the ring" and "Ring size per node view": 5 to 4, with the nodes' views briefly out of step
     (each learns of it by gossip, roughly log N probe periods apart); "SWIM gossip messages per second" bumps.
   - "Quorum-failed writes per second" should stay at 0 when the dead node was not the schema's primary
     (`W = 2` is still met by the other two replicas). If you kill the primary instead, expect YCSB `FAILED`
     updates (and quorum failures) until the others drop it from their rings: [RUN.md](RUN.md) describes
     that window.
4. **Bring it back and watch repair.** `docker compose start Node3` (not `down`: it must keep its data). Watch
   it return to `alive` at a higher incarnation, ring size back to 5, and "State handoff" show a pull and the
   documents whose copy on that node actually changed (only the writes it missed, not the whole schema). Then run workload C: "Read repairs"
   rises as reads by id find replicas that missed updates (read-repair covers read-by-id only), and
   "Client reads per second" shows the index and by-id paths.
5. **Add a node.** Add a `Node5` service (a copy of `Node4` with `NODE_NAME: "Node5"`) to
   `docker-compose.yml`, add `Node5:8080` to the `node` job in `monitoring/prometheus.yml` (targets are
   static), then `docker compose up -d Node5 && docker compose restart prometheus`. It joins through its
   seeds. Watch ring size go to 6 and "State handoff" pull the schemas that now place on it.
   Known limits from RUN.md, relevant to this demo: handoff moves schema definitions and documents only, **not
   secondary indexes**, so index queries (every YCSB read) that the cluster routes to the new replica cannot be
   assumed to work and a rise in `FAILED`/NOT_FOUND after the join should be read against that limit, not as a
   new bug; and a dynamically joined node outside `CLUSTER_NODES` is not in the coordinator's routing view.
6. **Stop the load** and let the panels settle.

What to take away: failure detection and ring reconfiguration happen on their own within a few probe periods, a
single-replica loss does not stop QUORUM writes, repair is visible as read-repair and handoff counters, and the
YCSB `FAILED` count and p99 are where a client sees the disturbance.
