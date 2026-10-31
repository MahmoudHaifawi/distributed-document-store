# M12 — Observability (Prometheus + Grafana) + YCSB benchmark

Status: design — pending review before implementation planning.
Milestone: M12 of the capstone roadmap (makes the system measurable — the "demo with numbers").
Workflow: work directly on `master`; canonical remote `distributed-document-store`.

## 1. Intent / why

M6–M9 built the distributed machinery (sharding, quorum, causal reconciliation, dynamic membership),
but none of it is **visible or measured**: there are no metrics, no dashboards, and no load test. You
cannot show an interviewer "here is the p99 write latency, here is the read-repair rate climbing when a
node dies, here is the ring reconfiguring under load." M12 closes that gap and turns the project into a
**demo with numbers**.

Three layers:
1. **Instrumentation** — Spring Boot Actuator + Micrometer expose a Prometheus endpoint on every role,
   with **custom domain metrics** that tell the distributed-systems story: quorum acks vs failures,
   read-repairs triggered, writes served-locally vs forwarded, replication fan-out, live ring size,
   SWIM alive/suspect/dead counts, gossip volume, handoff pulls, vector-clock conflicts resolved,
   tombstones written. These are the metrics that make the architecture legible, not just the
   free HTTP latency histograms.
2. **Prometheus + Grafana** — scrape every node/coordinator/gateway and ship a **provisioned** Grafana
   dashboard (datasource + dashboard JSON baked in, comes up ready) that visualises throughput,
   latency percentiles, quorum health, read-repair, and live membership.
3. **YCSB benchmark** — a real Yahoo! Cloud Serving Benchmark binding against the database, driving the
   standard workloads (A/B/C/F) so throughput and latency are measured with the recognised industry
   tool, with the load visible live on the Grafana dashboard.

Being able to say "I instrumented it with Micrometer, scrape it with Prometheus, visualise it in
Grafana, and load-test it with a YCSB binding I wrote — and here's what the quorum and read-repair
curves do when I kill a node" is the standout outcome: it demonstrates operability, not just features.

**Honest framing.** The absolute numbers characterise **this** system (full HTTP hops between roles,
no WAL/durability, one file per document) — they are not a Mongo/Cassandra comparison, and the design
says so. The value is the **methodology and the observability**: a correct YCSB binding, a real metrics
pipeline, and dashboards that explain the system's behaviour under load and under failure. The YCSB
run is most interesting against the multi-node Docker cluster; a single-node baseline is always
runnable for a smoke/number even when Docker is down.

## 2. Goals / non-goals / anti-goals

### Goals
- **Actuator + Micrometer + Prometheus registry** on the one bootable `app`, exposing
  `/actuator/prometheus` and `/actuator/health` on every role (node, coordinator, gateway), reachable
  by Prometheus (see Constraints for the security decision).
- A **catalogue of custom meters** registered via an injected `MeterRegistry`, each at the seam where
  the M6–M9 logic already lives, tagged by role/node where useful:
  - writes: `db.write.total{result,type}` (local vs forwarded, create/update/delete), replication
    fan-out size, `db.write.quorum.failed`;
  - reads: `db.read.total{path}`, `db.read.repair.triggered`, `db.read.repair.docs`;
  - consistency: `db.reconcile.conflicts{outcome=descends|concurrent}`, `db.tombstone.written`;
  - membership (gauges): `db.ring.size`, `db.swim.members{state=alive|suspect|dead}`,
    `db.swim.probes{kind=direct|indirect}`, `db.swim.gossip.sent/received`,
    `db.handoff.pulls`, `db.handoff.docs.applied`;
  - plus the free `http.server.requests` timer (per-endpoint throughput + latency percentiles).
- A **`monitoring/` stack** wired into `docker-compose.yml`: a `prometheus` service (scrape config for
  all five nodes + coordinator + gateway) and a `grafana` service with a **provisioned** datasource and
  a dashboard JSON, so `docker compose up` brings up a working dashboard with no manual clicks.
- A **YCSB binding** (a Java class implementing the YCSB `DB` SPI) that maps YCSB's key-value model onto
  this document store and drives the real YCSB runner for workloads **A** (50/50 read/update), **B**
  (95/5), **C** (read-only) and **F** (read-modify-write), reporting throughput + latency percentiles.
- A **benchmark writeup** (`BENCHMARKS.md`): how to run it (load phase + run phase, single-node and
  multi-node), the workloads, a results table from an actual run the implementer performs (at minimum
  single-node), and an honest interpretation tied to the architecture (where the HTTP hops and quorum
  show up in the curves).
- The existing suite stays green; new tests assert the custom meters are registered and move
  (increment/gauge) when their operation runs, and the Prometheus endpoint renders.

### Non-goals (this milestone)
- **Distributed tracing** (OpenTelemetry/Jaeger spans across role hops) — metrics only; tracing is a
  named follow-up.
- **Alerting / Alertmanager rules**, long-term metrics storage (Thanos/Cortex), or a hosted stack.
- **Tuning the database for better benchmark numbers** — M12 measures and visualises; it does not
  change the data path to win a benchmark (that would be a separate performance milestone).
- **A full YCSB workload matrix** (D/E and custom distributions) — A/B/C/F are the representative set;
  scan-heavy E depends on range scan, offered only if the binding's scan maps cleanly to the M1 index.
- **Authenticating the scrape pipeline with mTLS / per-scrape tokens** — see Constraints.

### Anti-goals (would make this a failure even if it "works")
- **Vanity metrics** that don't explain the system — a dashboard of CPU graphs with none of the
  quorum/membership/read-repair signals that make THIS architecture legible.
- **Dishonest benchmarking** — presenting the numbers as a comparison to production databases, hiding
  the single-node vs multi-node distinction, or reporting throughput without latency percentiles and
  the record/thread counts that produced them.
- **A metrics path that changes behaviour** — instrumentation that adds locking on the hot path,
  throws into the request, or measurably slows writes/reads.
- Breaking the single-node run, the role launcher, or M6/M7/M8/M9.

## 3. Constraints

- Java 17 / Spring Boot 2.7.6; the one-jar `app` module is the only `@SpringBootApplication`. Add
  `spring-boot-starter-actuator` + `micrometer-registry-prometheus` so the running app exposes
  `/actuator/prometheus`; the role modules (Node especially) depend on `micrometer-core` to register
  custom meters via an injected `MeterRegistry`. Build with `JAVA_HOME=C:\Program Files\Java\jdk-17`.
- Instrument at the EXISTING seams, read-only to behaviour: `WriteController.withWriteQuorum` /
  `WriteService` (write + fan-out + quorum result), `QuorumReadService` (read + repair),
  `Reconcile`/`WriteOperation` (conflicts, tombstones), `MembershipTable` / `SwimDetector` /
  `SwimScheduler` (ring size, SWIM states, probes, gossip), `HandoffService` (pulls). A meter update
  must never throw into the caller and must not add hot-path locking.
- **Security decision:** `/actuator/health` and `/actuator/prometheus` are `permitAll` on each role's
  `SecurityConfig` so Prometheus can scrape them over the cluster network, and `management.endpoints`
  exposes only those two (not env/beans/heapdump). This is the standard dev-cluster posture; the design
  states explicitly that a production deployment would put the scrape endpoint behind the cluster
  network boundary or a scrape credential. No business endpoint's auth changes.
- Prometheus/Grafana run as their official images in `docker-compose.yml` on the existing
  `db_cluster` network; all config is version-pinned and checked in under `monitoring/`.
- The YCSB binding lives in its own module/dir and depends on the YCSB core (`site.ycsb:core`, pinned).
  It talks to the cluster over the **gateway's** HTTP API (or a node's), reusing the existing
  create/read/update/delete/index endpoints; it does not reach into internals.
- Author every commit as Mahmoud Haifawi; no attribution trailers.

## 4. The design (decided "how")

### 4.1 Metrics layer
Actuator + a `PrometheusMeterRegistry` auto-configure from the starters; `management.endpoints.web.
exposure.include=health,prometheus` and a common tag `role` (+ `node` = NODE_NAME) via a
`MeterRegistryCustomizer`. Custom meters are registered in small `@Component` recorders or inline where
the operation completes, using counters/timers/gauges:
- Counters incremented on the completing path (e.g. `withWriteQuorum` tags the result
  success/quorum-failed and whether it was local or forwarded; `QuorumReadService` counts repairs and
  repaired-doc counts; `Reconcile` outcome is counted where the winner is chosen; a tombstone write is
  counted in `WriteOperation`).
- Gauges bound to live state (ring physical-node count; SWIM counts per state from `MembershipTable.
  snapshot()`), registered once and read lazily so they never touch the hot path.
- The free `http.server.requests` timer gives per-URI p50/p95/p99 and throughput with no code.
Every meter update is wrapped so a metrics failure can never break a request.

### 4.2 Prometheus + Grafana (monitoring/)
`monitoring/prometheus.yml`: a scrape job per role, targeting `Node0..4:8080`, `bootstrappingnode:8079`,
`dbms:8078` at `/actuator/prometheus`, 5–15s interval. `docker-compose.yml` gains `prometheus`
(mounts the config, 9090) and `grafana` (3000, env admin password, provisioning mounts). Grafana
provisioning under `monitoring/grafana/`: `datasource.yml` (Prometheus at `http://prometheus:9090`) and
a `dashboards.yml` + `nosql-cluster.json` dashboard with rows: **Cluster** (ring size, SWIM
alive/suspect/dead over time, gossip rate, handoff pulls), **Throughput & latency** (ops/sec and
p50/p95/p99 per endpoint from `http.server.requests`), **Consistency** (quorum success vs failed,
read-repair rate, reconcile conflicts, tombstones). The dashboard comes up populated once load runs.

### 4.3 YCSB binding + mapping
A `NoSqlDBClient extends site.ycsb.DB`. The impedance to resolve: YCSB chooses the record **key**
(e.g. `user1234`), while this store **assigns** an integer id on create and reads by id or by an indexed
field. Resolution — map YCSB onto the existing **secondary index** (M1), which also makes the benchmark
exercise the B+-tree index, quorum and replication together:
- **table → schema** `ycsb` (created in the load phase with the YCSB fields + a B+-tree index on a
  reserved `ykey` field);
- **insert(key, values)** → create a document `{ykey:key, ...values}` via the write API;
- **read(key)** → index point-query on `ykey` → resolve the document;
- **update(key, values)** → index-lookup the id → update by id (optimistic version from the read);
- **delete(key)** → index-lookup → delete by id (tombstone);
- **scan(startkey, count)** → index range-query (M1.3 range + limit), offered only if it maps cleanly.
`init()` reads the gateway URL + credentials from YCSB properties and logs in once. The binding returns
YCSB `Status` codes and never holds state between operations beyond the auth token.

### 4.4 Benchmark methodology (BENCHMARKS.md)
Standard two-phase YCSB: a **load** phase (insert N records) then a **run** phase per workload
(A/B/C/F) at a fixed operation count and thread count. Record, for each workload: throughput
(ops/sec), and read/update latency avg + p95 + p99, plus the record/op/thread counts and whether it was
single-node or the multi-node cluster. The implementer runs at least the **single-node** baseline for
real numbers; the multi-node run is documented and, if Docker is available, executed and its Grafana
view captured. The writeup interprets the numbers against the architecture (HTTP hop cost, quorum
round-trips, index two-step) — honestly, as a characterisation of this system, not a comparison.

### 4.5 Demo
`docker compose up` → open Grafana (provisioned dashboard) → run the YCSB load + workload A → watch
throughput/latency and quorum/read-repair panels move → `docker stop Node3` → watch SWIM mark it
suspect→dead, the ring size drop, read-repair tick up, and the cluster keep serving on quorum → start a
new node → watch it join and the handoff-pull counter move. That single sequence demonstrates every
distributed feature M6–M9 built, measured and visualised.

## 5. What's left to the builder
- Exact Micrometer meter names/tags/types and the recorder wiring at each seam (counters on the
  completing path; gauges bound to `MembershipTable`/ring; the no-throw wrapper); the
  `MeterRegistryCustomizer` common tags.
- The Actuator exposure + `permitAll` rules in each role's `SecurityConfig` (health + prometheus only).
- `monitoring/` contents: `prometheus.yml` scrape jobs, Grafana datasource + dashboard provisioning and
  the dashboard JSON panels/queries; the two compose services and their mounts/ports.
- The YCSB binding: the key→`ykey`-index mapping, the two-step lookup for read/update/delete, scan
  feasibility against the M1 range query, `Status` mapping, and the load-phase schema+index creation;
  the YCSB core version and how it is launched (its own module/dir, not shipped in the app jar).
- `BENCHMARKS.md`: the exact commands, the run matrix, the results table from the actual run, and the
  honest interpretation; which panels/screenshots to capture.
- Test design: meters are registered and move when their op runs (MockMvc / unit at the seam);
  `/actuator/prometheus` renders and is reachable without auth while business endpoints stay secured;
  the binding's mapping is unit-tested against a stub gateway (insert/read/update/delete round-trip)
  without needing a live cluster.

Likely plan decomposition (for the planning step, not binding): **M12.1** metrics instrumentation +
Actuator/Prometheus endpoint + custom meter catalogue + security exposure; **M12.2** `monitoring/`
Prometheus + Grafana stack with the provisioned dashboard in docker-compose; **M12.3** YCSB binding +
`BENCHMARKS.md` + an executed run. M12.3 (the YCSB binding) is the largest and most deferrable piece —
if the real YCSB framework integration proves heavy, it can fall back to a self-contained
YCSB-style load generator (same A/B/C/F mix, HdrHistogram latencies, same Prometheus/Grafana view),
which is a weaker claim but a complete deliverable.

The implementation plan (superpowers:writing-plans) follows approval of this document.
