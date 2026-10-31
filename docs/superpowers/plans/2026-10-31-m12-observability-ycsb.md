# M12 — Observability (Prometheus + Grafana) + YCSB benchmark Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the cluster measurable and visible — Actuator/Micrometer custom metrics on every role, a provisioned Prometheus+Grafana stack in docker-compose, and a real YCSB binding driving A/B/C/F workloads with an honest benchmark writeup.

**Architecture:** The one bootable `app` gains Actuator + a Prometheus registry; the Node module registers custom domain meters via an injected `MeterRegistry` at the existing M6–M9 seams (write/quorum, read/repair, reconcile/tombstone, SWIM/membership, handoff), never throwing into a request. A `monitoring/` stack (Prometheus scrape + provisioned Grafana dashboard) runs as two compose services. A separate YCSB binding module implements YCSB's `DB` SPI, mapping the record key onto the M1 secondary index so read/update/delete exercise the B+-tree + quorum + replication.

**Tech Stack:** Java 17, Spring Boot 2.7.6 (one-jar `app`), Spring Boot Actuator + Micrometer 1.9.x (`micrometer-registry-prometheus`), Prometheus + Grafana official images, YCSB core (`site.ycsb:core`), `com.sun.net.httpserver.HttpServer` stub for binding tests.

**Spec:** docs/superpowers/specs/2026-10-31-m12-observability-ycsb-design.md

## Global Constraints

- Build/test with `JAVA_HOME=C:\Program Files\Java\jdk-17`; full reactor `mvn -q -B -ntp verify`.
- Work directly on `master`; push to `origin` (`distributed-document-store`). Author EVERY commit as `Mahmoud Haifawi <75939511+MahmoudHaifawi@users.noreply.github.com>`; **no `Co-Authored-By` / attribution trailer of any kind** in any commit message.
- Instrumentation is READ-ONLY to behaviour: a meter update must NEVER throw into the caller (wrap in a no-throw helper) and must add NO hot-path locking. Gauges read live state lazily (bound to a supplier), never on the request path.
- Actuator exposes ONLY `health` + `prometheus` (`management.endpoints.web.exposure.include=health,prometheus`); `/actuator/health` + `/actuator/prometheus` are `permitAll` in each role's `SecurityConfig`; NO other actuator endpoint (env/beans/heapdump/loggers) is exposed; NO business-endpoint auth changes.
- All third-party images/deps are version-pinned (Prometheus, Grafana, Micrometer via the Boot BOM, `site.ycsb:core`); `monitoring/` config is checked in.
- The YCSB binding talks ONLY to the public gateway/node HTTP API (create/read/update/delete/index); it does not import or reach into server internals, and it is NOT shipped inside the `app` jar.

### Metric catalogue (EXACT names — T2/T3 register these; T4's dashboard PromQL references these verbatim; do not rename)
Common tags on every meter via a `MeterRegistryCustomizer`: `role` (node|coordinator|gateway), `node` (NODE_NAME when set).
- `db_write_total` (counter) tags: `result`=success|quorum_failed, `type`=create|update|delete, `origin`=local|forwarded — T2.
- `db_write_replicas` (distribution summary) — replication fan-out size per write — T2.
- `db_read_total` (counter) tags: `path`=by_id|all|index — T2.
- `db_read_repair_total` (counter) — read-repairs triggered — T2.
- `db_read_repair_docs_total` (counter) — docs repaired — T2.
- `db_reconcile_conflicts_total` (counter) tag: `outcome`=descends|concurrent — T2.
- `db_tombstone_written_total` (counter) — T2.
- `db_ring_size` (gauge) — physical node count in the ring — T3.
- `db_swim_members` (gauge) tag: `state`=alive|suspect|dead — T3.
- `db_swim_probes_total` (counter) tag: `kind`=direct|indirect — T3.
- `db_swim_gossip_total` (counter) tag: `direction`=sent|received — T3.
- `db_handoff_pulls_total` (counter) — T3.
- `db_handoff_docs_total` (counter) — docs applied via handoff — T3.
- (free) `http_server_requests_seconds` — per-URI throughput + p50/p95/p99, no code — all.

## Review Focus

- **Metrics never break a request.** A `MeterRegistry` that throws (or a null registry in a test slice) must not fail or slow the write/read path. → T2 `meterFailureDoesNotBreakTheWrite` (inject a throwing registry / recorder, assert the operation still succeeds).
- **Actuator surface is exactly health+prometheus, scrape is open but business stays secured.** `/actuator/prometheus` returns 200 unauthenticated; `/actuator/env` (and other sensitive endpoints) are NOT exposed (404); a business endpoint without a token still 401/403. → T1 tests.
- **Dashboard queries reference real metric names.** Every PromQL expression in the dashboard JSON uses a name from the Metric catalogue above (no typos, no renamed metric). → T4 step that greps each metric name in the dashboard against the catalogue.
- **YCSB key mapping round-trips.** insert(key,vals) then read(key) returns vals; update(key,vals2) then read returns vals2; delete(key) then read returns NOT_FOUND; a key maps to exactly one document. → T5 tests against a stub gateway.
- **Benchmark honesty.** The results table reports record count, op count, thread count, single-node vs multi-node, and read+update latency p95/p99 (not just throughput). → T6 BENCHMARKS.md review.

---

## Task 1: Actuator + Prometheus endpoint + security exposure

**Files:**
- Modify: `app/pom.xml` (add `spring-boot-starter-actuator`, `micrometer-registry-prometheus`), `Node/pom.xml` (add `micrometer-core` for custom meters), `app/src/main/resources/application*.properties` (management exposure)
- Create: a `MeterRegistryCustomizer` bean (common tags `role`,`node`) in the `app` module
- Modify: `Node`, `BootstrappingNode`, `DBMS` `security/SecurityConfig.java` — `permitAll` `/actuator/health`,`/actuator/prometheus`
- Test: `app/.../ActuatorExposureTest.java`

**Interfaces:**
- Produces: a running app that serves `/actuator/prometheus` (text exposition) + `/actuator/health` on every role, unauthenticated; common tags `role`/`node` on all meters.

- [ ] **Step 1: Write failing tests** — `ActuatorExposureTest` (@SpringBootTest node profile, dev JWT secret): `prometheusEndpointIsPublicAndRenders` (GET /actuator/prometheus → 200, body contains `http_server_requests` or `jvm_`); `envEndpointIsNotExposed` (GET /actuator/env → 404); `businessEndpointStillRequiresAuth` (an existing secured endpoint without a token → 401/403).
- [ ] **Step 2: Run, verify fail** — `mvn -q -B -ntp -pl app -am test -Dtest=ActuatorExposureTest`.
- [ ] **Step 3: Implement** the deps, exposure properties, the common-tag customizer, and the three SecurityConfig permitAll rules (mirror the existing `/health` permitAll style already in Node's SecurityConfig).
- [ ] **Step 4: Run, verify pass**, then whole reactor `mvn -q -B -ntp verify` green.
- [ ] **Step 5: Commit** — `feat(m12): expose Actuator Prometheus metrics on every role`.

## Task 2: Write / read / consistency meters

**Files:**
- Create: `Node/.../metrics/DbMetrics.java` (`@Component`, injected `MeterRegistry`, a no-throw `record*` API + a static `safe(Runnable)` wrapper)
- Modify: `Node/.../controllers/write/WriteController.java` + `services/write/WriteService.java` (write result/type/origin, fan-out size, quorum-failed), `services/read/QuorumReadService.java` (read path, repair triggered + docs), `utils/vclock/Reconcile.java` or `operations/write/WriteOperation.java` (conflict outcome, tombstone written)
- Test: `Node/.../metrics/DbMetricsTest.java`

**Interfaces:**
- Consumes: `MeterRegistry` (Micrometer).
- Produces (EXACT names from the catalogue): `db_write_total{result,type,origin}`, `db_write_replicas`, `db_read_total{path}`, `db_read_repair_total`, `db_read_repair_docs_total`, `db_reconcile_conflicts_total{outcome}`, `db_tombstone_written_total`. `DbMetrics.safe(...)` swallows any Throwable from a meter update.

- [ ] **Step 1: Write failing tests** — `DbMetricsTest`: after a local create, `db_write_total{result=success,type=create,origin=local}` == 1; a quorum-failed write increments `result=quorum_failed`; a read-repair increments `db_read_repair_total`; a concurrent reconcile increments `db_reconcile_conflicts_total{outcome=concurrent}`; a delete increments `db_tombstone_written_total`; `meterFailureDoesNotBreakTheWrite` (a recorder backed by a throwing registry → the write still returns success). Drive via the services with a `SimpleMeterRegistry`.
- [ ] **Step 2: Run, verify fail.**
- [ ] **Step 3: Implement** `DbMetrics` + the seam increments (on the completing path only; never before the operation’s result is known; wrapped in `safe`).
- [ ] **Step 4: Run, verify pass** + whole module green (`mvn -q -B -ntp -pl Node test`).
- [ ] **Step 5: Commit** — `feat(m12): write/read/consistency metrics at the quorum + reconcile seams`.

## Task 3: Membership / SWIM / handoff meters

**Files:**
- Modify: `Node/.../metrics/DbMetrics.java` (gauges + swim/handoff counters), `utils/swim/MembershipTable.java` (bind ring-size + per-state gauges), `utils/swim/SwimDetector.java` + `SwimScheduler.java` (probe + gossip counters), `utils/swim/HandoffService.java` (pull + docs counters)
- Test: `Node/.../metrics/MembershipMetricsTest.java`

**Interfaces:**
- Produces (EXACT names): `db_ring_size` (gauge), `db_swim_members{state}` (gauge), `db_swim_probes_total{kind}`, `db_swim_gossip_total{direction}`, `db_handoff_pulls_total`, `db_handoff_docs_total`. Gauges read live state LAZILY (a `Supplier` over `MembershipTable.snapshot()` / ring), registered once at startup, never on the hot path.

- [ ] **Step 1: Write failing tests** — `MembershipMetricsTest`: with N live members `db_ring_size` reports N and `db_swim_members{state=alive}` reports the alive count; after a node goes SUSPECT then DEAD the gauges follow; a direct probe increments `db_swim_probes_total{kind=direct}`; a handoff pass increments `db_handoff_pulls_total`/`db_handoff_docs_total`. Reset `Network` statics between tests.
- [ ] **Step 2: Run, verify fail.**
- [ ] **Step 3: Implement** the gauge bindings (lazy suppliers) + the swim/handoff counters.
- [ ] **Step 4: Run, verify pass** + whole module green.
- [ ] **Step 5: Commit** — `feat(m12): membership, SWIM and handoff metrics`.

## Task 4: Prometheus + Grafana stack (monitoring/)

**Files:**
- Create: `monitoring/prometheus.yml`, `monitoring/grafana/provisioning/datasources/datasource.yml`, `monitoring/grafana/provisioning/dashboards/dashboards.yml`, `monitoring/grafana/dashboards/nosql-cluster.json`
- Modify: `docker-compose.yml` (add `prometheus` + `grafana` services on `db_cluster`)

**Interfaces:**
- Consumes: the Metric catalogue names (T2/T3) in the dashboard PromQL.

- [ ] **Step 1: Write `prometheus.yml`** — one scrape job per role targeting `Node0..4:8080`, `bootstrappingnode:8079`, `dbms:8078` at `/actuator/prometheus`, 10s interval.
- [ ] **Step 2: Add compose services** — `prometheus` (official image pinned, mounts prometheus.yml, port 9090) and `grafana` (official image pinned, port 3000, admin password env, provisioning + dashboards mounts) on `db_cluster`.
- [ ] **Step 3: Grafana provisioning + dashboard** — datasource (Prometheus at `http://prometheus:9090`), a dashboards provider, and `nosql-cluster.json` with rows **Cluster** (`db_ring_size`, `db_swim_members` by state, `db_swim_gossip_total` rate, `db_handoff_pulls_total`), **Throughput & latency** (`rate(http_server_requests_seconds_count[1m])`, p50/p95/p99 via `histogram_quantile`), **Consistency** (`db_write_total` by result, `db_read_repair_total` rate, `db_reconcile_conflicts_total` by outcome, `db_tombstone_written_total`).
- [ ] **Step 4: Validate** — `docker compose config` resolves (report if the daemon is down); the dashboard JSON parses (`python -m json.tool` or `jq .`); grep EVERY metric name used in the dashboard JSON and confirm each is in the Metric catalogue (fail the task on any name not produced by T2/T3).
- [ ] **Step 5: Commit** — `feat(m12): Prometheus scrape + provisioned Grafana dashboard`.

## Task 5: YCSB binding

**Files:**
- Create: `ycsb-binding/` module (`pom.xml` depending on `site.ycsb:core` pinned, NOT added to the `app` jar; add the module to the root `pom.xml` `<modules>`), `ycsb-binding/.../NoSqlDBClient.java` (extends `site.ycsb.DB`), a small gateway HTTP client
- Test: `ycsb-binding/.../NoSqlDBClientTest.java`

**Interfaces:**
- Produces: a YCSB `DB` binding. Mapping: table→schema `ycsb` (load-phase: create schema + a B+-tree index on a reserved `ykey` field); `insert(table,key,values)`→create `{ykey:key,...values}`; `read(table,key,fields,result)`→index point-query on `ykey`→resolve doc→fill result; `update`→index-lookup id→update by id; `delete`→index-lookup→delete by id; `scan`→index range-query (only if it maps cleanly — else return `Status.NOT_IMPLEMENTED` and note it). `init()` reads `gateway.url`/creds from YCSB properties and authenticates once. Returns proper `site.ycsb.Status`.

- [ ] **Step 1: Write failing tests** — `NoSqlDBClientTest` against a `com.sun.net.httpserver.HttpServer` stub gateway: `insertThenReadReturnsValues`; `updateChangesValues`; `deleteThenReadIsNotFound`; `readMissingKeyIsNotFound`; `keyMapsToExactlyOneDocument`. Assert the client issues the expected create / index-query / by-id calls and maps responses to the right `Status`.
- [ ] **Step 2: Run, verify fail** — `mvn -q -B -ntp -pl ycsb-binding test -Dtest=NoSqlDBClientTest`.
- [ ] **Step 3: Implement** the binding + the gateway client + the load-phase schema/index creation.
- [ ] **Step 4: Run, verify pass**, then whole reactor `mvn -q -B -ntp verify` (the new module builds).
- [ ] **Step 5: Commit** — `feat(m12): YCSB binding mapping records onto the secondary index`.

## Task 6: BENCHMARKS.md + executed run + validation

**Files:**
- Create: `BENCHMARKS.md`
- Modify: `RUN.md` (link the monitoring stack + benchmark), `monitoring`/compose notes as needed

- [ ] **Step 1: Full reactor** — `JAVA_HOME="C:\Program Files\Java\jdk-17" mvn -q -B -ntp verify` → all modules green (record counts).
- [ ] **Step 2: Run the benchmark (single-node baseline, real numbers)** — start a single node (fat jar `--role=node`, NODE_NAME set), run the YCSB load phase (N records) then workloads A/B/C/F at a fixed op + thread count via the binding; capture throughput + read/update latency p95/p99. If a multi-node Docker run is possible, run it and capture the Grafana view; otherwise document the command and say the daemon was down.
- [ ] **Step 3: Write `BENCHMARKS.md`** — how to run (load + run phases, single-node and multi-node, how to point YCSB at the binding + the Grafana dashboard), the workload definitions, the results TABLE from the actual run (record/op/thread counts, single vs multi-node, throughput, read+update p95/p99), and an HONEST interpretation tied to the architecture (HTTP hops, quorum round-trips, index two-step; this characterises THIS system, not a comparison). Note the demo sequence (load → kill a node → watch SWIM/ring/read-repair on Grafana → add a node → watch handoff).
- [ ] **Step 4: Commit** — `docs(m12): benchmark methodology, results and the observability demo`.

---

## Self-Review

**Spec coverage:** Actuator/Prometheus endpoint + security (T1); custom meters — write/read/consistency (T2) and membership/SWIM/handoff (T3); Prometheus+Grafana provisioned stack (T4); YCSB binding with the index mapping (T5); BENCHMARKS.md + executed run + honest framing (T6). Non-goals (tracing, alerting, tuning-for-numbers, D/E workloads, scrape mTLS) are not implemented.

**Step scan:** each code step names the file + the exact meter/class/signature; the Metric catalogue pins names once so no step invents them. No "add appropriate metrics" placeholders.

**Type consistency:** the Metric catalogue is the single source of metric names; T2/T3 register exactly those, T4's PromQL references exactly those (a T4 step greps for drift); `DbMetrics.safe(...)` is the one no-throw wrapper used by every increment; the YCSB mapping names (`ycsb` schema, `ykey` field + index) are fixed in T5 and used in T6.

**Review Focus:** metrics-never-throw (T2), actuator-surface-exact + scrape-open-business-secured (T1), dashboard-queries-reference-real-names (T4), YCSB-key-round-trip (T5), benchmark-honesty (T6) — each has a named owning test or validation step.

**Proportion:** plan is shorter than the spec; task bodies are signatures + exact metric names + test names, not transcribed code or dashboard JSON.
