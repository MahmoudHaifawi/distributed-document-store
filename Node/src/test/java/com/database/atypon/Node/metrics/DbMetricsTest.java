package com.database.atypon.Node.metrics;

import com.database.atypon.Node.controllers.index.IndexController;
import com.database.atypon.Node.controllers.read.ReadController;
import com.database.atypon.Node.controllers.write.WriteController;
import com.database.atypon.Node.index.BPlusTree;
import com.database.atypon.Node.model.Network;
import com.database.atypon.Node.model.Node;
import com.database.atypon.Node.operations.write.WriteOperation;
import com.database.atypon.Node.security.JwtService;
import com.database.atypon.Node.services.index.IndexManager;
import com.database.atypon.Node.services.read.QuorumReadService;
import com.database.atypon.Node.services.read.ReadService;
import com.database.atypon.Node.services.write.WriteService;
import com.database.atypon.Node.utils.ring.HashRing;
import com.database.atypon.Node.utils.ring.RingRouter;
import com.database.atypon.Node.utils.response.Response;
import com.database.atypon.Node.utils.response.ResponseType;
import com.sun.net.httpserver.HttpServer;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.cumulative.CumulativeCounter;
import io.micrometer.core.instrument.distribution.DistributionStatisticConfig;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.test.context.TestPropertySource;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * M12: the domain meters that tell the write / read / consistency story, recorded at the existing quorum,
 * read-repair and reconcile seams. Each test drives the real controllers and services (a stub peer where one
 * is needed) against a {@link SimpleMeterRegistry} and reads the meters back by their catalogue names.
 *
 * <p>It shares the application context (and JWT secret) of the other integration tests: the read-repair
 * tests call stub peers with a service token, and {@code ServiceTokens} is a static bridge to whichever
 * context was created last, so a context with a different secret created mid-run would break the internal
 * calls of every cached context that runs after it. That context has no {@code MeterRegistry} bean (the
 * registry is wired in the application module), which is the situation {@link DbMetrics} must boot in.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestPropertySource(properties = "app.jwt.secret=dev-secret-change-me-please-at-least-32-bytes-long-000")
class DbMetricsTest {

    private static final String DB = "m12metrics";
    private static final String USERS = "users";
    private static final String WRITE_ACK = "[{\"responseType\":\"SUCCESS\",\"message\":\"stub applied\"}]";
    private static final String NOT_FOUND = "{\"responseType\":\"ERROR\",\"message\":\"Document not found\"}";

    private final Path db = Paths.get("./data/" + DB);
    private final Path records = db.resolve(USERS + "-records");

    @Autowired
    DbMetrics wiredMetrics;

    private List<Node> savedNodes;
    private Node savedSelf;
    private HashRing savedRing;

    private SimpleMeterRegistry registry;
    private DbMetrics metrics;
    private final List<Stub> stubs = new ArrayList<>();

    @BeforeEach
    void singleNodeWithAUsersSchema() throws Exception {
        savedNodes = Network.nodes;
        savedSelf = Network.self;
        savedRing = Network.ring;
        Network.nodes = Collections.synchronizedList(new ArrayList<>());
        Network.self = null;
        Network.ring = null;

        Files.createDirectories(db.resolve("schemas"));
        Files.createDirectories(records);
        Files.writeString(db.resolve("schemas").resolve(USERS + ".json"),
                new JSONObject().put("info", new JSONObject().put("schemaName", USERS).put("nextId", 0))
                        .put("schema", new JSONObject().put("Age", "Integer")).toString());

        registry = new SimpleMeterRegistry();
        metrics = new DbMetrics(registry);
    }

    @AfterEach
    void restoreNetworkAndCleanUp() throws Exception {
        SecurityContextHolder.clearContext();
        for (Stub stub : stubs) {
            stub.server.stop(0);
        }
        Network.nodes = savedNodes;
        Network.self = savedSelf;
        Network.ring = savedRing;
        if (Files.exists(db)) {
            try (var paths = Files.walk(db)) {
                paths.sorted((a, b) -> b.compareTo(a)).forEach(p -> p.toFile().delete());
            }
        }
    }

    // ---- wiring helpers ----------------------------------------------------------------------------------

    private WriteOperation writeOperation(DbMetrics m) {
        return new WriteOperation(mock(IndexManager.class), m);
    }

    private WriteController writeController(DbMetrics m) {
        return new WriteController(new WriteService(writeOperation(m)), m);
    }

    private static HashMap<String, Object> user(int age) {
        HashMap<String, Object> document = new HashMap<>();
        document.put("Age", age);
        return document;
    }

    private static void authenticateAs(String role) {
        SecurityContextHolder.getContext().setAuthentication(
                new UsernamePasswordAuthenticationToken("u", "token", JwtService.authorities(role)));
    }

    private static void assertSuccess(Vector<Response> out) {
        assertThat(out.get(0).getResponseType()).as(out.get(0).getMessage()).isEqualTo(ResponseType.SUCCESS);
    }

    private double count(String name, String... tags) {
        return count(registry, name, tags);
    }

    /** The sum over every counter of that name carrying the given tags (all of them when none are given). */
    private static double count(MeterRegistry from, String name, String... tags) {
        return from.find(name).tags(tags).counters().stream().mapToDouble(Counter::count).sum();
    }

    private DistributionSummary fanout() {
        return registry.find("db_write_replicas").summary();
    }

    private Path storedDocument(String id) {
        return records.resolve(id + ".json");
    }

    private void store(String id, JSONObject doc) throws IOException {
        Files.writeString(storedDocument(id), doc.toString());
    }

    private static JSONObject live(int age, int version, String clockNode, int clock, long ts) {
        return new JSONObject().put("Age", age).put("_version", version)
                .put("_vclock", new JSONObject().put(clockNode, clock)).put("_ts", ts);
    }

    private static JSONObject tombstone(int version, String clockNode, int clock, long ts) {
        return new JSONObject().put("_deleted", true).put("_version", version)
                .put("_vclock", new JSONObject().put(clockNode, clock)).put("_ts", ts);
    }

    /** A cluster of this node, Node0, and the given peers; every schema is then held by all of them. */
    private void clusterOf(Node... peers) {
        Network.self = new Node("Node0", "8080");
        Network.nodes = Collections.synchronizedList(new ArrayList<>(List.of(peers)));
        Network.rebuildRing();
    }

    private static String schemaWhere(java.util.function.Predicate<String> predicate) {
        for (int i = 0; i < 500; i++) {
            String schema = "s" + i;
            if (predicate.test(schema)) {
                return schema;
            }
        }
        throw new AssertionError("no schema name satisfies the placement the test needs");
    }

    private String schemaWhosePrimaryIs(String node) {
        return schemaWhere(s -> RingRouter.primaryFor(DB, s).getName().equals(node));
    }

    private String schemaWhosePrimaryIsSelf() {
        return schemaWhere(s -> RingRouter.isPrimary(DB, s));
    }

    // ---- writes: db_write_total{result,type,origin} + db_write_replicas ---------------------------------

    @Test
    void aLocalCreateCountsASuccessfulLocalCreate() {
        Vector<Response> out = writeController(metrics).createDocument(DB, USERS, user(30));

        assertSuccess(out);
        assertThat(count("db_write_total", "result", "success", "type", "create", "origin", "local")).isEqualTo(1);
        assertThat(count("db_write_total", "result", "quorum_failed")).isZero();
        assertThat(count("db_write_total", "origin", "forwarded")).isZero();
    }

    @Test
    void aLocalUpdateAndDeleteAreCountedByType() {
        WriteController controller = writeController(metrics);
        assertSuccess(controller.createDocument(DB, USERS, user(30)));

        HashMap<String, Object> changed = user(31);
        changed.put("_version", 1);
        assertSuccess(controller.updateDocument(DB, USERS, "0", changed));
        assertSuccess(controller.deleteDocument(DB, USERS, "0", 2));

        assertThat(count("db_write_total", "result", "success", "type", "create", "origin", "local")).isEqualTo(1);
        assertThat(count("db_write_total", "result", "success", "type", "update", "origin", "local")).isEqualTo(1);
        assertThat(count("db_write_total", "result", "success", "type", "delete", "origin", "local")).isEqualTo(1);
    }

    @Test
    void theFanOutSizeIsRecordedPerWriteAndIsZeroOnALoneNode() {
        writeController(metrics).createDocument(DB, USERS, user(30));

        assertThat(fanout().count()).isEqualTo(1);
        assertThat(fanout().totalAmount()).isZero();
    }

    @Test
    void aWriteThatMissedItsQuorumCountsAsQuorumFailedAndRecordsItsFanOut() {
        clusterOf(new Node("Node1", "8080"), new Node("Node2", "8080"));
        String schema = schemaWhosePrimaryIsSelf();
        WriteService writeService = mock(WriteService.class);
        when(writeService.createDocument(any(), any(), any()))
                .thenReturn(new Response(ResponseType.SUCCESS, "Document created successfully"));
        // both replicas fail to persist it: 1 ack (this node) against W = 2 of N = 3
        when(writeService.broadcastDocument(any(), any(), any())).thenReturn(List.of(
                new Response(ResponseType.ERROR, "down"), new Response(ResponseType.ERROR, "down")));

        Vector<Response> out = new WriteController(writeService, metrics).createDocument(DB, schema, user(30));

        assertThat(out.get(0).getResponseType()).isEqualTo(ResponseType.ERROR);
        assertThat(count("db_write_total", "result", "quorum_failed", "type", "create", "origin", "local"))
                .isEqualTo(1);
        assertThat(count("db_write_total", "result", "success")).isZero();
        assertThat(fanout().count()).isEqualTo(1);
        assertThat(fanout().totalAmount()).isEqualTo(2);
    }

    @Test
    void aWriteAcknowledgedByAQuorumOfAReplicatedSchemaCountsAsSuccessWithItsFanOut() {
        clusterOf(new Node("Node1", "8080"), new Node("Node2", "8080"));
        String schema = schemaWhosePrimaryIsSelf();
        WriteService writeService = mock(WriteService.class);
        when(writeService.createDocument(any(), any(), any()))
                .thenReturn(new Response(ResponseType.SUCCESS, "Document created successfully"));
        // one replica acks, one is down: 2 acks against W = 2
        when(writeService.broadcastDocument(any(), any(), any())).thenReturn(List.of(
                new Response(ResponseType.SUCCESS, "ok"), new Response(ResponseType.ERROR, "down")));

        new WriteController(writeService, metrics).createDocument(DB, schema, user(30));

        assertThat(count("db_write_total", "result", "success", "type", "create", "origin", "local")).isEqualTo(1);
        assertThat(count("db_write_total", "result", "quorum_failed")).isZero();
        assertThat(fanout().totalAmount()).isEqualTo(2);
    }

    @Test
    void aWriteTheLocalApplyRejectedIsNeitherASuccessNorAQuorumFailure() {
        WriteController controller = writeController(metrics);
        assertSuccess(controller.createDocument(DB, USERS, user(30)));

        HashMap<String, Object> stale = user(31);
        stale.put("_version", 9); // the stored document is at version 1
        Vector<Response> out = controller.updateDocument(DB, USERS, "0", stale);

        assertThat(out.get(0).getResponseType()).isEqualTo(ResponseType.ERROR);
        assertThat(count("db_write_total", "type", "update")).isZero();
        assertThat(fanout().count()).as("only the create fanned out").isEqualTo(1);
    }

    @Test
    void aReplicaApplyingAPeersWriteIsNotACountedClientWrite() {
        authenticateAs("INTERNAL");
        HashMap<String, Object> replicated = user(30);

        Vector<Response> out = writeController(metrics).createDocument(DB, USERS, replicated);

        assertSuccess(out);
        assertThat(registry.find("db_write_total").counters()).allSatisfy(c -> assertThat(c.count()).isZero());
        assertThat(fanout().count()).isZero();
    }

    @Test
    void aWriteHandedToThePrimaryIsCountedAsForwardedWithThePrimarysVerdict() throws Exception {
        Stub primary = stub("localhost");
        clusterOf(primary.node);
        String schema = schemaWhosePrimaryIs(primary.node.getName());
        primary.postReply.set("[{\"responseType\":\"SUCCESS\",\"message\":\"stub applied\"}]");

        Vector<Response> out = writeController(metrics).createDocument(DB, schema, user(30));

        assertSuccess(out);
        assertThat(count("db_write_total", "result", "success", "type", "create", "origin", "forwarded"))
                .isEqualTo(1);
        assertThat(count("db_write_total", "origin", "local")).as("nothing was applied here").isZero();
        assertThat(fanout().count()).as("the primary fans out, not this node").isZero();
    }

    @Test
    void aForwardedWriteTheRemotePrimaryFailedToQuorumIsCountedAsQuorumFailed() throws Exception {
        Stub primary = stub("localhost");
        clusterOf(primary.node);
        String schema = schemaWhosePrimaryIs(primary.node.getName());
        primary.postReply.set("[{\"responseType\":\"ERROR\",\"message\":\"write quorum not met (1/2)\"}]");

        writeController(metrics).deleteDocument(DB, schema, "0", 1);

        assertThat(count("db_write_total", "result", "quorum_failed", "type", "delete", "origin", "forwarded"))
                .isEqualTo(1);
    }

    @Test
    void aForwardedWriteTheRemotePrimaryRejectedForAnotherReasonIsNotCounted() throws Exception {
        Stub primary = stub("localhost");
        clusterOf(primary.node);
        String schema = schemaWhosePrimaryIs(primary.node.getName());
        primary.postReply.set("[{\"responseType\":\"ERROR\",\"message\":\"Version conflict: document is at version 4\"}]");

        writeController(metrics).updateDocument(DB, schema, "0", user(30));

        assertThat(registry.find("db_write_total").counters()).allSatisfy(c -> assertThat(c.count()).isZero());
    }

    // ---- reads: db_read_total{path}, read-repair ----------------------------------------------------------

    @Test
    void aQuorumReadByIdIsCountedWhetherOrNotTheDocumentExists() throws Exception {
        store("0", live(30, 1, "A", 1, 10));
        QuorumReadService reads = quorumReads(metrics);

        assertThat(reads.readById(DB, USERS, "0").getResponseType()).isEqualTo(ResponseType.SUCCESS);
        assertThat(reads.readById(DB, USERS, "7").getResponseType()).isEqualTo(ResponseType.ERROR);

        assertThat(count("db_read_total", "path", "by_id")).isEqualTo(2);
        assertThat(count("db_read_total", "path", "all")).isZero();
        assertThat(count("db_read_total", "path", "index")).isZero();
    }

    @Test
    void aReadRepairCountsOnceAndCountsEveryReplicaItRepaired() throws Exception {
        Stub first = stub("localhost");
        Stub second = stub("127.0.0.1");
        clusterOf(first.node, second.node);
        store("0", live(31, 2, "A", 2, 20)); // this node holds the newest copy
        first.holds(live(30, 1, "A", 1, 10)); // both peers are one write behind
        second.holds(live(30, 1, "A", 1, 10));

        Response read = quorumReads(metrics).readById(DB, USERS, "0");

        assertThat(read.getResponseType()).isEqualTo(ResponseType.SUCCESS);
        assertThat(count("db_read_repair_total")).isEqualTo(1);
        assertThat(count("db_read_repair_docs_total")).isEqualTo(2);
        assertThat(count("db_read_total", "path", "by_id")).isEqualTo(1);
    }

    @Test
    void aRepairThatNoReplicaAcceptedIsTriggeredButRepairedNothing() throws Exception {
        Stub first = stub("localhost");
        clusterOf(first.node);
        store("0", live(31, 2, "A", 2, 20));
        first.holds(live(30, 1, "A", 1, 10));
        first.postStatus.set(500); // the peer refuses the repair

        quorumReads(metrics).readById(DB, USERS, "0");

        assertThat(count("db_read_repair_total")).isEqualTo(1);
        assertThat(count("db_read_repair_docs_total")).isZero();
    }

    @Test
    void aReadOfReplicasThatAgreeTriggersNoRepair() throws Exception {
        Stub first = stub("localhost");
        clusterOf(first.node);
        JSONObject agreed = live(30, 1, "A", 1, 10);
        store("0", agreed);
        first.holds(agreed);

        quorumReads(metrics).readById(DB, USERS, "0");

        assertThat(count("db_read_repair_total")).isZero();
        assertThat(count("db_read_repair_docs_total")).isZero();
        assertThat(count("db_read_total", "path", "by_id")).isEqualTo(1);
    }

    @Test
    void repairingThisNodesStaleCopyCountsADescendsReconcile() throws Exception {
        Stub first = stub("localhost");
        clusterOf(first.node);
        store("0", live(30, 1, "A", 1, 10)); // this node is behind
        first.holds(live(31, 2, "A", 2, 20));

        quorumReads(metrics).readById(DB, USERS, "0");

        assertThat(new JSONObject(Files.readString(storedDocument("0"))).getInt("Age")).isEqualTo(31);
        assertThat(count("db_read_repair_total")).isEqualTo(1);
        assertThat(count("db_read_repair_docs_total")).isEqualTo(1);
        assertThat(count("db_reconcile_conflicts_total", "outcome", "descends")).isEqualTo(1);
        assertThat(count("db_reconcile_conflicts_total", "outcome", "concurrent")).isZero();
    }

    @Test
    void readingAllDocumentsAndQueryingAnIndexAreCountedByPath() throws Exception {
        store("0", live(30, 1, "A", 1, 10));
        ReadController reads = new ReadController(new ReadService(null), quorumReads(metrics), metrics);
        IndexManager indexManager = mock(IndexManager.class);
        when(indexManager.query(any(), any(), any(), any(), any(), any(), anyBoolean(), anyInt(), anyInt()))
                .thenReturn(List.of(0));
        IndexController indexes = new IndexController(indexManager, metrics);

        assertThat(reads.fetchAll(DB, USERS).getResponseType()).isEqualTo(ResponseType.SUCCESS);
        HashMap<String, Object> query = new HashMap<>();
        query.put("database", DB);
        query.put("schema", USERS);
        query.put("field", "Age");
        query.put("op", BPlusTree.Op.GTE.name());
        query.put("value", 25);
        assertThat(indexes.query(query).getResponseType()).isEqualTo(ResponseType.SUCCESS);

        assertThat(count("db_read_total", "path", "all")).isEqualTo(1);
        assertThat(count("db_read_total", "path", "index")).isEqualTo(1);
        assertThat(count("db_read_total", "path", "by_id")).isZero();
    }

    @Test
    void aReadOfAMissingSchemaIsNotCountedAsAnAllRead() {
        ReadController reads = new ReadController(new ReadService(null), quorumReads(metrics), metrics);

        assertThat(reads.fetchAll(DB, "nothing-here").getResponseType()).isEqualTo(ResponseType.ERROR);

        assertThat(count("db_read_total", "path", "all")).isZero();
    }

    // ---- consistency: reconcile outcomes and tombstones ---------------------------------------------------

    @Test
    void aConcurrentReplicatedCopyCountsAConcurrentConflict() throws Exception {
        store("0", live(30, 1, "A", 1, 10));
        WriteOperation writeOp = writeOperation(metrics);

        Response applied = writeOp.applyUpdate(DB, USERS, "0", live(40, 1, "B", 1, 20));

        assertThat(applied.getResponseType()).isEqualTo(ResponseType.SUCCESS);
        assertThat(count("db_reconcile_conflicts_total", "outcome", "concurrent")).isEqualTo(1);
        assertThat(count("db_reconcile_conflicts_total", "outcome", "descends")).isZero();
    }

    @Test
    void aCopyThatCausallyFollowsOrPrecedesTheStoredOneCountsADescendsOutcome() throws Exception {
        store("0", live(30, 1, "A", 2, 10));
        WriteOperation writeOp = writeOperation(metrics);

        writeOp.applyUpdate(DB, USERS, "0", live(31, 2, "A", 3, 20)); // newer: replaces the stored copy
        writeOp.applyUpdate(DB, USERS, "0", live(29, 1, "A", 1, 5));  // older: the stored copy is kept

        assertThat(count("db_reconcile_conflicts_total", "outcome", "descends")).isEqualTo(2);
        assertThat(count("db_reconcile_conflicts_total", "outcome", "concurrent")).isZero();
    }

    @Test
    void anIdenticalOrAnAbsentCopyIsNotAConflict() throws Exception {
        JSONObject same = live(30, 1, "A", 1, 10);
        store("0", same);
        WriteOperation writeOp = writeOperation(metrics);

        writeOp.applyUpdate(DB, USERS, "0", new JSONObject(same.toString())); // already current
        writeOp.applyUpdate(DB, USERS, "5", live(30, 1, "A", 1, 10));         // nothing stored: healed

        assertThat(registry.find("db_reconcile_conflicts_total").counters())
                .allSatisfy(c -> assertThat(c.count()).isZero());
        assertThat(Files.exists(storedDocument("5"))).isTrue();
    }

    @Test
    void aDeleteWritesATombstoneAndCountsIt() throws Exception {
        WriteController controller = writeController(metrics);
        assertSuccess(controller.createDocument(DB, USERS, user(30)));
        assertThat(count("db_tombstone_written_total")).isZero();

        assertSuccess(controller.deleteDocument(DB, USERS, "0", 1));

        assertThat(new JSONObject(Files.readString(storedDocument("0"))).optBoolean("_deleted")).isTrue();
        assertThat(count("db_tombstone_written_total")).isEqualTo(1);
    }

    @Test
    void aDeleteThatFindsNothingToDeleteWritesNoTombstone() {
        Vector<Response> out = writeController(metrics).deleteDocument(DB, USERS, "9", 1);

        assertThat(out.get(0).getResponseType()).isEqualTo(ResponseType.ERROR);
        assertThat(count("db_tombstone_written_total")).isZero();
    }

    @Test
    void aTombstoneAppliedFromAPeerCountsWhenStoredButNotWhenAlreadyHeld() throws Exception {
        store("0", live(30, 1, "A", 1, 10));
        WriteOperation writeOp = writeOperation(metrics);
        JSONObject deleted = tombstone(2, "A", 2, 20);

        writeOp.applyUpdate(DB, USERS, "0", deleted);
        writeOp.applyUpdate(DB, USERS, "0", new JSONObject(deleted.toString())); // the same delete again

        assertThat(count("db_tombstone_written_total")).isEqualTo(1);
    }

    // ---- a metrics failure never breaks a request ---------------------------------------------------------

    @Test
    void meterFailureDoesNotBreakTheWrite() throws Exception {
        DbMetrics brokenAtRegistration = new DbMetrics(new RegistryThatCannotRegister());
        DbMetrics brokenAtIncrement = new DbMetrics(new RegistryWhoseCountersThrow());

        for (DbMetrics broken : List.of(brokenAtRegistration, brokenAtIncrement)) {
            WriteController controller = writeController(broken);

            assertSuccess(controller.createDocument(DB, USERS, user(30)));
            HashMap<String, Object> changed = user(31);
            changed.put("_version", 1);
            assertSuccess(controller.updateDocument(DB, USERS, "0", changed));
            assertSuccess(controller.deleteDocument(DB, USERS, "0", 2));
            assertThat(new JSONObject(Files.readString(storedDocument("0"))).optBoolean("_deleted")).isTrue();

            // reset the schema so the next metrics instance starts from an empty collection
            Files.delete(storedDocument("0"));
            Files.writeString(db.resolve("schemas").resolve(USERS + ".json"),
                    new JSONObject().put("info", new JSONObject().put("schemaName", USERS).put("nextId", 0))
                            .put("schema", new JSONObject().put("Age", "Integer")).toString());
        }
    }

    @Test
    void meterFailureDoesNotBreakAReadOrARepair() throws Exception {
        Stub first = stub("localhost");
        clusterOf(first.node);
        store("0", live(30, 1, "A", 1, 10));
        first.holds(live(31, 2, "A", 2, 20));
        DbMetrics broken = new DbMetrics(new RegistryWhoseCountersThrow());

        Response read = quorumReads(broken).readById(DB, USERS, "0");

        assertThat(read.getResponseType()).isEqualTo(ResponseType.SUCCESS);
        assertThat(new JSONObject(read.getContent().toString()).getInt("Age")).isEqualTo(31);
        assertThat(new JSONObject(Files.readString(storedDocument("0"))).getInt("Age")).as("still repaired")
                .isEqualTo(31);
    }

    @Test
    void safeSwallowsAnythingThrown() {
        DbMetrics.safe(() -> {
            throw new IllegalStateException("registry down");
        });
        DbMetrics.safe(() -> {
            throw new AssertionError("even an Error");
        });
        DbMetrics.safe(() -> {
            throw new StackOverflowError();
        });
        DbMetrics.safe(null);

        AtomicReference<String> ran = new AtomicReference<>();
        DbMetrics.safe(() -> ran.set("ran"));
        assertThat(ran.get()).isEqualTo("ran");
    }

    // ---- the registry the meters land in -------------------------------------------------------------------

    @Test
    void withoutARegistryInTheContextMetersStillRecordIntoAFallback() {
        DbMetrics fallback = new DbMetrics(new DefaultListableBeanFactory().getBeanProvider(MeterRegistry.class));

        fallback.recordWrite(DbMetrics.WriteResult.SUCCESS, DbMetrics.WriteType.CREATE, DbMetrics.WriteOrigin.LOCAL);

        assertThat(fallback.registry()).isNotNull();
        assertThat(count(fallback.registry(), "db_write_total", "result", "success", "type", "create", "origin",
                "local")).isEqualTo(1);
    }

    @Test
    void theRegistryBeanIsUsedWhenOneIsPresent() {
        DefaultListableBeanFactory beans = new DefaultListableBeanFactory();
        beans.registerSingleton("meterRegistry", registry);

        DbMetrics wired = new DbMetrics(beans.getBeanProvider(MeterRegistry.class));
        wired.recordTombstoneWritten();

        assertThat(wired.registry()).isSameAs(registry);
        assertThat(count("db_tombstone_written_total")).isEqualTo(1);
    }

    @Test
    void theNodeContextBootsTheComponentWithoutAMeterRegistryBean() {
        assertThat(wiredMetrics).isNotNull();
        assertThat(wiredMetrics.registry()).isInstanceOf(SimpleMeterRegistry.class);
    }

    @Test
    void isAComponentSpringBootsWithOrWithoutAMeterRegistry() {
        try (AnnotationConfigApplicationContext bare = new AnnotationConfigApplicationContext(DbMetrics.class)) {
            assertThat(bare.getBean(DbMetrics.class).registry()).isNotNull();
        }
        try (AnnotationConfigApplicationContext withRegistry = new AnnotationConfigApplicationContext()) {
            withRegistry.registerBean(MeterRegistry.class, () -> registry);
            withRegistry.register(DbMetrics.class);
            withRegistry.refresh();
            assertThat(withRegistry.getBean(DbMetrics.class).registry()).isSameAs(registry);
        }
    }

    // ---- the catalogue ------------------------------------------------------------------------------------

    @Test
    void registersExactlyTheCataloguedNamesTagsAndValuesAndNeverTheReservedTags() {
        Set<String> names = registry.getMeters().stream().map(m -> m.getId().getName()).collect(Collectors.toSet());
        assertThat(names).containsExactlyInAnyOrder(
                "db_write_total", "db_write_replicas", "db_read_total", "db_read_repair_total",
                "db_read_repair_docs_total", "db_reconcile_conflicts_total", "db_tombstone_written_total");

        assertThat(tagValues("db_write_total")).containsExactlyInAnyOrder(
                "origin=forwarded|result=quorum_failed|type=create", "origin=forwarded|result=quorum_failed|type=delete",
                "origin=forwarded|result=quorum_failed|type=update", "origin=forwarded|result=success|type=create",
                "origin=forwarded|result=success|type=delete", "origin=forwarded|result=success|type=update",
                "origin=local|result=quorum_failed|type=create", "origin=local|result=quorum_failed|type=delete",
                "origin=local|result=quorum_failed|type=update", "origin=local|result=success|type=create",
                "origin=local|result=success|type=delete", "origin=local|result=success|type=update");
        assertThat(tagValues("db_read_total")).containsExactlyInAnyOrder("path=by_id", "path=all", "path=index");
        assertThat(tagValues("db_reconcile_conflicts_total")).containsExactlyInAnyOrder(
                "outcome=descends", "outcome=concurrent");
        assertThat(tagValues("db_write_replicas")).containsExactly("");
        assertThat(tagValues("db_read_repair_total")).containsExactly("");
        assertThat(tagValues("db_read_repair_docs_total")).containsExactly("");
        assertThat(tagValues("db_tombstone_written_total")).containsExactly("");

        for (Meter meter : registry.getMeters()) {
            assertThat(meter.getId().getTags()).extracting(Tag::getKey)
                    .as("%s must not use a reserved common tag", meter.getId().getName())
                    .doesNotContain("role", "node");
        }
        assertThat(registry.find("db_write_replicas").summary()).isNotNull();
    }

    private Set<String> tagValues(String name) {
        Set<String> combos = new HashSet<>();
        for (Meter meter : registry.find(name).meters()) {
            combos.add(meter.getId().getTags().stream().sorted()
                    .map(t -> t.getKey() + "=" + t.getValue()).collect(Collectors.joining("|")));
        }
        return combos;
    }

    // ---- fixtures ------------------------------------------------------------------------------------------

    private QuorumReadService quorumReads(DbMetrics m) {
        return new QuorumReadService(new ReadService(null), new WriteService(writeOperation(m)), m);
    }

    private Stub stub(String host) throws IOException {
        Stub stub = new Stub(host);
        stubs.add(stub);
        return stub;
    }

    /** A registry that fails the moment a meter is registered, as a broken exporter would. */
    private static final class RegistryThatCannotRegister extends SimpleMeterRegistry {
        @Override
        protected Counter newCounter(Meter.Id id) {
            throw new IllegalStateException("registry down");
        }

        @Override
        protected DistributionSummary newDistributionSummary(Meter.Id id, DistributionStatisticConfig config,
                                                             double scale) {
            throw new IllegalStateException("registry down");
        }
    }

    /** A registry whose counters register fine and then throw on every increment. */
    private static final class RegistryWhoseCountersThrow extends SimpleMeterRegistry {
        @Override
        protected Counter newCounter(Meter.Id id) {
            return new CumulativeCounter(id) {
                @Override
                public void increment(double amount) {
                    throw new IllegalStateException("counter down");
                }
            };
        }

        @Override
        protected DistributionSummary newDistributionSummary(Meter.Id id, DistributionStatisticConfig config,
                                                             double scale) {
            throw new IllegalStateException("summary down");
        }
    }

    /** A peer: answers a GET with the document it was told to hold and a POST with what the test set. */
    private static final class Stub {
        final HttpServer server;
        final Node node;
        final AtomicReference<String> getReply = new AtomicReference<>(NOT_FOUND);
        final AtomicReference<String> postReply = new AtomicReference<>(WRITE_ACK);
        final AtomicReference<Integer> postStatus = new AtomicReference<>(200);

        Stub(String host) throws IOException {
            server = HttpServer.create(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0), 0);
            server.createContext("/", exchange -> {
                exchange.getRequestBody().readAllBytes();
                boolean post = "POST".equals(exchange.getRequestMethod());
                byte[] reply = (post ? postReply.get() : getReply.get()).getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().add("Content-Type", "application/json");
                exchange.sendResponseHeaders(post ? postStatus.get() : 200, reply.length);
                exchange.getResponseBody().write(reply);
                exchange.close();
            });
            server.start();
            // the ring keys members by name, so peers need different names that all reach loopback
            node = new Node(host, String.valueOf(server.getAddress().getPort()));
        }

        void holds(JSONObject doc) {
            getReply.set(new JSONObject()
                    .put("responseType", "SUCCESS")
                    .put("message", "Document fetched successfully")
                    .put("content", doc.toString()).toString());
        }
    }
}
