package com.database.atypon.Node.utils.swim;

import com.database.atypon.Node.model.Network;
import com.database.atypon.Node.model.Node;
import com.database.atypon.Node.operations.write.WriteOperation;
import com.database.atypon.Node.services.index.IndexManager;
import com.database.atypon.Node.services.read.ReadService;
import com.database.atypon.Node.utils.ring.HashRing;
import com.database.atypon.Node.utils.ring.RingRouter;
import com.database.atypon.Node.utils.response.ResponseType;
import com.database.atypon.Node.utils.vclock.Reconcile;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * State handoff: when a membership change makes this node a replica of a schema it does not (fully) hold,
 * it pulls the schema from a node that does and merges it causally through {@code applyReplicated}.
 *
 * <p>Peers are real {@link HttpServer} stubs on loopback that answer {@code /swim/schemas} and
 * {@code /swim/pull} in the wire form of {@code HandoffController}; the receiver is a real
 * {@link WriteOperation} on the real data directory (under a database of its own), with only the index
 * manager mocked. Every test saves and restores the {@link Network} statics it rewires.
 */
class HandoffServiceTest {

    private static final String BEARER = "Bearer handoff-test-token";
    private static final String DB = "handoffdb";
    private static final String SCHEMA = "users";
    private static final String KEY = DB + "." + SCHEMA;

    private final Path dbDir = Paths.get("./data", DB);
    private final Path records = dbDir.resolve("users-records");
    private final Path schemaFile = dbDir.resolve("schemas").resolve("users.json");

    private List<Node> savedNodes;
    private Node savedSelf;
    private HashRing savedRing;

    private final Node self = new Node("handoff-self", "8080");
    private final List<Peer> peers = new ArrayList<>();
    private IndexManager indexManager;
    private WriteOperation writeOp;
    private ReadService reads;

    // ---- fixtures -----------------------------------------------------------------------------

    /** A peer that serves what it was told to, and records every request it gets. */
    private static final class Peer {
        final HttpServer server;
        final Node node;
        final List<String> requests = Collections.synchronizedList(new ArrayList<>());
        final List<String> authorizations = Collections.synchronizedList(new ArrayList<>());
        final Map<String, String> pullBodies = new ConcurrentHashMap<>();
        volatile List<String> listing = new ArrayList<>();
        volatile int schemasStatus = 200;
        volatile int pullStatus = 200;

        Peer(String host) throws IOException {
            server = HttpServer.create(new InetSocketAddress(InetAddress.getByName(host), 0), 0);
            node = new Node(host, String.valueOf(server.getAddress().getPort()));
            server.createContext("/swim/schemas", exchange -> {
                record(exchange);
                reply(exchange, schemasStatus, new JSONArray(listing).toString());
            });
            server.createContext("/swim/pull", exchange -> {
                record(exchange);
                String body = pullBodies.get(queryParam(exchange, "database") + "." + queryParam(exchange, "schema"));
                if (pullStatus != 200) {
                    reply(exchange, pullStatus, "{}");
                } else if (body == null) {
                    reply(exchange, 404, "{}");
                } else {
                    reply(exchange, 200, body);
                }
            });
            server.start();
        }

        /** Makes this peer advertise {@code key} and serve {@code docs} (id to document) when asked for it. */
        Peer offer(String key, JSONObject definition, Map<String, JSONObject> docs) {
            String[] parts = key.split("\\.", 2);
            JSONObject documents = new JSONObject();
            docs.forEach(documents::put);
            pullBodies.put(key, new JSONObject()
                    .put("database", parts[0]).put("schema", parts[1])
                    .put("definition", definition == null ? JSONObject.NULL : definition)
                    .put("documents", documents).toString());
            List<String> advertised = new ArrayList<>(listing);
            if (!advertised.contains(key)) {
                advertised.add(key);
            }
            listing = advertised;
            return this;
        }

        List<String> pulls() {
            synchronized (requests) {
                return requests.stream().filter(r -> r.startsWith("/swim/pull")).toList();
            }
        }

        List<String> listings() {
            synchronized (requests) {
                return requests.stream().filter(r -> r.startsWith("/swim/schemas")).toList();
            }
        }

        private void record(HttpExchange exchange) {
            requests.add(exchange.getRequestURI().getRawPath()
                    + (exchange.getRequestURI().getRawQuery() == null ? "" : "?" + exchange.getRequestURI().getRawQuery()));
            authorizations.add(exchange.getRequestHeaders().getFirst("Authorization"));
        }

        private static String queryParam(HttpExchange exchange, String name) {
            String query = exchange.getRequestURI().getRawQuery();
            if (query != null) {
                for (String pair : query.split("&")) {
                    if (pair.startsWith(name + "=")) {
                        return pair.substring(name.length() + 1);
                    }
                }
            }
            return "";
        }

        private static void reply(HttpExchange exchange, int status, String body) throws IOException {
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(status, bytes.length);
            exchange.getResponseBody().write(bytes);
            exchange.close();
        }
    }

    private Peer peer(String host) throws IOException {
        Peer peer = new Peer(host);
        peers.add(peer);
        return peer;
    }

    /** The cluster as this node sees it: itself and the given peers, ring included. */
    private void clusterOf(Peer... members) {
        List<Node> live = new ArrayList<>();
        live.add(self);
        for (Peer member : members) {
            live.add(member.node);
        }
        Network.setLiveMembers(live);
    }

    @BeforeEach
    void setUp() throws Exception {
        savedNodes = Network.nodes;
        savedSelf = Network.self;
        savedRing = Network.ring;
        Network.nodes = Collections.synchronizedList(new ArrayList<>());
        Network.self = self;
        Network.ring = null;

        deleteTree(dbDir);
        indexManager = mock(IndexManager.class);
        writeOp = new WriteOperation(indexManager);
        reads = new ReadService(null);
    }

    @AfterEach
    void tearDown() throws Exception {
        peers.forEach(p -> p.server.stop(0));
        deleteTree(dbDir);
        Network.nodes = savedNodes;
        Network.self = savedSelf;
        Network.ring = savedRing;
    }

    private HandoffService service() {
        return new HandoffService(new MembershipTable(), writeOp, () -> BEARER, 2_000L, Runnable::run);
    }

    private static JSONObject definition(int nextId) {
        return new JSONObject()
                .put("info", new JSONObject().put("schemaName", SCHEMA).put("nextId", nextId))
                .put("schema", new JSONObject().put("Name", "String").put("Age", "Integer"));
    }

    /** A live document at {@code version}, whose clock is the single entry {@code node:version}. */
    private static JSONObject doc(String name, int age, int version, String node, long ts) {
        return new JSONObject().put("Name", name).put("Age", age).put("_version", version)
                .put("_vclock", new JSONObject().put(node, version)).put("_ts", ts);
    }

    private static JSONObject tombstone(int version, String node, long ts) {
        return new JSONObject().put("_deleted", true).put("_version", version)
                .put("_vclock", new JSONObject().put(node, version)).put("_ts", ts);
    }

    private static Map<String, JSONObject> docs(Object... idsAndDocs) {
        Map<String, JSONObject> docs = new LinkedHashMap<>();
        for (int i = 0; i < idsAndDocs.length; i += 2) {
            docs.put((String) idsAndDocs[i], (JSONObject) idsAndDocs[i + 1]);
        }
        return docs;
    }

    /** This node already holds the schema, with {@code localDocs} (id to document) on disk. */
    private void seedReceiver(int nextId, Map<String, JSONObject> localDocs) throws Exception {
        Files.createDirectories(schemaFile.getParent());
        Files.createDirectories(records);
        Files.writeString(schemaFile, definition(nextId).toString());
        for (Map.Entry<String, JSONObject> entry : localDocs.entrySet()) {
            Files.writeString(records.resolve(entry.getKey() + ".json"), entry.getValue().toString());
        }
    }

    private JSONObject stored(String id) throws Exception {
        return new JSONObject(Files.readString(records.resolve(id + ".json")));
    }

    private static void assertSame(JSONObject actual, JSONObject expected) {
        assertThat(actual.similar(expected)).as("%s should equal %s", actual, expected).isTrue();
    }

    private static void deleteTree(Path root) throws IOException {
        if (Files.exists(root)) {
            try (var paths = Files.walk(root)) {
                paths.sorted((a, b) -> b.compareTo(a)).forEach(p -> p.toFile().delete());
            }
        }
    }

    // ---- the handoff --------------------------------------------------------------------------

    @Test
    void handoffPullsNewlyOwnedSchema() throws Exception {
        JSONObject first = doc("ann", 31, 2, "src", 2_000);
        JSONObject second = doc("bob", 40, 1, "src", 1_000);
        Peer source = peer("127.0.0.1").offer(KEY, definition(2), docs("0", first, "1", second));
        clusterOf(source); // this node has just joined: the ring now makes it a replica, and it holds nothing

        service().onChange();

        assertSame(stored("0"), first);
        assertSame(stored("1"), second);
        assertThat(source.pulls()).hasSize(1);
        assertThat(source.pulls().get(0)).isEqualTo("/swim/pull?database=" + DB + "&schema=" + SCHEMA);
        assertThat(source.authorizations).as("node-to-node calls carry the service token").containsOnly(BEARER);
        // the node can serve the schema from now on: its definition is here, and new ids start after the pulled ones
        JSONObject adopted = new JSONObject(Files.readString(schemaFile));
        assertThat(adopted.getJSONObject("schema").similar(definition(2).getJSONObject("schema"))).isTrue();
        assertThat(adopted.getJSONObject("info").getInt("nextId")).isEqualTo(2);
        assertThat(reads.fetchAll(DB, SCHEMA).getResponseType()).isEqualTo(ResponseType.SUCCESS);
    }

    @Test
    void handoffCarriesTombstone() throws Exception {
        JSONObject live = doc("ann", 31, 1, "src", 1_000);
        // this node holds an older live copy of document 1; the source has since deleted it, and deleted 2, which this node never saw
        seedReceiver(2, docs("1", doc("bob", 40, 1, "src", 1_000)));
        Peer source = peer("127.0.0.1").offer(KEY, definition(3), docs(
                "0", live, "1", tombstone(2, "src", 2_000), "2", tombstone(2, "src", 2_000)));
        clusterOf(source);

        service().onChange();

        assertSame(stored("0"), live);
        assertThat(Reconcile.isTombstone(stored("1"))).as("the delete reached the stale replica").isTrue();
        assertThat(Reconcile.isTombstone(stored("2"))).as("a delete is carried even to a replica that never held the document").isTrue();
        // read-by-id on this node: the raw copy is the tombstone, so the client is told "not found"; a listing never shows it
        assertThat(Reconcile.isTombstone(new JSONObject((String) reads.fetchById("1", DB, SCHEMA).getContent()))).isTrue();
        assertThat(String.valueOf(reads.fetchAll(DB, SCHEMA).getContent())).contains("0.json").doesNotContain("1.json").doesNotContain("2.json");
        verify(indexManager).onDelete(eq(DB), eq(SCHEMA), eq(1), any());
    }

    @Test
    void handoffDoesNotRegressNewerLocal() throws Exception {
        JSONObject newerLocal = doc("zed", 99, 5, "src", 5_000);
        JSONObject liveAgainstStaleDelete = doc("kim", 22, 6, "src", 6_000);
        seedReceiver(3, docs("0", newerLocal, "2", liveAgainstStaleDelete));
        String before0 = Files.readString(records.resolve("0.json"));
        String before2 = Files.readString(records.resolve("2.json"));
        Peer source = peer("127.0.0.1").offer(KEY, definition(3), docs(
                "0", doc("ann", 31, 2, "src", 2_000),    // an older version of what this node already has newer
                "1", doc("bob", 40, 1, "src", 1_000),    // something this node is genuinely missing
                "2", tombstone(2, "src", 2_000)));       // a delete this node's later write supersedes
        clusterOf(source);

        service().onChange();

        assertThat(Files.readString(records.resolve("0.json"))).as("newer local data is never clobbered").isEqualTo(before0);
        assertThat(Files.readString(records.resolve("2.json"))).as("a stale delete cannot kill a newer live copy").isEqualTo(before2);
        assertSame(stored("1"), doc("bob", 40, 1, "src", 1_000)); // the gap is filled
        verify(indexManager, never()).onUpdate(any(), any(), eq(0), any(), any());
        verify(indexManager, never()).onDelete(any(), any(), eq(2), any());
    }

    @Test
    void singleNodeDoesNoHandoff() throws Exception {
        Peer stranger = peer("127.0.0.1").offer(KEY, definition(1), docs("0", doc("ann", 31, 1, "src", 1_000)));
        clusterOf(); // this node alone: it owns everything, and there is nobody to pull from

        service().onChange();

        assertThat(stranger.requests).as("a single node asks nobody anything").isEmpty();
        assertThat(Files.exists(dbDir)).isFalse();
    }

    @Test
    void handoffIsIdempotent() throws Exception {
        JSONObject first = doc("ann", 31, 2, "src", 2_000);
        Peer source = peer("127.0.0.1").offer(KEY, definition(2), docs(
                "0", first, "1", tombstone(2, "src", 2_000)));
        clusterOf(source);
        HandoffService service = service();

        service.onChange();
        String file0 = Files.readString(records.resolve("0.json"));
        String file1 = Files.readString(records.resolve("1.json"));
        String schemaBefore = Files.readString(schemaFile);
        verify(indexManager, times(1)).onUpdate(eq(DB), eq(SCHEMA), eq(0), any(), any());
        verify(indexManager, times(1)).onDelete(eq(DB), eq(SCHEMA), eq(1), any());
        clearInvocations(indexManager);

        // the same service again: the schema already moved, so there is nothing left to move
        service.onChange();
        assertThat(source.pulls()).as("one pass per change moves only what is still moving").hasSize(1);

        // a fresh service (a restart, say) does pull again, and the causal apply finds nothing to write
        service().onChange();
        assertThat(source.pulls()).hasSize(2);
        assertThat(Files.readString(records.resolve("0.json"))).isEqualTo(file0);
        assertThat(Files.readString(records.resolve("1.json"))).isEqualTo(file1);
        assertThat(Files.readString(schemaFile)).isEqualTo(schemaBefore);
        verify(indexManager, never()).onUpdate(any(), any(), anyInt(), any(), any());
        verify(indexManager, never()).onDelete(any(), any(), anyInt(), any());
    }

    @Test
    void nextIdNeverMovesBackwardsWhenTheSchemaIsAlreadyHeld() throws Exception {
        seedReceiver(7, docs("0", doc("ann", 31, 1, "src", 1_000))); // this node's own counter is ahead of the source's
        Peer source = peer("127.0.0.1").offer(KEY, definition(3), docs("0", doc("ann", 31, 1, "src", 1_000)));
        clusterOf(source);

        service().onChange();

        assertThat(new JSONObject(Files.readString(schemaFile)).getJSONObject("info").getInt("nextId")).isEqualTo(7);
    }

    @Test
    void nextIdCoversEveryPulledDocumentEvenIfTheSourceCounterLags() throws Exception {
        Peer source = peer("127.0.0.1").offer(KEY, definition(1), docs(
                "0", doc("ann", 31, 1, "src", 1_000), "4", doc("bob", 40, 1, "src", 1_000)));
        clusterOf(source);

        service().onChange();

        assertThat(new JSONObject(Files.readString(schemaFile)).getJSONObject("info").getInt("nextId"))
                .as("a create here must not reuse the id of a document that was handed over").isEqualTo(5);
    }

    // ---- which schemas, from whom --------------------------------------------------------------

    /** A cluster of this node and four stubs (so the ring is wider than the replication factor). */
    private List<Peer> wideCluster() throws IOException {
        List<Peer> four = new ArrayList<>();
        for (int i = 1; i <= 4; i++) {
            four.add(peer("127.0.0." + i));
        }
        clusterOf(four.toArray(new Peer[0]));
        return four;
    }

    private static String schemaNamed(java.util.function.Predicate<String> accept) {
        for (int i = 0; i < 500; i++) {
            String name = "s" + i;
            if (accept.test(name)) {
                return name;
            }
        }
        return null;
    }

    @Test
    void aSchemaThisNodeDoesNotOwnIsNotPulled() throws Exception {
        List<Peer> four = wideCluster();
        String owned = schemaNamed(name -> RingRouter.isReplica(DB, name));
        String foreign = schemaNamed(name -> !RingRouter.isReplica(DB, name));
        Assumptions.assumeTrue(owned != null && foreign != null, "REPLICATION_FACTOR leaves no schema this node does not own");
        for (Peer p : four) {
            p.offer(DB + "." + owned, definition(1), docs("0", doc("ann", 31, 1, "src", 1_000)));
            p.offer(DB + "." + foreign, definition(1), docs("0", doc("ann", 31, 1, "src", 1_000)));
        }

        service().onChange();

        List<String> pulled = four.stream().flatMap(p -> p.pulls().stream()).toList();
        assertThat(pulled).hasSize(1);
        assertThat(pulled.get(0)).isEqualTo("/swim/pull?database=" + DB + "&schema=" + owned);
        assertThat(Files.exists(dbDir.resolve(foreign + "-records"))).isFalse();
    }

    @Test
    void theSourceIsARingReplicaInPreferenceToOtherHolders() throws Exception {
        List<Peer> four = wideCluster();
        String name = schemaNamed(n -> RingRouter.isReplica(DB, n)
                && !RingRouter.otherReplicas(DB, n).isEmpty() && RingRouter.otherReplicas(DB, n).size() < four.size());
        Assumptions.assumeTrue(name != null, "REPLICATION_FACTOR leaves no schema with a holder that is not a replica");
        List<String> replicas = RingRouter.otherReplicas(DB, name).stream().map(Node::getName).toList();
        for (Peer p : four) { // every peer holds the schema: the ring replicas, and the ones that are not replicas of it
            p.offer(DB + "." + name, definition(1), docs("0", doc("ann", 31, 1, "src", 1_000)));
        }
        // The holders that are not replicas come first in this node's peer list, so only a deliberate preference skips them.
        List<Node> ordered = new ArrayList<>();
        ordered.add(self);
        four.stream().filter(p -> !replicas.contains(p.node.getName())).forEach(p -> ordered.add(p.node));
        four.stream().filter(p -> replicas.contains(p.node.getName())).forEach(p -> ordered.add(p.node));
        Network.setLiveMembers(ordered);

        service().onChange();

        List<Peer> asked = four.stream().filter(p -> !p.pulls().isEmpty()).toList();
        assertThat(asked).hasSize(1);
        assertThat(asked.get(0).node.getName()).isEqualTo(replicas.get(0));
    }

    @Test
    void aFailedPullFallsBackToTheNextHolder() throws Exception {
        Peer a = peer("127.0.0.1").offer(KEY, definition(1), docs("0", doc("ann", 31, 1, "src", 1_000)));
        Peer b = peer("127.0.0.2").offer(KEY, definition(1), docs("0", doc("ann", 31, 1, "src", 1_000)));
        a.pullStatus = 500;
        clusterOf(a, b);

        service().onChange();

        assertSame(stored("0"), doc("ann", 31, 1, "src", 1_000));
        assertThat(b.pulls()).hasSize(1);
        assertThat(a.pulls().size()).isLessThanOrEqualTo(1);
    }

    @Test
    void anUnreachablePeerDoesNotStopTheHandoff() throws Exception {
        Peer down = peer("127.0.0.1");
        down.schemasStatus = 503;
        Peer up = peer("127.0.0.2").offer(KEY, definition(1), docs("0", doc("ann", 31, 1, "src", 1_000)));
        clusterOf(down, up);

        service().onChange();

        assertSame(stored("0"), doc("ann", 31, 1, "src", 1_000));
    }

    @Test
    void aSchemaThatCouldNotBePulledIsRetriedOnTheNextChange() throws Exception {
        Peer source = peer("127.0.0.1").offer(KEY, definition(1), docs("0", doc("ann", 31, 1, "src", 1_000)));
        source.pullStatus = 500;
        clusterOf(source);
        HandoffService service = service();

        service.onChange(); // must not throw, and must not pretend it moved the schema

        assertThat(Files.exists(records.resolve("0.json"))).isFalse();

        source.pullStatus = 200;
        service.onChange();

        assertSame(stored("0"), doc("ann", 31, 1, "src", 1_000));
    }

    @Test
    void aSchemaThisNodeNoLongerOwnsIsPulledAgainWhenItComesBack() throws Exception {
        List<Peer> four = wideCluster();
        String name = schemaNamed(n -> RingRouter.isReplica(DB, n));
        Assumptions.assumeTrue(name != null);
        four.get(0).offer(DB + "." + name, definition(1), docs("0", doc("ann", 31, 1, "src", 1_000)));
        HandoffService service = service();
        service.onChange();
        assertThat(four.get(0).pulls()).hasSize(1);

        // the ring changes so that this node is no longer a replica (it is on its own again), then it is back
        Network.setLiveMembers(List.of(self));
        service.onChange();
        clusterOf(four.toArray(new Peer[0]));
        service.onChange();

        assertThat(four.get(0).pulls()).as("having lost the schema, it is newly owned again").hasSize(2);
    }

    // ---- the hook ------------------------------------------------------------------------------

    @Test
    void aMembershipChangeTriggersTheHandoff() throws Exception {
        JSONObject first = doc("ann", 31, 2, "src", 2_000);
        Peer source = peer("127.0.0.1").offer(KEY, definition(1), docs("0", first));
        MembershipTable table = new MembershipTable();
        HandoffService service = new HandoffService(table, writeOp, () -> BEARER, 2_000L, Runnable::run);
        table.initSelf(self);
        service.start();
        assertThat(source.requests).as("nothing has changed yet").isEmpty();

        table.merge(new Member(source.node, MemberState.ALIVE, 0, 0L)); // a peer joins: the live set, and the ring, change

        assertSame(stored("0"), first);

        service.stop();
        Peer latecomer = peer("127.0.0.2");
        table.merge(new Member(latecomer.node, MemberState.ALIVE, 0, 0L)); // another change, after the service has stopped
        assertThat(latecomer.requests).as("a stopped service no longer listens").isEmpty();
    }

    @Test
    void changesAreCoalescedIntoOneQueuedPass() throws Exception {
        Peer source = peer("127.0.0.1").offer(KEY, definition(1), docs("0", doc("ann", 31, 1, "src", 1_000)));
        List<Runnable> queued = new ArrayList<>();
        Executor manual = queued::add;
        MembershipTable table = new MembershipTable();
        HandoffService service = new HandoffService(table, writeOp, () -> BEARER, 2_000L, manual);
        table.initSelf(self);
        service.start();
        queued.clear();

        table.merge(new Member(source.node, MemberState.ALIVE, 0, 0L));
        table.merge(new Member(peer("127.0.0.2").node, MemberState.ALIVE, 0, 0L));
        table.merge(new Member(peer("127.0.0.3").node, MemberState.ALIVE, 0, 0L));

        assertThat(queued).as("three changes before the pass runs ask for one pass, not three").hasSize(1);
        assertThat(source.requests).as("the gossip thread that changed the table never ran the pull").isEmpty();

        queued.remove(0).run();
        assertThat(source.pulls()).hasSize(1);

        table.merge(new Member(peer("127.0.0.4").node, MemberState.ALIVE, 0, 0L));
        assertThat(queued).as("a change after the pass started is not lost").hasSize(1);
        service.stop();
    }
}
