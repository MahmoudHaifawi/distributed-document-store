package com.database.atypon.Node.controllers.read;

import com.database.atypon.Node.model.Network;
import com.database.atypon.Node.model.Node;
import com.database.atypon.Node.security.JwtService;
import com.database.atypon.Node.utils.quorum.ConsistencyConfig;
import com.database.atypon.Node.utils.quorum.ConsistencyLevel;
import com.database.atypon.Node.utils.ring.HashRing;
import com.database.atypon.Node.utils.ring.RingRouter;
import com.sun.net.httpserver.HttpServer;
import org.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.TestPropertySource;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * M8 over real HTTP with Spring Security active: the same three-member cluster as
 * {@link ReadRepairIntegrationTest} (this node plus two stub peers that record what they receive), now with
 * documents carrying vector clocks and tombstones. A client read reconciles every replica's copy causally; a
 * tombstone winner reads as "Document not found", and read-repair sends the winner (a tombstone included) to
 * every replica whose copy differs from it, an absent one too.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestPropertySource(properties = "app.jwt.secret=dev-secret-change-me-please-at-least-32-bytes-long-000")
class ReadRepairVClockTest {

    private static final String DB = "m8rr";
    private static final String ID = "0";
    private static final String SELF = "Node0";
    private static final String WRITE_ACK = "[{\"responseType\":\"SUCCESS\",\"message\":\"stub applied\"}]";
    private static final String NOT_FOUND = "{\"responseType\":\"ERROR\",\"message\":\"Document not found\"}";

    @LocalServerPort
    int port;

    @Autowired
    TestRestTemplate rest;

    @Autowired
    JwtService jwtService;

    private List<Node> savedNodes;
    private Node savedSelf;
    private HashRing savedRing;

    private Stub first;
    private Stub second;
    private String schema;

    private record Received(String method, String uri, String auth, String body) {
    }

    /** A peer that records every request and answers a read with whatever the test last set. */
    private static final class Stub {
        final HttpServer server;
        final Node node;
        final List<Received> received = Collections.synchronizedList(new ArrayList<>());
        final AtomicReference<String> readReply = new AtomicReference<>(NOT_FOUND);

        Stub(String host) throws IOException {
            server = HttpServer.create(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0), 0);
            server.createContext("/", exchange -> {
                String method = exchange.getRequestMethod();
                received.add(new Received(
                        method,
                        exchange.getRequestURI().toString(),
                        exchange.getRequestHeaders().getFirst("Authorization"),
                        new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8)));
                byte[] reply = ("POST".equals(method) ? WRITE_ACK : readReply.get()).getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().add("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, reply.length);
                exchange.getResponseBody().write(reply);
                exchange.close();
            });
            server.start();
            // the ring keys members by name, so the two stubs need different names that both reach loopback
            node = new Node(host, String.valueOf(server.getAddress().getPort()));
        }

        /** This peer holds {@code doc} (a live document or a tombstone). */
        void holds(String doc) {
            JSONObject reply = new JSONObject()
                    .put("responseType", "SUCCESS")
                    .put("message", "Document fetched successfully")
                    .put("content", doc);
            readReply.set(reply.toString());
        }

        List<Received> repairs() {
            synchronized (received) {
                return received.stream()
                        .filter(r -> r.method().equals("POST") && r.uri().startsWith("/write/document/repair?"))
                        .toList();
            }
        }

        List<Received> writes() {
            synchronized (received) {
                return received.stream().filter(r -> r.method().equals("POST")).toList();
            }
        }
    }

    @BeforeEach
    void formThreeNodeCluster() throws IOException {
        // the verdicts below assume the default; a deployment-level override would make them meaningless
        assertThat(ConsistencyConfig.readConsistency()).isEqualTo(ConsistencyLevel.QUORUM);

        savedNodes = Network.nodes;
        savedSelf = Network.self;
        savedRing = Network.ring;

        first = new Stub("localhost");
        second = new Stub("127.0.0.1");
        Network.self = new Node(SELF, "8080");
        Network.nodes = Collections.synchronizedList(new ArrayList<>(List.of(first.node, second.node)));
        Network.rebuildRing();

        schema = schemaHeldHere();
        Files.createDirectories(Paths.get("./data/" + DB + "/schemas"));
        Files.createDirectories(Paths.get("./data/" + DB + "/" + schema + "-records"));
        Files.writeString(Paths.get("./data/" + DB + "/schemas/" + schema + ".json"),
                "{\"info\":{\"schemaName\":\"" + schema + "\",\"nextId\":1},\"schema\":{\"n\":\"Integer\"}}");
    }

    @AfterEach
    void stopStubsAndRestoreMembership() throws IOException {
        first.server.stop(0);
        second.server.stop(0);
        Network.nodes = savedNodes;
        Network.self = savedSelf;
        Network.ring = savedRing;

        Path db = Paths.get("./data/" + DB);
        if (Files.exists(db)) {
            try (var paths = Files.walk(db)) {
                paths.sorted((a, b) -> b.compareTo(a)).forEach(p -> p.toFile().delete());
            }
        }
    }

    @Test
    void clientReadOfATombstonedDocReturnsNotFound() throws IOException {
        writeLocal(tombstone(2, SELF + ":2", 2_000));

        JSONObject read = new JSONObject(get().getBody());

        assertThat(read.getString("responseType")).isEqualTo("ERROR");
        assertThat(read.getString("message")).isEqualTo("Document not found");
        assertThat(read.isNull("content")).isTrue(); // the tombstone itself never reaches the client
    }

    @Test
    void readRepairPropagatesATombstoneToAReplicaThatMissedTheDelete() throws IOException {
        writeLocal(tombstone(2, SELF + ":2", 2_000));
        first.holds(live(5, 1, SELF + ":1", 1_000));     // missed the delete: still serves the document
        // second answers not-found

        JSONObject read = new JSONObject(get().getBody());

        assertThat(read.getString("responseType")).isEqualTo("ERROR");
        assertThat(read.getString("message")).isEqualTo("Document not found");

        // the replica that missed the delete is handed the tombstone through the internal repair endpoint
        assertThat(first.repairs()).hasSize(1);
        Received repair = first.repairs().get(0);
        assertThat(repair.uri())
                .isEqualTo("/write/document/repair?database=" + DB + "&schema=" + schema + "&id=" + ID);
        assertThat(jwtService.role(bearerToken(repair))).isEqualTo("INTERNAL");
        JSONObject sent = new JSONObject(repair.body());
        assertThat(sent.getBoolean("_deleted")).isTrue();
        assertThat(sent.getJSONObject("_vclock").getInt(SELF)).isEqualTo(2);

        // this node already holds the tombstone, so it is left as it is
        JSONObject local = new JSONObject(Files.readString(localDocument()));
        assertThat(local.getBoolean("_deleted")).isTrue();
    }

    @Test
    void readRepairHealsAReplicaThatMissedACreate() throws IOException {
        writeLocal(live(7, 1, SELF + ":1", 1_000));
        first.holds(live(7, 1, SELF + ":1", 1_000));     // has it already
        // second answers not-found: it missed the create

        JSONObject read = new JSONObject(get().getBody());

        assertThat(read.getString("responseType")).isEqualTo("SUCCESS");
        assertThat(new JSONObject(read.getString("content")).getInt("n")).isEqualTo(7);

        // the replica with no copy is healed from the live winner; the one that already had it is not touched
        assertThat(second.repairs()).hasSize(1);
        Received repair = second.repairs().get(0);
        assertThat(repair.uri())
                .isEqualTo("/write/document/repair?database=" + DB + "&schema=" + schema + "&id=" + ID);
        assertThat(jwtService.role(bearerToken(repair))).isEqualTo("INTERNAL");
        JSONObject sent = new JSONObject(repair.body());
        assertThat(sent.optBoolean("_deleted", false)).isFalse();
        assertThat(sent.getInt("n")).isEqualTo(7);
        assertThat(sent.getJSONObject("_vclock").getInt(SELF)).isEqualTo(1);
        assertThat(first.writes()).isEmpty();
    }

    @Test
    void aReplicaThatNeverHeldTheDocumentIsToldItWasDeleted() throws IOException {
        writeLocal(tombstone(2, SELF + ":2", 2_000));
        // both peers answer not-found

        get();

        // an absent replica learns of the delete, so a late copy of the old document cannot win there
        assertThat(first.repairs()).hasSize(1);
        assertThat(second.repairs()).hasSize(1);
        assertThat(new JSONObject(first.repairs().get(0).body()).getBoolean("_deleted")).isTrue();
    }

    @Test
    void aLiveCopyConcurrentWithTheTombstoneAndNewerSurvivesTheRead() throws IOException {
        writeLocal(tombstone(2, SELF + ":2", 1_000));
        first.holds(live(9, 2, "PeerX:1", 5_000));       // a write the delete never saw, and the later one

        JSONObject read = new JSONObject(get().getBody());

        // neither dominates; the later write wins, so the document is not deleted
        assertThat(read.getString("responseType")).isEqualTo("SUCCESS");
        assertThat(new JSONObject(read.getString("content")).getInt("n")).isEqualTo(9);

        // this node's tombstone is replaced by the live winner, whose clock covers both histories
        JSONObject local = new JSONObject(Files.readString(localDocument()));
        assertThat(local.optBoolean("_deleted", false)).isFalse();
        assertThat(local.getInt("n")).isEqualTo(9);
        assertThat(local.getJSONObject("_vclock").getInt(SELF)).isEqualTo(2);
        assertThat(local.getJSONObject("_vclock").getInt("PeerX")).isEqualTo(1);
    }

    @Test
    void aTombstoneThatIsTheLaterOfTwoConcurrentWritesWinsTheRead() throws IOException {
        writeLocal(live(9, 2, "PeerX:1", 1_000));
        first.holds(tombstone(2, SELF + ":2", 5_000));

        JSONObject read = new JSONObject(get().getBody());

        assertThat(read.getString("responseType")).isEqualTo("ERROR");
        assertThat(read.getString("message")).isEqualTo("Document not found");
        // the local live copy is replaced by the tombstone through this node's own repair
        assertThat(new JSONObject(Files.readString(localDocument())).getBoolean("_deleted")).isTrue();
    }

    @Test
    void aSingleReplicaClusterReadsALiveDocumentAndHidesATombstone() throws IOException {
        // N = 1: no peers to gather from, so the reconcile is over this node's own copy alone
        Network.nodes = Collections.synchronizedList(new ArrayList<>());
        Network.rebuildRing();
        String solo = schemaHeldHere();
        Files.createDirectories(Paths.get("./data/" + DB + "/" + solo + "-records"));
        Files.writeString(Paths.get("./data/" + DB + "/schemas/" + solo + ".json"),
                "{\"info\":{\"schemaName\":\"" + solo + "\",\"nextId\":1},\"schema\":{\"n\":\"Integer\"}}");
        Path file = Paths.get("./data/" + DB + "/" + solo + "-records/" + ID + ".json");

        Files.writeString(file, live(3, 1, SELF + ":1", 1_000));
        JSONObject liveRead = new JSONObject(getFrom(solo).getBody());
        assertThat(liveRead.getString("responseType")).isEqualTo("SUCCESS");
        assertThat(new JSONObject(liveRead.getString("content")).getInt("n")).isEqualTo(3);

        Files.writeString(file, tombstone(2, SELF + ":2", 2_000));
        JSONObject deletedRead = new JSONObject(getFrom(solo).getBody());
        assertThat(deletedRead.getString("responseType")).isEqualTo("ERROR");
        assertThat(deletedRead.getString("message")).isEqualTo("Document not found");

        assertThat(first.received).isEmpty();
        assertThat(second.received).isEmpty();
    }

    /** A live document at {@code version} whose clock is a single {@code node:count} entry. */
    private static String live(int n, int version, String clockEntry, long ts) {
        return new JSONObject().put("n", n).put("_version", version)
                .put("_vclock", clock(clockEntry)).put("_ts", ts).toString();
    }

    private static String tombstone(int version, String clockEntry, long ts) {
        return new JSONObject().put("_deleted", true).put("_version", version)
                .put("_vclock", clock(clockEntry)).put("_ts", ts).toString();
    }

    private static JSONObject clock(String entry) {
        String[] parts = entry.split(":");
        return new JSONObject().put(parts[0], Integer.parseInt(parts[1]));
    }

    private Path localDocument() {
        return Paths.get("./data/" + DB + "/" + schema + "-records/" + ID + ".json");
    }

    private void writeLocal(String doc) throws IOException {
        Files.writeString(localDocument(), doc);
    }

    private static String bearerToken(Received request) {
        return request.auth().substring("Bearer ".length());
    }

    /** A schema (by scan) whose preference list includes this node. */
    private static String schemaHeldHere() {
        for (int i = 0; i < 1000; i++) {
            String schema = "s" + i;
            if (RingRouter.isReplica(DB, schema)) {
                return schema;
            }
        }
        throw new AssertionError("no schema held by this node");
    }

    /** A client (USER) read by id. */
    private ResponseEntity<String> get() {
        return getFrom(schema);
    }

    private ResponseEntity<String> getFrom(String schemaName) {
        HttpHeaders headers = new HttpHeaders();
        headers.set("Authorization",
                "Bearer " + jwtService.generateToken("alice", "USER", Duration.ofMinutes(5)));
        return rest.exchange("http://localhost:" + port + "/user/read/document?id=" + ID
                        + "&databaseName=" + DB + "&schemaName=" + schemaName,
                HttpMethod.GET, new HttpEntity<>(headers), String.class);
    }
}
