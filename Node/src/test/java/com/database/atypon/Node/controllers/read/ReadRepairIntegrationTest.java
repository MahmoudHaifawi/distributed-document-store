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
import org.springframework.http.MediaType;
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
 * Over real HTTP with Spring Security active: a three-member cluster of this node plus two stub peers
 * (JDK {@link HttpServer}s that record what they receive and answer reads as the test dictates). Every
 * schema is held by all three nodes. A client read by id asks every replica, returns the freshest copy
 * and writes it back to each replica that holds an older one, but never to a replica that has no copy.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestPropertySource(properties = "app.jwt.secret=dev-secret-change-me-please-at-least-32-bytes-long-000")
class ReadRepairIntegrationTest {

    private static final String DB = "m7rr";
    private static final String ID = "0";
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

        /** This peer holds the document at {@code version}. */
        void holds(String doc) {
            JSONObject reply = new JSONObject()
                    .put("responseType", "SUCCESS")
                    .put("message", "Document fetched successfully")
                    .put("content", doc);
            readReply.set(reply.toString());
        }

        List<Received> reads() {
            synchronized (received) {
                return received.stream().filter(r -> r.method().equals("GET")).toList();
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
        Network.self = new Node("Node0", "8080");
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
    void staleReplicaIsRepairedAndAReplicaWithNoCopyIsNot() throws IOException {
        writeLocal(doc(7, 3));
        first.holds(doc(5, 1));      // stale: one version behind the winner
        // second answers not-found: it must not be handed a document it never had

        JSONObject read = new JSONObject(get().getBody());

        // the client gets the freshest copy, whichever replica it lives on
        assertThat(read.getString("responseType")).isEqualTo("SUCCESS");
        assertThat(new JSONObject(read.getString("content")).getInt("_version")).isEqualTo(3);
        assertThat(new JSONObject(read.getString("content")).getInt("n")).isEqualTo(7);

        // every other replica was asked, as an internal peer call
        assertThat(first.reads()).hasSize(1);
        assertThat(second.reads()).hasSize(1);
        assertThat(first.reads().get(0).uri())
                .isEqualTo("/user/read/document?id=" + ID + "&databaseName=" + DB + "&schemaName=" + schema);
        assertThat(jwtService.role(bearerToken(first.reads().get(0)))).isEqualTo("INTERNAL");

        // the stale replica was sent the winning document through the conditional repair endpoint (never the
        // verbatim update, which would upsert), as an internal peer write
        assertThat(first.writes()).hasSize(1);
        Received repair = first.writes().get(0);
        assertThat(repair.uri())
                .isEqualTo("/write/document/repair?database=" + DB + "&schema=" + schema + "&id=" + ID);
        assertThat(jwtService.role(bearerToken(repair))).isEqualTo("INTERNAL");
        JSONObject repaired = new JSONObject(repair.body());
        assertThat(repaired.getInt("_version")).isEqualTo(3);
        assertThat(repaired.getInt("n")).isEqualTo(7);

        // the replica with no copy was left alone: no resurrection
        assertThat(second.writes()).isEmpty();
    }

    @Test
    void staleLocalCopyIsRepairedFromTheFresherPeer() throws IOException {
        writeLocal(doc(1, 1));
        first.holds(doc(9, 3));      // the winner lives on a peer

        JSONObject read = new JSONObject(get().getBody());

        assertThat(new JSONObject(read.getString("content")).getInt("_version")).isEqualTo(3);
        assertThat(new JSONObject(read.getString("content")).getInt("n")).isEqualTo(9);

        // this node's own copy now matches the winner; the peer that already had it is not written to
        JSONObject local = new JSONObject(Files.readString(localDocument()));
        assertThat(local.getInt("_version")).isEqualTo(3);
        assertThat(local.getInt("n")).isEqualTo(9);
        assertThat(first.writes()).isEmpty();
        assertThat(second.writes()).isEmpty();
    }

    @Test
    void localReplicaWithNoCopyIsNotResurrected() {
        // no local copy; one peer is stale-but-present, the other has nothing
        first.holds(doc(4, 2));

        JSONObject read = new JSONObject(get().getBody());

        assertThat(new JSONObject(read.getString("content")).getInt("_version")).isEqualTo(2);
        assertThat(Files.exists(localDocument())).isFalse();
        assertThat(first.writes()).isEmpty();
        assertThat(second.writes()).isEmpty();
    }

    @Test
    void documentHeldByNoReplicaIsNotFound() {
        JSONObject read = new JSONObject(get().getBody());

        assertThat(read.getString("responseType")).isEqualTo("ERROR");
        assertThat(read.getString("message")).isEqualTo("Document not found");
        assertThat(first.writes()).isEmpty();
        assertThat(second.writes()).isEmpty();
    }

    @Test
    void degradedReadWithOnePeerUnreachableStillReturnsTheLocalDocument() throws IOException {
        writeLocal(doc(7, 3));
        first.holds(doc(5, 1));
        second.server.stop(0);       // unreachable: its answer is simply missing from the gather

        JSONObject read = new JSONObject(get().getBody());

        assertThat(read.getString("responseType")).isEqualTo("SUCCESS");
        assertThat(new JSONObject(read.getString("content")).getInt("_version")).isEqualTo(3);
        // the reachable stale peer is still repaired; the unreachable one cannot be
        assertThat(first.writes()).hasSize(1);
        assertThat(new JSONObject(first.writes().get(0).body()).getInt("_version")).isEqualTo(3);
    }

    @Test
    void bothPeersUnreachableStillReturnsTheLocalDocument() throws IOException {
        writeLocal(doc(7, 3));
        first.server.stop(0);
        second.server.stop(0);

        JSONObject read = new JSONObject(get().getBody());

        assertThat(read.getString("responseType")).isEqualTo("SUCCESS");
        assertThat(new JSONObject(read.getString("content")).getInt("_version")).isEqualTo(3);
    }

    @Test
    void internalReadServesTheLocalCopyAndAsksNoPeer() throws IOException {
        // an INTERNAL read is a peer coordinating its own quorum read: answering it by coordinating again
        // would recurse across the cluster, so it must be served from this node's store alone
        writeLocal(doc(7, 3));
        first.holds(doc(5, 9));      // a fresher copy on a peer must not be consulted or returned
        second.holds(doc(5, 9));

        JSONObject read = new JSONObject(getAs(serviceBearer()).getBody());

        assertThat(read.getString("responseType")).isEqualTo("SUCCESS");
        assertThat(new JSONObject(read.getString("content")).getInt("_version")).isEqualTo(3);
        assertThat(new JSONObject(read.getString("content")).getInt("n")).isEqualTo(7);
        assertThat(first.received).isEmpty();
        assertThat(second.received).isEmpty();
    }

    @Test
    void internalReadOfAMissingLocalCopyIsNotFoundWithoutAskingAnyPeer() {
        first.holds(doc(5, 9));      // a peer has it, but an internal read answers only for this node
        second.holds(doc(5, 9));

        JSONObject read = new JSONObject(getAs(serviceBearer()).getBody());

        assertThat(read.getString("responseType")).isEqualTo("ERROR");
        assertThat(read.getString("message")).isEqualTo("Document not found");
        assertThat(first.received).isEmpty();
        assertThat(second.received).isEmpty();
        assertThat(Files.exists(localDocument())).isFalse();
    }

    @Test
    void internalRepairEndpointOnlyUpgradesAPresentOlderDocument() throws IOException {
        writeLocal(doc(1, 1));

        repairAsInternal(doc(9, 3));
        assertThat(new JSONObject(Files.readString(localDocument())).getInt("_version")).isEqualTo(3);

        repairAsInternal(doc(4, 2));  // older than what is stored: must not regress it
        JSONObject stored = new JSONObject(Files.readString(localDocument()));
        assertThat(stored.getInt("_version")).isEqualTo(3);
        assertThat(stored.getInt("n")).isEqualTo(9);

        Files.delete(localDocument());
        repairAsInternal(doc(9, 5));  // the document is gone here: must not be recreated
        assertThat(Files.exists(localDocument())).isFalse();
    }

    @Test
    void aClientCannotCallTheInternalRepairEndpoint() throws IOException {
        writeLocal(doc(1, 1));

        ResponseEntity<String> response = post(
                "/write/document/repair?database=" + DB + "&schema=" + schema + "&id=" + ID,
                "Bearer " + jwtService.generateToken("alice", "USER", Duration.ofMinutes(5)), doc(9, 3));

        assertThat(response.getStatusCode().value()).isEqualTo(403);
        assertThat(new JSONObject(Files.readString(localDocument())).getInt("_version")).isEqualTo(1);
    }

    private void repairAsInternal(String winner) {
        ResponseEntity<String> response = post(
                "/write/document/repair?database=" + DB + "&schema=" + schema + "&id=" + ID, serviceBearer(), winner);
        assertThat(response.getStatusCode().value()).isEqualTo(200);
    }

    private String serviceBearer() {
        return "Bearer " + jwtService.generateServiceToken();
    }

    private static String doc(int n, int version) {
        return "{\"n\":" + n + ",\"_version\":" + version + "}";
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
        return getAs("Bearer " + jwtService.generateToken("alice", "USER", Duration.ofMinutes(5)));
    }

    private ResponseEntity<String> getAs(String authorization) {
        HttpHeaders headers = new HttpHeaders();
        headers.set("Authorization", authorization);
        return rest.exchange("http://localhost:" + port + "/user/read/document?id=" + ID
                        + "&databaseName=" + DB + "&schemaName=" + schema,
                HttpMethod.GET, new HttpEntity<>(headers), String.class);
    }

    private ResponseEntity<String> post(String pathWithQuery, String authorization, String jsonBody) {
        HttpHeaders headers = new HttpHeaders();
        headers.set("Authorization", authorization);
        headers.setContentType(MediaType.APPLICATION_JSON);
        return rest.exchange("http://localhost:" + port + pathWithQuery, HttpMethod.POST,
                new HttpEntity<>(jsonBody, headers), String.class);
    }
}
