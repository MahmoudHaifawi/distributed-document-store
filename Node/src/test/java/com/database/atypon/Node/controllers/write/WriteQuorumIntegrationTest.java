package com.database.atypon.Node.controllers.write;

import com.database.atypon.Node.model.Network;
import com.database.atypon.Node.model.Node;
import com.database.atypon.Node.security.JwtService;
import com.database.atypon.Node.utils.quorum.ConsistencyConfig;
import com.database.atypon.Node.utils.quorum.ConsistencyLevel;
import com.database.atypon.Node.utils.ring.HashRing;
import com.database.atypon.Node.utils.ring.RingRouter;
import com.sun.net.httpserver.HttpServer;
import org.json.JSONArray;
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
 * (JDK {@link HttpServer}s whose answer the test controls). Every schema is held by all three nodes, so
 * at the default QUORUM write consistency the client is acknowledged only when two of them persisted the
 * write: this node's own apply plus at least one stub's SUCCESS.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestPropertySource(properties = "app.jwt.secret=dev-secret-change-me-please-at-least-32-bytes-long-000")
class WriteQuorumIntegrationTest {

    private static final String DB = "m7wq";
    private static final String ACK = "[{\"responseType\":\"SUCCESS\",\"message\":\"stub applied\"}]";
    private static final String NACK = "[{\"responseType\":\"ERROR\",\"message\":\"stub down\"}]";

    @LocalServerPort
    int port;

    @Autowired
    TestRestTemplate rest;

    @Autowired
    JwtService jwtService;

    private List<Node> savedNodes;
    private Node savedSelf;
    private HashRing savedRing;

    private Stub stub1;
    private Stub stub2;

    /** A peer whose reply to every request is whatever the test last set. */
    private static final class Stub {
        final HttpServer server;
        final Node node;
        final AtomicReference<String> reply = new AtomicReference<>(ACK);

        Stub(String host) throws IOException {
            server = HttpServer.create(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0), 0);
            server.createContext("/", exchange -> {
                exchange.getRequestBody().readAllBytes();
                byte[] body = reply.get().getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().add("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, body.length);
                exchange.getResponseBody().write(body);
                exchange.close();
            });
            server.start();
            // the ring keys members by name, so the two stubs need different names that both reach loopback
            node = new Node(host, String.valueOf(server.getAddress().getPort()));
        }

        void ack() {
            reply.set(ACK);
        }

        void nack() {
            reply.set(NACK);
        }
    }

    @BeforeEach
    void formThreeNodeCluster() throws IOException {
        // the verdicts below assume the default; a deployment-level override would make them meaningless
        assertThat(ConsistencyConfig.writeConsistency()).isEqualTo(ConsistencyLevel.QUORUM);

        savedNodes = Network.nodes;
        savedSelf = Network.self;
        savedRing = Network.ring;

        stub1 = new Stub("localhost");
        stub2 = new Stub("127.0.0.1");
        Network.self = new Node("Node0", "8080");
        Network.nodes = Collections.synchronizedList(new ArrayList<>(List.of(stub1.node, stub2.node)));
        Network.rebuildRing();

        Files.createDirectories(Paths.get("./data/" + DB + "/schemas"));
    }

    @AfterEach
    void stopStubsAndRestoreMembership() throws IOException {
        stub1.server.stop(0);
        stub2.server.stop(0);
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
    void documentCreateSucceedsWhenBothPeersAck() {
        String schema = createSchemaThisNodeIsPrimaryOf();

        ResponseEntity<String> response = createDocument(schema);

        assertThat(responseTypes(response)).containsExactly("SUCCESS", "SUCCESS", "SUCCESS");
    }

    @Test
    void documentCreateStillSucceedsWithOneAckingPeerBecauseThatIsTwoOfThree() {
        String schema = createSchemaThisNodeIsPrimaryOf();
        stub2.nack();

        ResponseEntity<String> response = createDocument(schema);

        assertThat(firstType(response)).isEqualTo("SUCCESS");
        assertThat(responseTypes(response)).containsExactlyInAnyOrder("SUCCESS", "SUCCESS", "ERROR");
    }

    @Test
    void documentCreateFailsWithQuorumNotMetWhenNoPeerAcks() {
        String schema = createSchemaThisNodeIsPrimaryOf();
        stub1.nack();
        stub2.nack();

        ResponseEntity<String> response = createDocument(schema);

        JSONObject first = new JSONArray(response.getBody()).getJSONObject(0);
        assertThat(first.getString("responseType")).isEqualTo("ERROR");
        assertThat(first.getString("message")).contains("write quorum not met").contains("1/2");
        // the peers' own answers still follow, so the caller can see who failed
        assertThat(responseTypes(response)).containsExactly("ERROR", "ERROR", "ERROR");
    }

    @Test
    void schemaCreateFailsWithQuorumNotMetWhenNoPeerAcks() {
        stub1.nack();
        stub2.nack();
        String schema = schemasWhosePrimaryIsThisNode(1).get(0);

        ResponseEntity<String> response = post("/write/schema/new?database=" + DB,
                "{\"schemaName\":\"" + schema + "\",\"schema\":{\"n\":\"Integer\"}}");

        assertThat(firstMessage(response)).contains("write quorum not met");
    }

    @Test
    void updateFailsWithQuorumNotMetWhenNoPeerAcks() {
        String schema = createSchemaThisNodeIsPrimaryOf();
        assertThat(firstType(createDocument(schema))).isEqualTo("SUCCESS");
        stub1.nack();
        stub2.nack();

        ResponseEntity<String> response = post(
                "/write/document/update?database=" + DB + "&schema=" + schema + "&id=0", "{\"n\":8,\"_version\":1}");

        assertThat(firstMessage(response)).contains("write quorum not met");
    }

    @Test
    void updateSucceedsWhenOnePeerAcks() {
        String schema = createSchemaThisNodeIsPrimaryOf();
        assertThat(firstType(createDocument(schema))).isEqualTo("SUCCESS");
        stub1.nack();

        ResponseEntity<String> response = post(
                "/write/document/update?database=" + DB + "&schema=" + schema + "&id=0", "{\"n\":8,\"_version\":1}");

        assertThat(firstType(response)).isEqualTo("SUCCESS");
    }

    @Test
    void deleteFailsWithQuorumNotMetWhenNoPeerAcks() {
        String schema = createSchemaThisNodeIsPrimaryOf();
        assertThat(firstType(createDocument(schema))).isEqualTo("SUCCESS");
        stub1.nack();
        stub2.nack();

        ResponseEntity<String> response = post(
                "/write/document/delete?database=" + DB + "&schema=" + schema + "&id=0&version=1", null);

        assertThat(firstMessage(response)).contains("write quorum not met");
    }

    @Test
    void aLocalFailureStaysTheClientAnswerEvenWhenThePeersAck() {
        String schema = createSchemaThisNodeIsPrimaryOf();
        assertThat(firstType(createDocument(schema))).isEqualTo("SUCCESS");

        // wrong expected version: the local apply conflicts, which is what the client must be told
        ResponseEntity<String> response = post(
                "/write/document/update?database=" + DB + "&schema=" + schema + "&id=0", "{\"n\":8,\"_version\":9}");

        assertThat(firstMessage(response)).startsWith("Version conflict");
    }

    /** Creates, with both peers acking, a schema this node is primary of and returns its name. */
    private String createSchemaThisNodeIsPrimaryOf() {
        stub1.ack();
        stub2.ack();
        String schema = schemasWhosePrimaryIsThisNode(1).get(0);
        assertThat(RingRouter.replicasFor(DB, schema)).hasSize(3); // N = 3, so W = 2
        ResponseEntity<String> created = post("/write/schema/new?database=" + DB,
                "{\"schemaName\":\"" + schema + "\",\"schema\":{\"n\":\"Integer\"}}");
        assertThat(firstType(created)).isEqualTo("SUCCESS");
        return schema;
    }

    private ResponseEntity<String> createDocument(String schema) {
        return post("/write/document/new?database=" + DB + "&schema=" + schema, "{\"n\":1}");
    }

    private List<String> schemasWhosePrimaryIsThisNode(int count) {
        List<String> found = new ArrayList<>();
        for (int i = 0; i < 1000 && found.size() < count; i++) {
            String schema = "s" + i;
            if (RingRouter.isPrimary(DB, schema)) {
                found.add(schema);
            }
        }
        assertThat(found).hasSize(count);
        return found;
    }

    private ResponseEntity<String> post(String pathWithQuery, String jsonBody) {
        HttpHeaders headers = new HttpHeaders();
        headers.set("Authorization", "Bearer " + jwtService.generateToken("alice", "USER", Duration.ofMinutes(5)));
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<String> request = jsonBody == null ? new HttpEntity<>(headers) : new HttpEntity<>(jsonBody, headers);
        return rest.exchange("http://localhost:" + port + pathWithQuery, HttpMethod.POST, request, String.class);
    }

    private static List<String> responseTypes(ResponseEntity<String> response) {
        JSONArray array = new JSONArray(response.getBody());
        List<String> types = new ArrayList<>();
        for (int i = 0; i < array.length(); i++) {
            types.add(array.getJSONObject(i).getString("responseType"));
        }
        return types;
    }

    private static String firstType(ResponseEntity<String> response) {
        return new JSONArray(response.getBody()).getJSONObject(0).getString("responseType");
    }

    private static String firstMessage(ResponseEntity<String> response) {
        return new JSONArray(response.getBody()).getJSONObject(0).getString("message");
    }
}
