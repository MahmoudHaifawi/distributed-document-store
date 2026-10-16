package com.database.atypon.Node.controllers.write;

import com.database.atypon.Node.model.Network;
import com.database.atypon.Node.model.Node;
import com.database.atypon.Node.security.JwtService;
import com.database.atypon.Node.utils.ring.HashRing;
import com.database.atypon.Node.utils.ring.RingRouter;
import com.sun.net.httpserver.HttpServer;
import org.json.JSONArray;
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
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Over real HTTP with Spring Security active: a two-member cluster of this node plus a stub peer (a JDK
 * {@link HttpServer} that records what it receives). A client write for a schema whose primary is the
 * stub must be handed to that primary with the caller's own token, never applied here; a write for a
 * schema this node is primary of is applied here and copied to the stub as an internal call.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestPropertySource(properties = "app.jwt.secret=dev-secret-change-me-please-at-least-32-bytes-long-000")
class WriteForwardIntegrationTest {

    private static final String DB = "m6fwd";
    private static final String STUB_REPLY = "[{\"responseType\":\"SUCCESS\",\"message\":\"stub applied\"}]";

    @LocalServerPort
    int port;

    @Autowired
    TestRestTemplate rest;

    @Autowired
    JwtService jwtService;

    private List<Node> savedNodes;
    private Node savedSelf;
    private HashRing savedRing;

    private HttpServer stub;
    private Node stubNode;
    private final List<Received> received = Collections.synchronizedList(new ArrayList<>());

    private record Received(String method, String uri, String auth, String body) {
    }

    @BeforeEach
    void startStubPeerAndFormTwoNodeCluster() throws IOException {
        savedNodes = Network.nodes;
        savedSelf = Network.self;
        savedRing = Network.ring;

        stub = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        stub.createContext("/", exchange -> {
            received.add(new Received(
                    exchange.getRequestMethod(),
                    exchange.getRequestURI().toString(),
                    exchange.getRequestHeaders().getFirst("Authorization"),
                    new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8)));
            byte[] reply = STUB_REPLY.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, reply.length);
            exchange.getResponseBody().write(reply);
            exchange.close();
        });
        stub.start();

        stubNode = new Node("localhost", String.valueOf(stub.getAddress().getPort()));
        Network.self = new Node("Node0", "8080");
        Network.nodes = Collections.synchronizedList(new ArrayList<>(List.of(stubNode)));
        Network.rebuildRing();
    }

    @AfterEach
    void stopStubAndRestoreMembership() throws IOException {
        stub.stop(0);
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
    void nonPrimaryForwardsWriteToPrimary() {
        String schema = schemaWhosePrimaryIs(stubNode.getName());
        String jwt = userJwt();

        ResponseEntity<String> response = post(
                "/write/document/new?database=" + DB + "&schema=" + schema, jwt, "{\"n\":1}");

        assertThat(received).hasSize(1);
        Received forwarded = received.get(0);
        assertThat(forwarded.method()).isEqualTo("POST");
        assertThat(forwarded.uri()).isEqualTo("/write/document/new?database=" + DB + "&schema=" + schema);
        assertThat(forwarded.auth()).isEqualTo("Bearer " + jwt); // the caller's own token, so the primary is the origin
        assertThat(forwarded.body()).contains("\"n\":1");
        // the client sees the primary's answer, and nothing was applied here
        assertThat(firstMessage(response)).isEqualTo("stub applied");
        assertThat(Files.exists(Paths.get("./data/" + DB))).isFalse();
    }

    @Test
    void nonPrimaryForwardsSchemaCreateToPrimary() {
        String schema = schemaWhosePrimaryIs(stubNode.getName());
        String jwt = userJwt();

        ResponseEntity<String> response = post("/write/schema/new?database=" + DB, jwt,
                "{\"schemaName\":\"" + schema + "\",\"schema\":{\"n\":\"Integer\"}}");

        assertThat(received).hasSize(1);
        assertThat(received.get(0).method()).isEqualTo("POST");
        assertThat(received.get(0).uri()).isEqualTo("/write/schema/new?database=" + DB);
        assertThat(received.get(0).auth()).isEqualTo("Bearer " + jwt);
        assertThat(received.get(0).body()).contains("\"schemaName\":\"" + schema + "\"");
        assertThat(firstMessage(response)).isEqualTo("stub applied");
        assertThat(Files.exists(Paths.get("./data/" + DB))).isFalse();
    }

    @Test
    void nonPrimaryForwardsUpdateToPrimary() {
        String schema = schemaWhosePrimaryIs(stubNode.getName());
        String jwt = userJwt();

        ResponseEntity<String> response = post(
                "/write/document/update?database=" + DB + "&schema=" + schema + "&id=0", jwt,
                "{\"n\":8,\"_version\":1}");

        assertThat(received).hasSize(1);
        assertThat(received.get(0).method()).isEqualTo("POST");
        assertThat(received.get(0).uri())
                .isEqualTo("/write/document/update?database=" + DB + "&schema=" + schema + "&id=0");
        assertThat(received.get(0).auth()).isEqualTo("Bearer " + jwt);
        assertThat(received.get(0).body()).contains("\"_version\":1");
        assertThat(firstMessage(response)).isEqualTo("stub applied");
    }

    @Test
    void nonPrimaryForwardsDeleteToPrimaryKeepingTheExpectedVersion() {
        String schema = schemaWhosePrimaryIs(stubNode.getName());
        String jwt = userJwt();

        ResponseEntity<String> response = post(
                "/write/document/delete?database=" + DB + "&schema=" + schema + "&id=0&version=3", jwt, null);

        assertThat(received).hasSize(1);
        assertThat(received.get(0).method()).isEqualTo("POST");
        assertThat(received.get(0).uri())
                .isEqualTo("/write/document/delete?database=" + DB + "&schema=" + schema + "&id=0&version=3");
        assertThat(received.get(0).auth()).isEqualTo("Bearer " + jwt);
        assertThat(received.get(0).body()).isEmpty();
        assertThat(firstMessage(response)).isEqualTo("stub applied");
    }

    @Test
    void primaryAppliesLocallyAndCopiesToTheOtherReplicaWithoutAnAffinityFile() throws IOException {
        String schema = schemaWhosePrimaryIs(Network.self.getName());
        Files.createDirectories(Paths.get("./data/" + DB + "/schemas"));
        String jwt = userJwt();

        ResponseEntity<String> response = post("/write/schema/new?database=" + DB, jwt,
                "{\"schemaName\":\"" + schema + "\",\"schema\":{\"n\":\"Integer\"}}");

        JSONArray responses = new JSONArray(response.getBody());
        assertThat(responses.length()).isEqualTo(2); // applied here + the stub replica's answer
        assertThat(responses.getJSONObject(0).getString("responseType")).isEqualTo("SUCCESS");
        assertThat(Files.exists(Paths.get("./data/" + DB + "/schemas/" + schema + ".json"))).isTrue();

        // the only peer is the other replica, so it is told to apply the schema, as an internal peer copy
        assertThat(received).hasSize(1);
        assertThat(received.get(0).uri()).isEqualTo("/write/schema/new?database=" + DB);
        String peerToken = received.get(0).auth().substring("Bearer ".length());
        assertThat(jwtService.role(peerToken)).isEqualTo("INTERNAL");

        // placement comes from the ring: no per-schema affinity file is written any more
        assertThat(Files.exists(Paths.get("./data/" + DB + "/affinities"))).isFalse();
    }

    private String schemaWhosePrimaryIs(String nodeName) {
        for (int i = 0; i < 1000; i++) {
            String schema = "s" + i;
            if (RingRouter.primaryFor(DB, schema).getName().equals(nodeName)) {
                return schema;
            }
        }
        throw new AssertionError("no schema whose primary is " + nodeName);
    }

    private String userJwt() {
        return jwtService.generateToken("alice", "USER", Duration.ofMinutes(5));
    }

    private ResponseEntity<String> post(String pathWithQuery, String jwt, String jsonBody) {
        HttpHeaders headers = new HttpHeaders();
        headers.set("Authorization", "Bearer " + jwt);
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<String> request = jsonBody == null ? new HttpEntity<>(headers) : new HttpEntity<>(jsonBody, headers);
        return rest.exchange("http://localhost:" + port + pathWithQuery, HttpMethod.POST, request, String.class);
    }

    private static String firstMessage(ResponseEntity<String> response) {
        return new JSONArray(response.getBody()).getJSONObject(0).getString("message");
    }
}
