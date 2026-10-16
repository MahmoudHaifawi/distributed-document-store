package com.database.atypon.Node.controllers.read;

import com.database.atypon.Node.model.Network;
import com.database.atypon.Node.model.Node;
import com.database.atypon.Node.security.JwtService;
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
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Over real HTTP with Spring Security active: a five-member cluster of this node, a stub peer (a JDK
 * {@link HttpServer} that records what it receives) and three silent members, so that a schema can be held
 * by a replication group that does not include this node. A read or index query for such a schema must be
 * handed to the schema's primary with the caller's own token, never served here; one for a schema this node
 * holds is served here. An index build or drop is handed to the primary when this node is not it.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestPropertySource(properties = "app.jwt.secret=dev-secret-change-me-please-at-least-32-bytes-long-000")
class ReadForwardIntegrationTest {

    private static final String DB = "m6read";

    /** What a read or query endpoint answers with: a single Response. */
    private static final String STUB_READ_REPLY =
            "{\"responseType\":\"SUCCESS\",\"message\":\"stub read\",\"content\":[1,2]}";
    /** What an index build or drop answers with: a vector of Responses. */
    private static final String STUB_INDEX_REPLY = "[{\"responseType\":\"SUCCESS\",\"message\":\"stub indexed\"}]";

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
    void startStubPeerAndFormFiveNodeCluster() throws IOException {
        savedNodes = Network.nodes;
        savedSelf = Network.self;
        savedRing = Network.ring;

        stub = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        stub.createContext("/", exchange -> {
            String uri = exchange.getRequestURI().toString();
            received.add(new Received(
                    exchange.getRequestMethod(),
                    uri,
                    exchange.getRequestHeaders().getFirst("Authorization"),
                    new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8)));
            byte[] reply = (uri.startsWith("/admin/") ? STUB_INDEX_REPLY : STUB_READ_REPLY)
                    .getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, reply.length);
            exchange.getResponseBody().write(reply);
            exchange.close();
        });
        stub.start();

        stubNode = new Node("localhost", String.valueOf(stub.getAddress().getPort()));
        Network.self = new Node("Node0", "8080");
        // three further members that are never contacted: with a replication factor of 3, a 2-node cluster
        // would make this node a replica of everything, leaving nothing to forward
        Network.nodes = Collections.synchronizedList(new ArrayList<>(List.of(
                stubNode, new Node("Node2", "8080"), new Node("Node3", "8080"), new Node("Node4", "8080"))));
        Network.rebuildRing();
    }

    @AfterEach
    void stopStubAndRestoreMembership() {
        stub.stop(0);
        Network.nodes = savedNodes;
        Network.self = savedSelf;
        Network.ring = savedRing;
    }

    @Test
    void nonReplicaForwardsFetchByIdToTheSchemasPrimary() {
        String schema = schemaNotHeldHereWhosePrimaryIsTheStub();
        String jwt = userJwt();

        ResponseEntity<String> response = get(
                "/user/read/document?id=7&databaseName=" + DB + "&schemaName=" + schema, jwt);

        assertThat(received).hasSize(1);
        Received forwarded = received.get(0);
        assertThat(forwarded.method()).isEqualTo("GET");
        assertThat(forwarded.uri()).isEqualTo("/user/read/document?id=7&databaseName=" + DB + "&schemaName=" + schema);
        assertThat(forwarded.auth()).isEqualTo("Bearer " + jwt); // the caller's own token travels with the read
        // the client sees the replica's answer
        JSONObject body = new JSONObject(response.getBody());
        assertThat(body.getString("message")).isEqualTo("stub read");
        assertThat(body.getJSONArray("content").toList()).containsExactly(1, 2);
    }

    @Test
    void nonReplicaForwardsFetchAllToTheSchemasPrimary() {
        String schema = schemaNotHeldHereWhosePrimaryIsTheStub();
        String jwt = userJwt();

        ResponseEntity<String> response = get("/user/read/all?databaseName=" + DB + "&schemaName=" + schema, jwt);

        assertThat(received).hasSize(1);
        assertThat(received.get(0).method()).isEqualTo("GET");
        assertThat(received.get(0).uri()).isEqualTo("/user/read/all?databaseName=" + DB + "&schemaName=" + schema);
        assertThat(received.get(0).auth()).isEqualTo("Bearer " + jwt);
        assertThat(new JSONObject(response.getBody()).getString("message")).isEqualTo("stub read");
    }

    @Test
    void nonReplicaForwardsIndexQueryToTheSchemasPrimary() {
        String schema = schemaNotHeldHereWhosePrimaryIsTheStub();
        String jwt = userJwt();

        ResponseEntity<String> response = post("/user/index/query", jwt,
                "{\"database\":\"" + DB + "\",\"schema\":\"" + schema
                        + "\",\"field\":\"Age\",\"op\":\"GTE\",\"value\":25}");

        assertThat(received).hasSize(1);
        Received forwarded = received.get(0);
        assertThat(forwarded.method()).isEqualTo("POST");
        assertThat(forwarded.uri()).isEqualTo("/user/index/query");
        assertThat(forwarded.auth()).isEqualTo("Bearer " + jwt);
        JSONObject forwardedBody = new JSONObject(forwarded.body());
        assertThat(forwardedBody.getString("schema")).isEqualTo(schema);
        assertThat(forwardedBody.getString("field")).isEqualTo("Age");
        assertThat(forwardedBody.getString("op")).isEqualTo("GTE");
        assertThat(forwardedBody.getInt("value")).isEqualTo(25);
        assertThat(new JSONObject(response.getBody()).getString("message")).isEqualTo("stub read");
    }

    @Test
    void nonPrimaryForwardsIndexCreateAndDropToThePrimary() {
        String schema = schemaNotHeldHereWhosePrimaryIsTheStub();
        String jwt = adminJwt();

        ResponseEntity<String> created = post(
                "/admin/index/create?database=" + DB + "&schema=" + schema + "&field=Age", jwt, null);
        ResponseEntity<String> dropped = post(
                "/admin/index/drop?database=" + DB + "&schema=" + schema + "&field=Age", jwt, null);

        assertThat(received).hasSize(2);
        assertThat(received.get(0).method()).isEqualTo("POST");
        assertThat(received.get(0).uri())
                .isEqualTo("/admin/index/create?database=" + DB + "&schema=" + schema + "&field=Age");
        assertThat(received.get(0).auth()).isEqualTo("Bearer " + jwt); // so the primary is the origin and fans out
        assertThat(received.get(1).method()).isEqualTo("POST");
        assertThat(received.get(1).uri())
                .isEqualTo("/admin/index/drop?database=" + DB + "&schema=" + schema + "&field=Age");
        assertThat(received.get(1).auth()).isEqualTo("Bearer " + jwt);
        assertThat(new JSONArray(created.getBody()).getJSONObject(0).getString("message")).isEqualTo("stub indexed");
        assertThat(new JSONArray(dropped.getBody()).getJSONObject(0).getString("message")).isEqualTo("stub indexed");
    }

    @Test
    void nonReplicaForwardsListIndexesToTheSchemasPrimary() {
        String schema = schemaNotHeldHereWhosePrimaryIsTheStub();
        String jwt = adminJwt();

        ResponseEntity<String> response = get("/admin/index/list?database=" + DB + "&schema=" + schema, jwt);

        assertThat(received).hasSize(1);
        Received forwarded = received.get(0);
        assertThat(forwarded.method()).isEqualTo("GET"); // a read stays a GET and carries no body
        assertThat(forwarded.uri()).isEqualTo("/admin/index/list?database=" + DB + "&schema=" + schema);
        assertThat(forwarded.auth()).isEqualTo("Bearer " + jwt);
        assertThat(forwarded.body()).isEmpty();
        // the client sees the replica's list, not an empty one from a node that holds no copy
        assertThat(new JSONArray(response.getBody()).getJSONObject(0).getString("message")).isEqualTo("stub indexed");
    }

    @Test
    void replicaServesListIndexesLocally() {
        String schema = schemaHeldHere();

        ResponseEntity<String> response = get("/admin/index/list?database=" + DB + "&schema=" + schema, adminJwt());

        assertThat(received).isEmpty(); // served here, nothing handed to a peer
        assertThat(new JSONArray(response.getBody()).getJSONObject(0).getString("message"))
                .isNotEqualTo("stub indexed");
    }

    @Test
    void replicaServesReadsAndIndexQueriesLocally() {
        String schema = schemaHeldHere();
        String jwt = userJwt();

        ResponseEntity<String> byId = get("/user/read/document?id=7&databaseName=" + DB + "&schemaName=" + schema, jwt);
        ResponseEntity<String> all = get("/user/read/all?databaseName=" + DB + "&schemaName=" + schema, jwt);
        ResponseEntity<String> query = post("/user/index/query", jwt,
                "{\"database\":\"" + DB + "\",\"schema\":\"" + schema + "\",\"field\":\"Age\",\"op\":\"GTE\",\"value\":1}");

        assertThat(received).isEmpty(); // served here, nothing handed to a peer
        // answered by this node (the database holds no such data), not by the stub
        assertThat(new JSONObject(byId.getBody()).getString("responseType")).isEqualTo("ERROR");
        assertThat(new JSONObject(all.getBody()).getString("message")).isNotEqualTo("stub read");
        assertThat(new JSONObject(query.getBody()).getString("message")).isNotEqualTo("stub read");
    }

    /** A schema (by scan) that this node does not hold and whose primary is the stub peer. */
    private String schemaNotHeldHereWhosePrimaryIsTheStub() {
        for (int i = 0; i < 5000; i++) {
            String schema = "s" + i;
            if (!RingRouter.isReplica(DB, schema)
                    && RingRouter.primaryFor(DB, schema).getName().equals(stubNode.getName())) {
                return schema;
            }
        }
        throw new AssertionError("no schema held away from this node with the stub as its primary");
    }

    /** A schema (by scan) whose preference list includes this node. */
    private static String schemaHeldHere() {
        for (int i = 0; i < 5000; i++) {
            String schema = "s" + i;
            if (RingRouter.isReplica(DB, schema)) {
                return schema;
            }
        }
        throw new AssertionError("no schema held by this node");
    }

    private String userJwt() {
        return jwtService.generateToken("alice", "USER", Duration.ofMinutes(5));
    }

    private String adminJwt() {
        return jwtService.generateToken("root", "ADMIN", Duration.ofMinutes(5));
    }

    private ResponseEntity<String> get(String pathWithQuery, String jwt) {
        HttpHeaders headers = new HttpHeaders();
        headers.set("Authorization", "Bearer " + jwt);
        return rest.exchange("http://localhost:" + port + pathWithQuery, HttpMethod.GET,
                new HttpEntity<>(headers), String.class);
    }

    private ResponseEntity<String> post(String pathWithQuery, String jwt, String jsonBody) {
        HttpHeaders headers = new HttpHeaders();
        headers.set("Authorization", "Bearer " + jwt);
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<String> request = jsonBody == null ? new HttpEntity<>(headers) : new HttpEntity<>(jsonBody, headers);
        return rest.exchange("http://localhost:" + port + pathWithQuery, HttpMethod.POST, request, String.class);
    }
}
