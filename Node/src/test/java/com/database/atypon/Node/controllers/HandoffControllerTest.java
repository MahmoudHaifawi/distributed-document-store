package com.database.atypon.Node.controllers;

import com.database.atypon.Node.security.JwtService;
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
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.TestPropertySource;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The two endpoints a node answers during a state handoff, over real HTTP through the real security chain:
 * {@code /swim/schemas} says which schemas this node holds, and {@code /swim/pull} hands over every stored
 * document of one, read raw from disk so that deletes (tombstones) travel with the rest. Node-to-node only.
 *
 * <p>It shares the application context (and JWT secret) of the other integration tests: {@code ServiceTokens}
 * is a static bridge to whichever context was created last, so a context with a different secret created
 * mid-run would break the internal calls of every cached context that runs after it.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestPropertySource(properties = "app.jwt.secret=dev-secret-change-me-please-at-least-32-bytes-long-000")
class HandoffControllerTest {

    private static final String DB = "handoffctldb";

    private final Path dbDir = Paths.get("./data", DB);
    private final Path records = dbDir.resolve("items-records");

    @LocalServerPort
    int port;
    @Autowired
    TestRestTemplate rest;
    @Autowired
    JwtService jwt;

    private final JSONObject schemaDefinition = new JSONObject()
            .put("info", new JSONObject().put("schemaName", "items").put("nextId", 2))
            .put("schema", new JSONObject().put("Name", "String"));
    private final JSONObject live = new JSONObject().put("Name", "ann").put("_version", 3)
            .put("_vclock", new JSONObject().put("n1", 3)).put("_ts", 3_000);
    private final JSONObject deleted = new JSONObject().put("_deleted", true).put("_version", 2)
            .put("_vclock", new JSONObject().put("n1", 2)).put("_ts", 2_000);

    @BeforeEach
    void seed() throws Exception {
        deleteTree(dbDir);
        Files.createDirectories(dbDir.resolve("schemas"));
        Files.createDirectories(records);
        Files.writeString(dbDir.resolve("schemas").resolve("items.json"), schemaDefinition.toString());
        Files.writeString(records.resolve("0.json"), live.toString());
        Files.writeString(records.resolve("1.json"), deleted.toString());
        Files.writeString(records.resolve("notes.txt"), "not a document");
        // a schema whose data directory is missing is not one this node holds
        Files.writeString(dbDir.resolve("schemas").resolve("orphan.json"), schemaDefinition.toString());
    }

    @AfterEach
    void clean() throws Exception {
        deleteTree(dbDir);
    }

    private static void deleteTree(Path root) throws Exception {
        if (Files.exists(root)) {
            try (var paths = Files.walk(root)) {
                paths.sorted((a, b) -> b.compareTo(a)).forEach(p -> p.toFile().delete());
            }
        }
    }

    private String bearer(String role) {
        return "Bearer " + jwt.generateToken("u", role, Duration.ofMinutes(5));
    }

    private ResponseEntity<String> get(String pathAndQuery, String authorization) {
        HttpHeaders headers = new HttpHeaders();
        if (authorization != null) {
            headers.set("Authorization", authorization);
        }
        // a URI, not a template string: the percent-escapes below must reach the server as they are written
        return rest.exchange(URI.create("http://localhost:" + port + pathAndQuery), HttpMethod.GET, new HttpEntity<>(headers), String.class);
    }

    @Test
    void schemasListsEverySchemaThisNodeHolds() {
        ResponseEntity<String> response = get("/swim/schemas", bearer("INTERNAL"));

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        JSONArray keys = new JSONArray(response.getBody());
        List<Object> listed = new ArrayList<>(keys.toList());
        assertThat(listed).contains(DB + ".items").doesNotContain(DB + ".orphan");
        assertThat(listed).as("every entry is a db.schema key").allMatch(key -> key.toString().contains("."));
    }

    @Test
    void pullHandsOverEveryDocumentRawIncludingTombstones() {
        ResponseEntity<String> response = get("/swim/pull?database=" + DB + "&schema=items", bearer("INTERNAL"));

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        JSONObject body = new JSONObject(response.getBody());
        assertThat(body.getString("database")).isEqualTo(DB);
        assertThat(body.getString("schema")).isEqualTo("items");
        assertThat(body.getJSONObject("definition").similar(schemaDefinition)).isTrue();
        JSONObject documents = body.getJSONObject("documents");
        assertThat(documents.keySet()).containsExactlyInAnyOrder("0", "1");
        assertThat(documents.getJSONObject("0").similar(live)).as("the live document, exactly as stored").isTrue();
        assertThat(documents.getJSONObject("1").similar(deleted)).as("the tombstone is carried, not hidden").isTrue();
    }

    @Test
    void pullOfASchemaThisNodeDoesNotHoldIsNotFound() {
        assertThat(get("/swim/pull?database=" + DB + "&schema=orphan", bearer("INTERNAL")).getStatusCode())
                .isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(get("/swim/pull?database=nosuchdb&schema=items", bearer("INTERNAL")).getStatusCode())
                .isEqualTo(HttpStatus.NOT_FOUND);
    }

    @Test
    void pullOfADataDirectoryWithNoDefinitionIsNotFound() throws Exception {
        Files.createDirectories(dbDir.resolve("ghost-records"));
        Files.writeString(dbDir.resolve("ghost-records").resolve("0.json"), live.toString());

        assertThat(get("/swim/pull?database=" + DB + "&schema=ghost", bearer("INTERNAL")).getStatusCode())
                .isEqualTo(HttpStatus.NOT_FOUND);
    }

    // A writer truncates a file before it rewrites it, and the pull reads without a lock. The pull must then
    // fail (so the receiver asks someone else), never answer without the file: a snapshot with a document or
    // the definition missing looks whole to the receiver, which would adopt it.

    @Test
    void pullFailsWhileADocumentCannotBeRead() throws Exception {
        Path inFlight = records.resolve("2.json");

        Files.writeString(inFlight, ""); // truncated, not yet rewritten
        assertThat(get("/swim/pull?database=" + DB + "&schema=items", bearer("INTERNAL")).getStatusCode())
                .isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);

        Files.writeString(inFlight, "{\"Name\":\"ann\",\"_ver"); // half written
        assertThat(get("/swim/pull?database=" + DB + "&schema=items", bearer("INTERNAL")).getStatusCode())
                .isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);

        Files.writeString(inFlight, live.toString()); // the write finished: the pull works again
        ResponseEntity<String> done = get("/swim/pull?database=" + DB + "&schema=items", bearer("INTERNAL"));
        assertThat(done.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(new JSONObject(done.getBody()).getJSONObject("documents").keySet()).contains("2");
    }

    @Test
    void pullFailsWhileTheDefinitionCannotBeRead() throws Exception {
        Path definitionFile = dbDir.resolve("schemas").resolve("items.json");

        Files.writeString(definitionFile, ""); // every create rewrites the schema file: this is the window
        assertThat(get("/swim/pull?database=" + DB + "&schema=items", bearer("INTERNAL")).getStatusCode())
                .isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);

        Files.writeString(definitionFile, "{\"info\":{\"schemaName\":\"it");
        assertThat(get("/swim/pull?database=" + DB + "&schema=items", bearer("INTERNAL")).getStatusCode())
                .isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
    }

    @Test
    void pullRefusesNamesThatWouldLeaveTheDataDirectory() {
        for (String query : List.of("database=..&schema=items", "database=" + DB + "&schema=..%2F..%2Finfo",
                "database=%2E%2E%2F%2E%2E&schema=items", "database=&schema=items")) {
            assertThat(get("/swim/pull?" + query, bearer("INTERNAL")).getStatusCode().is4xxClientError())
                    .as(query).isTrue();
        }
        assertThat(get("/swim/pull?database=" + DB + "&schema=..%5Citems", bearer("INTERNAL")).getStatusCode()
                .is4xxClientError()).isTrue();
    }

    @Test
    void bothEndpointsAreForInternalCallersOnly() {
        for (String path : List.of("/swim/schemas", "/swim/pull?database=" + DB + "&schema=items")) {
            assertThat(get(path, bearer("USER")).getStatusCode()).as(path).isEqualTo(HttpStatus.FORBIDDEN);
            assertThat(get(path, bearer("ADMIN")).getStatusCode()).as(path).isEqualTo(HttpStatus.FORBIDDEN);
            assertThat(get(path, null).getStatusCode().is4xxClientError()).as(path).isTrue();
        }
    }
}
