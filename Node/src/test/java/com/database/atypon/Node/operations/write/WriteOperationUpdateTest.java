package com.database.atypon.Node.operations.write;

import com.database.atypon.Node.services.index.IndexManager;
import com.database.atypon.Node.utils.response.ResponseType;
import org.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class WriteOperationUpdateTest {

    private final Path db = Paths.get("./data/updb");
    private IndexManager indexManagerMock;
    private WriteOperation writeOp;

    @BeforeEach
    void setUp() throws Exception {
        Files.createDirectories(db.resolve("schemas"));
        Files.createDirectories(db.resolve("users-records"));
        Files.writeString(db.resolve("schemas").resolve("users.json"),
                new JSONObject().put("info", new JSONObject().put("schemaName", "users").put("nextId", 0))
                        .put("schema", new JSONObject().put("Age", "Integer")).toString());
        indexManagerMock = mock(IndexManager.class);
        writeOp = new WriteOperation(indexManagerMock);
        // seed one document at version 1
        writeOp.createDocument("updb", "users", new JSONObject().put("Age", 42));
    }

    @AfterEach
    void cleanup() throws Exception {
        if (Files.exists(db)) {
            try (var paths = Files.walk(db)) {
                paths.sorted((a, b) -> b.compareTo(a)).forEach(p -> p.toFile().delete());
            }
        }
    }

    @Test
    void updateWithMatchingVersionIncrementsAndWrites() throws Exception {
        var resp = writeOp.updateDocument("updb", "users", "0", new JSONObject().put("Age", 50), 1);

        assertThat(resp.getResponseType()).isEqualTo(ResponseType.SUCCESS);
        assertThat(((Number) resp.getContent()).intValue()).isEqualTo(2);
        JSONObject stored = new JSONObject(Files.readString(db.resolve("users-records").resolve("0.json")));
        assertThat(stored.getInt("Age")).isEqualTo(50);
        assertThat(stored.getInt("_version")).isEqualTo(2);
        verify(indexManagerMock).onUpdate(eq("updb"), eq("users"), eq(0), any(), any());
    }

    @Test
    void updateWithStaleVersionConflictsAndDoesNotWrite() throws Exception {
        writeOp.updateDocument("updb", "users", "0", new JSONObject().put("Age", 50), 1); // -> v2
        var resp = writeOp.updateDocument("updb", "users", "0", new JSONObject().put("Age", 99), 1); // stale

        assertThat(resp.getResponseType()).isEqualTo(ResponseType.ERROR);
        assertThat(((Number) resp.getContent()).intValue()).isEqualTo(2); // current version
        JSONObject stored = new JSONObject(Files.readString(db.resolve("users-records").resolve("0.json")));
        assertThat(stored.getInt("Age")).isEqualTo(50); // unchanged
        assertThat(stored.getInt("_version")).isEqualTo(2);
    }

    @Test
    void updateMissingDocumentReturnsNotFound() throws Exception {
        var resp = writeOp.updateDocument("updb", "users", "999", new JSONObject().put("Age", 1), 1);
        assertThat(resp.getResponseType()).isEqualTo(ResponseType.ERROR);
        assertThat(resp.getMessage()).contains("not found");
    }

    @Test
    void updateFailingValidationDoesNotWrite() throws Exception {
        var resp = writeOp.updateDocument("updb", "users", "0", new JSONObject().put("Age", "notAnInt"), 1);
        assertThat(resp.getResponseType()).isEqualTo(ResponseType.ERROR);
        JSONObject stored = new JSONObject(Files.readString(db.resolve("users-records").resolve("0.json")));
        assertThat(stored.getInt("Age")).isEqualTo(42); // original untouched
    }

    @Test
    void applyUpdateWritesTheDominatingCopyWithoutVersionCheck() throws Exception {
        // M8: applyUpdate is a causal reconcile, not a blind upsert, so the incoming copy must descend the local
        // one ({localhost:1}) to be stored; the version is then taken from the document, not incremented
        JSONObject local = new JSONObject(Files.readString(db.resolve("users-records").resolve("0.json")));
        JSONObject newer = new JSONObject().put("Age", 7).put("_version", 9)
                .put("_vclock", local.getJSONObject("_vclock").put("replica", 4)).put("_ts", 1L);
        var resp = writeOp.applyUpdate("updb", "users", "0", newer);
        assertThat(resp.getResponseType()).isEqualTo(ResponseType.SUCCESS);
        JSONObject stored = new JSONObject(Files.readString(db.resolve("users-records").resolve("0.json")));
        assertThat(stored.getInt("Age")).isEqualTo(7);
        assertThat(stored.getInt("_version")).isEqualTo(9); // taken from the document, not incremented
    }
}
