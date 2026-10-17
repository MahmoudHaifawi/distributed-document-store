package com.database.atypon.Node.services.write;

import com.database.atypon.Node.model.Network;
import com.database.atypon.Node.operations.write.WriteOperation;
import com.database.atypon.Node.services.index.IndexManager;
import com.database.atypon.Node.utils.response.ResponseType;
import org.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * The origin stamps a write's version, vector clock and timestamp, and replicates the very map it was handed.
 * So after a successful origin create or update the service must copy that stamp back onto the map; otherwise
 * peers would receive an unstamped document and a causal apply would drop it as older than their own copy.
 */
class WriteServiceStampTest {

    private final Path db = Paths.get("./data/stampdb");
    private WriteService service;

    @BeforeEach
    void setUp() throws Exception {
        Files.createDirectories(db.resolve("schemas"));
        Files.createDirectories(db.resolve("users-records"));
        Files.writeString(db.resolve("schemas").resolve("users.json"),
                new JSONObject().put("info", new JSONObject().put("schemaName", "users").put("nextId", 0))
                        .put("schema", new JSONObject().put("Age", "Integer")).toString());
        service = new WriteService(new WriteOperation(mock(IndexManager.class)));
    }

    @AfterEach
    void cleanup() throws Exception {
        if (Files.exists(db)) {
            try (var paths = Files.walk(db)) {
                paths.sorted((a, b) -> b.compareTo(a)).forEach(p -> p.toFile().delete());
            }
        }
    }

    private static String self() {
        return Network.getSelf() != null ? Network.getSelf().getName() : "localhost";
    }

    @Test
    void anOriginCreateCopiesItsStampBackOntoTheMapToReplicate() {
        HashMap<String, Object> document = new HashMap<>(Map.of("Age", 42));

        var resp = service.createDocument("stampdb", "users", document);

        assertThat(resp.getResponseType()).isEqualTo(ResponseType.SUCCESS);
        assertThat(document).containsEntry("_version", 1).containsKey("_ts");
        assertThat(document.get("_vclock")).isEqualTo(Map.of(self(), 1));
    }

    @Test
    void anOriginUpdateCopiesItsStampBackOntoTheMapToReplicate() {
        service.createDocument("stampdb", "users", new HashMap<>(Map.of("Age", 42)));
        HashMap<String, Object> document = new HashMap<>(Map.of("Age", 50));

        var resp = service.updateDocument("stampdb", "users", "0", document, 1);

        assertThat(resp.getResponseType()).isEqualTo(ResponseType.SUCCESS);
        assertThat(document).containsEntry("_version", 2).containsKey("_ts");
        assertThat(document.get("_vclock")).isEqualTo(Map.of(self(), 2));
    }

    @Test
    void aFailedOriginWriteLeavesTheMapUnstamped() {
        HashMap<String, Object> document = new HashMap<>(Map.of("Age", "notAnInt"));

        var resp = service.createDocument("stampdb", "users", document);

        assertThat(resp.getResponseType()).isEqualTo(ResponseType.ERROR);
        assertThat(document).doesNotContainKeys("_version", "_vclock", "_ts");
    }

    @Test
    void aStampedMapRoundTripsThroughAReplicasCausalApply() {
        // the primary's create + update, replicated verbatim to a replica that holds the created copy
        HashMap<String, Object> created = new HashMap<>(Map.of("Age", 42));
        service.createDocument("stampdb", "users", created);
        HashMap<String, Object> updated = new HashMap<>(Map.of("Age", 50));
        service.updateDocument("stampdb", "users", "0", updated, 1);

        // the replica still has the version-1 copy; the stamped update must reconcile as newer and win
        service.createReplicatedDocument("stampdb", "users", created); // a second doc (id 1) holding the v1 copy
        var applied = service.applyUpdate("stampdb", "users", "1", updated);

        assertThat(applied.getResponseType()).isEqualTo(ResponseType.SUCCESS);
        assertThat(applied.getContent()).isEqualTo(true);
    }
}
