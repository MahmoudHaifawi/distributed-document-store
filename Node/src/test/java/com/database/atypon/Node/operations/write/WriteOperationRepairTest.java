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
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * Read-repair writes the freshest copy of a document onto a replica that holds an older one. Unlike the
 * verbatim {@code applyUpdate}, it must be conditional: it may only upgrade a document that is still there
 * and still older, so a repair racing a delete or a newer write can neither resurrect nor regress anything.
 */
class WriteOperationRepairTest {

    private final Path db = Paths.get("./data/repairdb");
    private final Path records = db.resolve("users-records");
    private IndexManager indexManagerMock;
    private WriteOperation writeOp;

    @BeforeEach
    void setUp() throws Exception {
        Files.createDirectories(db.resolve("schemas"));
        Files.createDirectories(records);
        Files.writeString(db.resolve("schemas").resolve("users.json"),
                new JSONObject().put("info", new JSONObject().put("schemaName", "users").put("nextId", 0))
                        .put("schema", new JSONObject().put("Age", "Integer")).toString());
        indexManagerMock = mock(IndexManager.class);
        writeOp = new WriteOperation(indexManagerMock);
    }

    @AfterEach
    void cleanup() throws Exception {
        if (Files.exists(db)) {
            try (var paths = Files.walk(db)) {
                paths.sorted((a, b) -> b.compareTo(a)).forEach(p -> p.toFile().delete());
            }
        }
    }

    private void store(int age, int version) throws Exception {
        Files.writeString(records.resolve("0.json"), doc(age, version).toString());
    }

    private static JSONObject doc(int age, int version) {
        return new JSONObject().put("Age", age).put("_version", version);
    }

    private JSONObject stored() throws Exception {
        return new JSONObject(Files.readString(records.resolve("0.json")));
    }

    @Test
    void repairUpgradesAPresentOlderDocument() throws Exception {
        store(10, 1);

        var resp = writeOp.repairDocument("repairdb", "users", "0", doc(30, 3));

        assertThat(resp.getResponseType()).isEqualTo(ResponseType.SUCCESS);
        assertThat(resp.getContent()).isEqualTo(true); // applied
        assertThat(stored().getInt("Age")).isEqualTo(30);
        assertThat(stored().getInt("_version")).isEqualTo(3); // taken from the winner, not incremented
        verify(indexManagerMock).onUpdate(eq("repairdb"), eq("users"), eq(0), any(), any());
    }

    @Test
    void repairNeverCreatesADocumentThatIsAbsent() throws Exception {
        // the document was deleted (or never copied) here: repairing it would bring it back from the dead
        var resp = writeOp.repairDocument("repairdb", "users", "0", doc(30, 3));

        assertThat(resp.getResponseType()).isEqualTo(ResponseType.SUCCESS);
        assertThat(resp.getContent()).isEqualTo(false); // skipped
        assertThat(Files.exists(records.resolve("0.json"))).isFalse();
        verify(indexManagerMock, never()).onUpdate(any(), any(), anyInt(), any(), any());
    }

    @Test
    void repairNeverOverwritesADocumentAtTheSameVersion() throws Exception {
        store(10, 3);

        var resp = writeOp.repairDocument("repairdb", "users", "0", doc(30, 3));

        assertThat(resp.getContent()).isEqualTo(false);
        assertThat(stored().getInt("Age")).isEqualTo(10);
        assertThat(stored().getInt("_version")).isEqualTo(3);
        verify(indexManagerMock, never()).onUpdate(any(), any(), anyInt(), any(), any());
    }

    @Test
    void repairNeverRegressesADocumentThatIsNewerThanTheWinner() throws Exception {
        store(99, 4); // a newer write landed here after the read gathered its copies

        var resp = writeOp.repairDocument("repairdb", "users", "0", doc(30, 3));

        assertThat(resp.getResponseType()).isEqualTo(ResponseType.SUCCESS);
        assertThat(resp.getContent()).isEqualTo(false);
        assertThat(stored().getInt("Age")).isEqualTo(99);
        assertThat(stored().getInt("_version")).isEqualTo(4);
        verify(indexManagerMock, never()).onUpdate(any(), any(), anyInt(), any(), any());
    }

    @Test
    void aStoredDocumentWithNoVersionCountsAsVersionOne() throws Exception {
        Files.writeString(records.resolve("0.json"), new JSONObject().put("Age", 10).toString());

        var resp = writeOp.repairDocument("repairdb", "users", "0", doc(30, 2));

        assertThat(resp.getContent()).isEqualTo(true);
        assertThat(stored().getInt("_version")).isEqualTo(2);
    }

    @Test
    void repairFailingValidationDoesNotWrite() throws Exception {
        store(10, 1);

        var resp = writeOp.repairDocument("repairdb", "users", "0",
                new JSONObject().put("Age", "notAnInt").put("_version", 3));

        assertThat(resp.getResponseType()).isEqualTo(ResponseType.ERROR);
        assertThat(stored().getInt("Age")).isEqualTo(10);
        assertThat(stored().getInt("_version")).isEqualTo(1);
    }
}
