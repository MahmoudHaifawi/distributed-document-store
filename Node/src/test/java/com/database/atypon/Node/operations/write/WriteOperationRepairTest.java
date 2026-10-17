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
 * Read-repair hands a replica the winning copy of a document, and the replica applies it through the same
 * causal reconcile as replication ({@code applyReplicated}). So it is neither a blind overwrite nor a
 * version compare: a winner that descends the stored copy is stored, a tombstone winner tombstones a copy it
 * dominates, an absent copy is healed, and a winner the stored copy already dominates is left alone.
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

    /** A live document at {@code version} whose clock is the single entry {@code node:version}. */
    private static JSONObject doc(int age, int version, String node, long ts) {
        return new JSONObject().put("Age", age).put("_version", version)
                .put("_vclock", new JSONObject().put(node, version)).put("_ts", ts);
    }

    private static JSONObject tombstone(int version, String node, long ts) {
        return new JSONObject().put("_deleted", true).put("_version", version)
                .put("_vclock", new JSONObject().put(node, version)).put("_ts", ts);
    }

    private void store(JSONObject doc) throws Exception {
        Files.writeString(records.resolve("0.json"), doc.toString());
    }

    private JSONObject stored() throws Exception {
        return new JSONObject(Files.readString(records.resolve("0.json")));
    }

    @Test
    void repairUpgradesADocumentTheWinnerDescends() throws Exception {
        store(doc(10, 1, "n1", 1_000));

        var resp = writeOp.repairDocument("repairdb", "users", "0", doc(30, 3, "n1", 3_000));

        assertThat(resp.getResponseType()).isEqualTo(ResponseType.SUCCESS);
        assertThat(resp.getContent()).isEqualTo(true); // applied
        assertThat(stored().getInt("Age")).isEqualTo(30);
        assertThat(stored().getInt("_version")).isEqualTo(3); // taken from the winner, not incremented
        assertThat(stored().getJSONObject("_vclock").getInt("n1")).isEqualTo(3);
        verify(indexManagerMock).onUpdate(eq("repairdb"), eq("users"), eq(0), any(), any());
    }

    @Test
    void repairHealsADocumentThatIsAbsent() throws Exception {
        // the replica missed the create: with deletes recorded as tombstones, an absent file was never deleted
        var resp = writeOp.repairDocument("repairdb", "users", "0", doc(30, 3, "n1", 3_000));

        assertThat(resp.getResponseType()).isEqualTo(ResponseType.SUCCESS);
        assertThat(resp.getContent()).isEqualTo(true);
        assertThat(stored().getInt("Age")).isEqualTo(30);
        assertThat(stored().getInt("_version")).isEqualTo(3);
        verify(indexManagerMock).onUpdate(eq("repairdb"), eq("users"), eq(0), any(), any());
    }

    @Test
    void aTombstoneWinnerTombstonesTheLiveCopyItDominates() throws Exception {
        store(doc(10, 1, "n1", 1_000));

        var resp = writeOp.repairDocument("repairdb", "users", "0", tombstone(2, "n1", 2_000));

        assertThat(resp.getResponseType()).isEqualTo(ResponseType.SUCCESS);
        assertThat(resp.getContent()).isEqualTo(true);
        assertThat(stored().getBoolean("_deleted")).isTrue();
        assertThat(stored().has("Age")).isFalse();
        assertThat(stored().getInt("_version")).isEqualTo(2);
        verify(indexManagerMock).onDelete(eq("repairdb"), eq("users"), eq(0), any());
        verify(indexManagerMock, never()).onUpdate(any(), any(), anyInt(), any(), any());
    }

    @Test
    void aTombstoneWinnerIsStoredOnAReplicaThatNeverHeldTheDocumentWithoutSchemaValidation() throws Exception {
        // a tombstone carries none of the schema's fields, so validating it would reject every delete
        var resp = writeOp.repairDocument("repairdb", "users", "0", tombstone(2, "n1", 2_000));

        assertThat(resp.getResponseType()).isEqualTo(ResponseType.SUCCESS);
        assertThat(resp.getContent()).isEqualTo(true);
        assertThat(stored().getBoolean("_deleted")).isTrue();
    }

    @Test
    void repairNeverOverwritesADocumentWithTheSameClock() throws Exception {
        store(doc(10, 3, "n1", 3_000));

        var resp = writeOp.repairDocument("repairdb", "users", "0", doc(30, 3, "n1", 3_000));

        assertThat(resp.getResponseType()).isEqualTo(ResponseType.SUCCESS);
        assertThat(resp.getContent()).isEqualTo(false); // skipped: nothing to change
        assertThat(stored().getInt("Age")).isEqualTo(10);
        assertThat(stored().getInt("_version")).isEqualTo(3);
        verify(indexManagerMock, never()).onUpdate(any(), any(), anyInt(), any(), any());
    }

    @Test
    void repairNeverRegressesADocumentThatDominatesTheWinner() throws Exception {
        store(doc(99, 4, "n1", 4_000)); // a newer write landed here after the read gathered its copies

        var resp = writeOp.repairDocument("repairdb", "users", "0", doc(30, 3, "n1", 3_000));

        assertThat(resp.getResponseType()).isEqualTo(ResponseType.SUCCESS);
        assertThat(resp.getContent()).isEqualTo(false);
        assertThat(stored().getInt("Age")).isEqualTo(99);
        assertThat(stored().getInt("_version")).isEqualTo(4);
        verify(indexManagerMock, never()).onUpdate(any(), any(), anyInt(), any(), any());
    }

    @Test
    void aStaleTombstoneCannotDeleteALiveCopyThatSupersedesIt() throws Exception {
        store(doc(99, 4, "n1", 4_000)); // re-created / updated after the delete the winner records

        var resp = writeOp.repairDocument("repairdb", "users", "0", tombstone(2, "n1", 2_000));

        assertThat(resp.getContent()).isEqualTo(false);
        assertThat(stored().optBoolean("_deleted", false)).isFalse();
        assertThat(stored().getInt("Age")).isEqualTo(99);
        verify(indexManagerMock, never()).onDelete(any(), any(), anyInt(), any());
    }

    @Test
    void aConcurrentLosingWinnerLeavesTheStoredContentAndOnlyMergesTheClock() throws Exception {
        store(doc(10, 1, "n1", 9_000));  // the later of two concurrent writes

        var resp = writeOp.repairDocument("repairdb", "users", "0", doc(30, 1, "n2", 1_000));

        assertThat(resp.getResponseType()).isEqualTo(ResponseType.SUCCESS);
        assertThat(stored().getInt("Age")).isEqualTo(10);
        assertThat(stored().optBoolean("_deleted", false)).isFalse();
        // the copy now records that it has seen the loser, so the loser can never read as unseen again
        assertThat(stored().getJSONObject("_vclock").getInt("n1")).isEqualTo(1);
        assertThat(stored().getJSONObject("_vclock").getInt("n2")).isEqualTo(1);
    }

    @Test
    void aStoredDocumentWithNoClockIsUpgradedByAWinnerWithOne() throws Exception {
        Files.writeString(records.resolve("0.json"), new JSONObject().put("Age", 10).toString());

        var resp = writeOp.repairDocument("repairdb", "users", "0", doc(30, 2, "n1", 2_000));

        assertThat(resp.getContent()).isEqualTo(true);
        assertThat(stored().getInt("Age")).isEqualTo(30);
        assertThat(stored().getInt("_version")).isEqualTo(2);
    }
}
