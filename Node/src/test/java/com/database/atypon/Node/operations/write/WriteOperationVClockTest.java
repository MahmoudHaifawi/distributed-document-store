package com.database.atypon.Node.operations.write;

import com.database.atypon.Node.model.Network;
import com.database.atypon.Node.services.index.IndexManager;
import com.database.atypon.Node.utils.response.Response;
import com.database.atypon.Node.utils.response.ResponseType;
import org.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * M8: the origin path stamps a vector clock and a timestamp on every write and turns a delete into a
 * tombstone, and {@code applyReplicated} is the one causal apply that replication goes through: it stores
 * the reconciled winner of the local copy and the incoming one.
 */
class WriteOperationVClockTest {

    private static final String A = "A";

    private final Path db = Paths.get("./data/vcdb");
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

    /** The key this node stamps into a clock: its own name, or "localhost" when it has not joined a network. */
    private static String self() {
        return Network.getSelf() != null ? Network.getSelf().getName() : "localhost";
    }

    private JSONObject stored() throws Exception {
        return new JSONObject(Files.readString(records.resolve("0.json")));
    }

    private void store(JSONObject doc) throws Exception {
        Files.writeString(records.resolve("0.json"), doc.toString());
    }

    private static JSONObject live(int age, int version, int clockA, long ts) {
        return new JSONObject().put("Age", age).put("_version", version)
                .put("_vclock", new JSONObject().put(A, clockA)).put("_ts", ts);
    }

    private static JSONObject tombstone(int version, int clockA, long ts) {
        return new JSONObject().put("_deleted", true).put("_version", version)
                .put("_vclock", new JSONObject().put(A, clockA)).put("_ts", ts);
    }

    // ---- origin path: stamping ------------------------------------------------------------------------

    @Test
    void createStampsAOneEntryClock() throws Exception {
        long before = System.currentTimeMillis();

        var resp = writeOp.createDocument("vcdb", "users", new JSONObject().put("Age", 42));

        assertThat(resp.getResponseType()).isEqualTo(ResponseType.SUCCESS);
        JSONObject doc = stored();
        assertThat(doc.getInt("_version")).isEqualTo(1);
        JSONObject clock = doc.getJSONObject("_vclock");
        assertThat(clock.keySet()).containsExactly(self());
        assertThat(clock.getInt(self())).isEqualTo(1);
        assertThat(doc.getLong("_ts")).isGreaterThanOrEqualTo(before);
        assertThat(doc.optBoolean("_deleted", false)).isFalse();
    }

    @Test
    void updateIncrementsSelfInTheClock() throws Exception {
        writeOp.createDocument("vcdb", "users", new JSONObject().put("Age", 42));
        long createdTs = stored().getLong("_ts");

        var resp = writeOp.updateDocument("vcdb", "users", "0", new JSONObject().put("Age", 50), 1);

        assertThat(resp.getResponseType()).isEqualTo(ResponseType.SUCCESS);
        JSONObject doc = stored();
        assertThat(doc.getInt("_version")).isEqualTo(2);
        assertThat(doc.getJSONObject("_vclock").keySet()).containsExactly(self());
        assertThat(doc.getJSONObject("_vclock").getInt(self())).isEqualTo(2);
        assertThat(doc.getLong("_ts")).isGreaterThanOrEqualTo(createdTs);
    }

    @Test
    void updateKeepsTheOtherNodesEntriesOfTheClock() throws Exception {
        store(live(10, 3, 5, 100)); // clock {A:5}, written by another node

        writeOp.updateDocument("vcdb", "users", "0", new JSONObject().put("Age", 11), 3);

        JSONObject clock = stored().getJSONObject("_vclock");
        assertThat(clock.getInt(A)).isEqualTo(5);
        assertThat(clock.getInt(self())).isEqualTo(1);
    }

    @Test
    void deleteLeavesATombstoneNotAnAbsentFile() throws Exception {
        writeOp.createDocument("vcdb", "users", new JSONObject().put("Age", 42)); // v1, {self:1}
        long before = System.currentTimeMillis();

        var resp = writeOp.deleteDocument("vcdb", "users", "0", 1);

        assertThat(resp.getResponseType()).isEqualTo(ResponseType.SUCCESS);
        assertThat(new File("./data/vcdb/users-records/0.json")).exists();
        JSONObject doc = stored();
        assertThat(doc.getBoolean("_deleted")).isTrue();
        assertThat(doc.getInt("_version")).isEqualTo(2);
        assertThat(doc.getJSONObject("_vclock").getInt(self())).isEqualTo(2);
        assertThat(doc.getLong("_ts")).isGreaterThanOrEqualTo(before);
        assertThat(doc.has("Age")).isFalse(); // a tombstone carries no schema fields

        // the response hands the tombstone to the caller so it can replicate it to the other replicas
        assertThat(new JSONObject((String) resp.getContent()).similar(doc)).isTrue();
        verify(indexManagerMock).onDelete(eq("vcdb"), eq("users"), eq(0), argThat(d -> d.optInt("Age", -1) == 42));
    }

    @Test
    void aTombstonedDocumentCanNeitherBeUpdatedNorDeletedAgain() throws Exception {
        writeOp.createDocument("vcdb", "users", new JSONObject().put("Age", 42));
        writeOp.deleteDocument("vcdb", "users", "0", 1); // tombstone at v2

        var update = writeOp.updateDocument("vcdb", "users", "0", new JSONObject().put("Age", 1), 2);
        var delete = writeOp.deleteDocument("vcdb", "users", "0", 2);

        assertThat(update.getResponseType()).isEqualTo(ResponseType.ERROR);
        assertThat(update.getMessage()).contains("not found");
        assertThat(delete.getResponseType()).isEqualTo(ResponseType.ERROR);
        assertThat(delete.getMessage()).contains("not found");
        assertThat(stored().getBoolean("_deleted")).isTrue();
    }

    @Test
    void reservedKeysAreServerControlledOnTheOriginPath() throws Exception {
        // schema validation ignores the reserved keys, and the origin path then overwrites (or drops) them
        JSONObject forged = new JSONObject().put("Age", 42).put("_deleted", true).put("_ts", 1L)
                .put("_vclock", new JSONObject().put("forged", 99)).put("_version", 77);

        var created = writeOp.createDocument("vcdb", "users", forged);

        assertThat(created.getResponseType()).isEqualTo(ResponseType.SUCCESS);
        JSONObject doc = stored();
        assertThat(doc.has("_deleted")).isFalse();
        assertThat(doc.getInt("_version")).isEqualTo(1);
        assertThat(doc.getJSONObject("_vclock").keySet()).containsExactly(self());
        assertThat(doc.getLong("_ts")).isGreaterThan(1L);

        var updated = writeOp.updateDocument("vcdb", "users", "0",
                new JSONObject().put("Age", 43).put("_deleted", true).put("_vclock", new JSONObject().put("forged", 99)), 1);

        assertThat(updated.getResponseType()).isEqualTo(ResponseType.SUCCESS);
        assertThat(stored().has("_deleted")).isFalse(); // only a delete makes a tombstone
        assertThat(stored().getJSONObject("_vclock").keySet()).containsExactly(self());
        assertThat(stored().getJSONObject("_vclock").getInt(self())).isEqualTo(2);
    }

    @Test
    void createReplicatedKeepsTheStampTheOriginPutOnTheDocument() throws Exception {
        JSONObject fromOrigin = live(42, 1, 1, 123L); // clock {A:1}, stamped by node A

        var resp = writeOp.createReplicatedDocument("vcdb", "users", fromOrigin);

        assertThat(resp.getResponseType()).isEqualTo(ResponseType.SUCCESS);
        JSONObject doc = stored();
        assertThat(doc.getJSONObject("_vclock").keySet()).containsExactly(A); // not re-stamped with this node
        assertThat(doc.getLong("_ts")).isEqualTo(123L);
        assertThat(doc.getInt("_version")).isEqualTo(1);
    }

    @Test
    void createReplicatedStampsADocumentThatArrivesWithoutOne() throws Exception {
        writeOp.createReplicatedDocument("vcdb", "users", new JSONObject().put("Age", 42));

        assertThat(stored().getJSONObject("_vclock").getInt(self())).isEqualTo(1);
        assertThat(stored().getInt("_version")).isEqualTo(1);
    }

    // ---- applyReplicated: causal apply ----------------------------------------------------------------

    @Test
    void applyReplicatedKeepsTheDominatingCopy() throws Exception {
        store(live(10, 1, 1, 100)); // local {A:1}

        Response newer = writeOp.applyReplicated("vcdb", "users", "0", live(20, 2, 2, 200)); // {A:2}
        assertThat(newer.getResponseType()).isEqualTo(ResponseType.SUCCESS);
        assertThat(newer.getContent()).isEqualTo(true); // applied
        assertThat(stored().getInt("Age")).isEqualTo(20);
        assertThat(stored().getJSONObject("_vclock").getInt(A)).isEqualTo(2);

        Response dominated = writeOp.applyReplicated("vcdb", "users", "0", live(99, 1, 1, 999)); // {A:1}
        assertThat(dominated.getResponseType()).isEqualTo(ResponseType.SUCCESS);
        assertThat(dominated.getContent()).isEqualTo(false); // skipped
        assertThat(stored().getInt("Age")).isEqualTo(20); // unchanged
        assertThat(stored().getJSONObject("_vclock").getInt(A)).isEqualTo(2);
    }

    @Test
    void applyReplicatedTombstoneOverLiveTombstonesIt() throws Exception {
        store(live(10, 1, 1, 100)); // local live {A:1}

        Response resp = writeOp.applyReplicated("vcdb", "users", "0", tombstone(2, 2, 200)); // {A:2}

        assertThat(resp.getResponseType()).isEqualTo(ResponseType.SUCCESS);
        assertThat(stored().getBoolean("_deleted")).isTrue();
        assertThat(stored().getInt("_version")).isEqualTo(2);
        // the index loses the live document's keys, so the hook gets the copy that was replaced
        verify(indexManagerMock).onDelete(eq("vcdb"), eq("users"), eq(0), argThat(d -> d.optInt("Age", -1) == 10));
        verify(indexManagerMock, never()).onUpdate(any(), any(), anyInt(), any(), any());
    }

    @Test
    void applyReplicatedLiveCopyThatDominatesATombstoneResurrectsIt() throws Exception {
        store(tombstone(2, 2, 200)); // local tombstone {A:2}

        writeOp.applyReplicated("vcdb", "users", "0", live(30, 3, 3, 300)); // re-created at {A:3}

        assertThat(stored().has("_deleted") && stored().getBoolean("_deleted")).isFalse();
        assertThat(stored().getInt("Age")).isEqualTo(30);
        verify(indexManagerMock).onUpdate(eq("vcdb"), eq("users"), eq(0), any(), argThat(d -> d.getInt("Age") == 30));
    }

    @Test
    void applyReplicatedStoresACopyThatIsAbsentLocally() throws Exception {
        assertThat(new File("./data/vcdb/users-records/0.json")).doesNotExist();

        Response resp = writeOp.applyReplicated("vcdb", "users", "0", live(10, 1, 1, 100));

        assertThat(resp.getResponseType()).isEqualTo(ResponseType.SUCCESS);
        assertThat(resp.getContent()).isEqualTo(true);
        assertThat(stored().getInt("Age")).isEqualTo(10);
        verify(indexManagerMock).onUpdate(eq("vcdb"), eq("users"), eq(0), any(), argThat(d -> d.getInt("Age") == 10));
    }

    @Test
    void applyReplicatedStoresATombstoneThatIsAbsentLocallyWithoutValidatingIt() throws Exception {
        // a tombstone has no schema fields; replication is the trusted path and never schema-validates
        Response resp = writeOp.applyReplicated("vcdb", "users", "0", tombstone(2, 2, 200));

        assertThat(resp.getResponseType()).isEqualTo(ResponseType.SUCCESS);
        assertThat(stored().getBoolean("_deleted")).isTrue();
    }

    @Test
    void applyReplicatedOfAnIdenticalCopyIsANoOpAndFiresNoIndexHook() throws Exception {
        store(live(10, 1, 1, 100));

        Response resp = writeOp.applyReplicated("vcdb", "users", "0", live(10, 1, 1, 100));

        assertThat(resp.getResponseType()).isEqualTo(ResponseType.SUCCESS);
        assertThat(resp.getContent()).isEqualTo(false);
        verify(indexManagerMock, never()).onUpdate(any(), any(), anyInt(), any(), any());
        verify(indexManagerMock, never()).onDelete(any(), any(), anyInt(), any());
    }

    @Test
    void applyReplicatedOfAConcurrentCopyMergesTheClocks() throws Exception {
        store(live(10, 1, 1, 100)); // {A:1}
        JSONObject incoming = new JSONObject().put("Age", 20).put("_version", 1) // {B:1}, newer wall clock
                .put("_vclock", new JSONObject().put("B", 1)).put("_ts", 500L);

        writeOp.applyReplicated("vcdb", "users", "0", incoming);

        JSONObject doc = stored();
        assertThat(doc.getInt("Age")).isEqualTo(20); // latest _ts wins a true conflict
        assertThat(doc.getJSONObject("_vclock").getInt(A)).isEqualTo(1); // ...but the stored clock
        assertThat(doc.getJSONObject("_vclock").getInt("B")).isEqualTo(1); // dominates both inputs
    }

    @Test
    void applyUpdateAndApplyDeleteGoThroughTheSameCausalApply() throws Exception {
        store(live(10, 1, 1, 100));

        writeOp.applyUpdate("vcdb", "users", "0", live(5, 1, 1, 50)); // equal clock, local wins
        assertThat(stored().getInt("Age")).isEqualTo(10);

        writeOp.applyDelete("vcdb", "users", "0");
        assertThat(new File("./data/vcdb/users-records/0.json")).exists(); // a tombstone, not an absent file
        assertThat(stored().getBoolean("_deleted")).isTrue();
        assertThat(stored().getJSONObject("_vclock").getInt(A)).isEqualTo(1);
        assertThat(stored().getJSONObject("_vclock").getInt(self())).isEqualTo(1);

        verify(indexManagerMock, times(1)).onDelete(eq("vcdb"), eq("users"), eq(0), any());
    }
}
