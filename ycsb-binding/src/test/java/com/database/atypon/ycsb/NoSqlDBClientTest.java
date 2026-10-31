package com.database.atypon.ycsb;

import com.database.atypon.ycsb.StubGateway.Request;
import org.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import site.ycsb.ByteIterator;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

/**
 * Drives the YCSB binding against a stub of the data node's HTTP API and asserts both the {@link Status} it
 * returns and the exact calls it makes: the record key is a reserved indexed field ({@code ykey}), so every
 * read, update and delete is "index point-query, then by-id".
 */
class NoSqlDBClientTest {

    private static final String TABLE = "usertable"; // YCSB's table name; the binding maps it onto schema "ycsb"

    private StubGateway gateway;
    private final List<NoSqlDBClient> opened = new ArrayList<>();
    private NoSqlDBClient client;

    @BeforeEach
    void start() throws Exception {
        NoSqlDBClient.resetSetupGuard();
        gateway = new StubGateway();
        client = open(StubGateway.PASSWORD);
    }

    @AfterEach
    void stop() throws Exception {
        for (NoSqlDBClient c : opened) {
            c.cleanup();
        }
        gateway.close();
    }

    private NoSqlDBClient open(String password) throws DBException {
        Properties props = new Properties();
        props.setProperty("gateway.url", gateway.url());
        props.setProperty("nosql.username", StubGateway.USER);
        props.setProperty("nosql.password", password);
        props.setProperty("fieldcount", "3");
        NoSqlDBClient c = new NoSqlDBClient();
        c.setProperties(props);
        c.init();
        opened.add(c);
        return c;
    }

    private static Map<String, ByteIterator> values(String f0, String f1, String f2) {
        Map<String, ByteIterator> m = new HashMap<>();
        m.put("field0", new StringByteIterator(f0));
        m.put("field1", new StringByteIterator(f1));
        m.put("field2", new StringByteIterator(f2));
        return m;
    }

    private static Map<String, String> strings(Map<String, ByteIterator> result) {
        return StringByteIterator.getStringMap(result);
    }

    // ---- the five contract tests ----

    @Test
    void insertThenReadReturnsValues() {
        assertThat(client.insert(TABLE, "user1", values("a", "b", "c"))).isEqualTo(Status.OK);

        Map<String, ByteIterator> result = new HashMap<>();
        assertThat(client.read(TABLE, "user1", null, result)).isEqualTo(Status.OK);

        // the record's own fields only: the reserved key and the server's bookkeeping do not leak into the row
        assertThat(strings(result)).containsOnly(entry("field0", "a"), entry("field1", "b"), entry("field2", "c"));

        // the create: key + values in one document, to the ycsb schema, authenticated
        Request create = gateway.only("POST", "/write/document/new");
        assertThat(create.query()).containsOnly(entry("database", "ycsb"), entry("schema", "ycsb"));
        assertThat(create.authorization()).isEqualTo("Bearer " + StubGateway.TOKEN);
        JSONObject sent = create.json();
        assertThat(sent.getString("ykey")).isEqualTo("user1");
        assertThat(sent.getString("field0")).isEqualTo("a");

        // the read: an index point-query on ykey, then the by-id read of the id it returned
        Request query = gateway.only("POST", "/user/index/query");
        assertThat(query.authorization()).isEqualTo("Bearer " + StubGateway.TOKEN);
        JSONObject q = query.json();
        assertThat(q.getString("database")).isEqualTo("ycsb");
        assertThat(q.getString("schema")).isEqualTo("ycsb");
        assertThat(q.getString("field")).isEqualTo("ykey");
        assertThat(q.getString("op")).isEqualTo("EQ");
        assertThat(q.getString("value")).isEqualTo("user1");

        Request byId = gateway.only("GET", "/user/read/document");
        assertThat(byId.authorization()).isEqualTo("Bearer " + StubGateway.TOKEN);
        assertThat(byId.query()).containsOnly(entry("id", "0"), entry("databaseName", "ycsb"), entry("schemaName", "ycsb"));
        assertThat(gateway.requests().indexOf(query)).isLessThan(gateway.requests().indexOf(byId));
    }

    @Test
    void updateChangesValues() {
        client.insert(TABLE, "user1", values("a", "b", "c"));

        Map<String, ByteIterator> change = new HashMap<>();
        change.put("field1", new StringByteIterator("B2"));
        assertThat(client.update(TABLE, "user1", change)).isEqualTo(Status.OK);

        Map<String, ByteIterator> result = new HashMap<>();
        assertThat(client.read(TABLE, "user1", null, result)).isEqualTo(Status.OK);
        // the changed field is new, the others are as inserted
        assertThat(strings(result)).containsOnly(entry("field0", "a"), entry("field1", "B2"), entry("field2", "c"));

        // the update replaces the whole document, so it carries the key and every field, and the version
        // it was read at (optimistic locking)
        Request update = gateway.only("POST", "/write/document/update");
        assertThat(update.query()).containsOnly(entry("database", "ycsb"), entry("schema", "ycsb"), entry("id", "0"));
        assertThat(update.authorization()).isEqualTo("Bearer " + StubGateway.TOKEN);
        JSONObject sent = update.json();
        assertThat(sent.getString("ykey")).isEqualTo("user1");
        assertThat(sent.getString("field0")).isEqualTo("a");
        assertThat(sent.getString("field1")).isEqualTo("B2");
        assertThat(sent.getString("field2")).isEqualTo("c");
        assertThat(sent.getInt("_version")).isEqualTo(1);
    }

    @Test
    void deleteThenReadIsNotFound() {
        client.insert(TABLE, "user1", values("a", "b", "c"));

        assertThat(client.delete(TABLE, "user1")).isEqualTo(Status.OK);

        // the delete is by id, at the version the document was read at
        Request delete = gateway.only("POST", "/write/document/delete");
        assertThat(delete.query()).containsOnly(entry("database", "ycsb"), entry("schema", "ycsb"),
                entry("id", "0"), entry("version", "1"));
        assertThat(delete.authorization()).isEqualTo("Bearer " + StubGateway.TOKEN);

        assertThat(client.read(TABLE, "user1", null, new HashMap<>())).isEqualTo(Status.NOT_FOUND);
        assertThat(client.delete(TABLE, "user1")).isEqualTo(Status.NOT_FOUND);
        assertThat(client.update(TABLE, "user1", values("x", "y", "z"))).isEqualTo(Status.NOT_FOUND);
    }

    @Test
    void readMissingKeyIsNotFound() {
        Map<String, ByteIterator> result = new HashMap<>();
        assertThat(client.read(TABLE, "nobody", null, result)).isEqualTo(Status.NOT_FOUND);
        assertThat(result).isEmpty();

        // the index said "no such key", so there was no document to fetch
        assertThat(gateway.calls("POST", "/user/index/query")).hasSize(1);
        assertThat(gateway.calls("GET", "/user/read/document")).isEmpty();
    }

    /** The node's own answer for an absent document: HTTP 200 with an ERROR envelope. */
    private static final String DOCUMENT_NOT_FOUND =
            "{\"responseType\":\"ERROR\",\"message\":\"Document not found\"}";

    @Test
    void aDocumentRemovedBetweenTheIndexAndTheFetchIsNotFound() {
        client.insert(TABLE, "user1", values("a", "b", "c"));
        // the index still has the id but the by-id read reports the document gone: the node's genuine miss
        gateway.forceAnswer("GET", "/user/read/document", 200, DOCUMENT_NOT_FOUND);

        assertThat(client.read(TABLE, "user1", null, new HashMap<>())).isEqualTo(Status.NOT_FOUND);
        assertThat(client.delete(TABLE, "user1")).isEqualTo(Status.NOT_FOUND);
        assertThat(client.update(TABLE, "user1", values("x", "y", "z"))).isEqualTo(Status.NOT_FOUND);
        Vector<HashMap<String, ByteIterator>> rows = new Vector<>();
        assertThat(client.scan(TABLE, "user1", 5, null, rows)).isEqualTo(Status.OK); // a vanished row is skipped
        assertThat(rows).isEmpty();
    }

    @Test
    void aFailureOnTheByIdStepIsAnErrorNotAMiss() {
        client.insert(TABLE, "user1", values("a", "b", "c"));

        String[][] failures = {
                // a Spring 404 page: the URL is wrong or the route is gone, not "the document is missing"
                {"404", "{\"timestamp\":\"t\",\"status\":404,\"error\":\"Not Found\",\"path\":\"/user/read/document\"}"},
                {"404", "Not Found"},
                // an I/O failure on the node
                {"500", "{\"status\":500,\"error\":\"Internal Server Error\",\"message\":\"File not found\"}"},
                // the node's own ERROR envelope, but not its missing-document answer
                {"200", "{\"responseType\":\"ERROR\",\"message\":\"File not found\"}"},
                {"200", "{\"responseType\":\"ERROR\",\"message\":\"Schema not found\"}"},
                // not an envelope at all
                {"200", "<html>Not Found</html>"},
                {"200", "{\"status\":404,\"error\":\"Not Found\"}"},
        };
        for (String[] failure : failures) {
            gateway.forceAnswer("GET", "/user/read/document", Integer.parseInt(failure[0]), failure[1]);
            String answer = failure[0] + " " + failure[1];

            assertThat(client.read(TABLE, "user1", null, new HashMap<>())).as("read, " + answer).isEqualTo(Status.ERROR);
            assertThat(client.update(TABLE, "user1", values("x", "y", "z"))).as("update, " + answer).isEqualTo(Status.ERROR);
            assertThat(client.delete(TABLE, "user1")).as("delete, " + answer).isEqualTo(Status.ERROR);
            assertThat(client.scan(TABLE, "user1", 5, null, new Vector<>())).as("scan, " + answer).isEqualTo(Status.ERROR);
        }
        gateway.clearForcedAnswers();
        assertThat(client.read(TABLE, "user1", null, new HashMap<>())).isEqualTo(Status.OK); // nothing was lost
    }

    @Test
    void aWriteRejectedWithAnHttpErrorIsAnErrorNotAMiss() {
        client.insert(TABLE, "user1", values("a", "b", "c"));

        gateway.forceAnswer("POST", "/write/document/update", 404, "{\"status\":404,\"error\":\"Not Found\"}");
        assertThat(client.update(TABLE, "user1", values("x", "y", "z"))).isEqualTo(Status.ERROR);

        gateway.forceAnswer("POST", "/write/document/delete", 404, "{\"status\":404,\"error\":\"Not Found\"}");
        assertThat(client.delete(TABLE, "user1")).isEqualTo(Status.ERROR);

        gateway.forceAnswer("POST", "/user/index/query", 404, "{\"status\":404,\"error\":\"Not Found\"}");
        assertThat(client.read(TABLE, "user1", null, new HashMap<>())).isEqualTo(Status.ERROR);
    }

    @Test
    void keyMapsToExactlyOneDocument() {
        client.insert(TABLE, "user1", values("a", "b", "c"));
        client.insert(TABLE, "user2", values("d", "e", "f"));

        // the key resolves through the index to exactly one id, and the ids of different keys differ
        assertThat(client.lookupIds("user1")).containsExactly(0);
        assertThat(client.lookupIds("user2")).containsExactly(1);
        assertThat(client.lookupIds("user3")).isEmpty();
        assertThat(gateway.liveDocumentsWithKey("user1")).hasSize(1);

        // updating and reading never makes a second document for the key
        client.update(TABLE, "user1", values("x", "y", "z"));
        client.read(TABLE, "user1", null, new HashMap<>());
        assertThat(client.lookupIds("user1")).containsExactly(0);
        assertThat(gateway.liveDocumentsWithKey("user1")).hasSize(1);
        assertThat(gateway.calls("POST", "/write/document/new")).hasSize(2); // one create per inserted key
    }

    // ---- field selection, scan ----

    @Test
    void readReturnsOnlyTheRequestedFields() {
        client.insert(TABLE, "user1", values("a", "b", "c"));

        Map<String, ByteIterator> result = new HashMap<>();
        assertThat(client.read(TABLE, "user1", Set.of("field2"), result)).isEqualTo(Status.OK);
        assertThat(strings(result)).containsOnly(entry("field2", "c"));
    }

    @Test
    void scanReturnsRecordsFromTheStartKeyInKeyOrder() {
        for (String key : List.of("user3", "user1", "user5", "user2", "user4")) {
            client.insert(TABLE, key, values(key + "-a", key + "-b", key + "-c"));
        }

        Vector<HashMap<String, ByteIterator>> result = new Vector<>();
        assertThat(client.scan(TABLE, "user2", 3, null, result)).isEqualTo(Status.OK);

        assertThat(result).hasSize(3);
        assertThat(result.stream().map(row -> row.get("field0").toString()))
                .containsExactly("user2-a", "user3-a", "user4-a");

        // a bounded range query on the index: ykey >= start, ascending, at most recordcount ids
        JSONObject q = gateway.only("POST", "/user/index/query").json();
        assertThat(q.getString("field")).isEqualTo("ykey");
        assertThat(q.getString("op")).isEqualTo("GTE");
        assertThat(q.getString("value")).isEqualTo("user2");
        assertThat(q.getString("order")).isEqualTo("ASC");
        assertThat(q.getInt("limit")).isEqualTo(3);
    }

    // ---- concurrency, auth, setup ----

    @Test
    void updateRetriesWhenAConcurrentWriterBumpedTheVersion() {
        client.insert(TABLE, "user1", values("a", "b", "c"));
        gateway.injectVersionConflicts(1);

        Map<String, ByteIterator> change = new HashMap<>();
        change.put("field0", new StringByteIterator("mine"));
        assertThat(client.update(TABLE, "user1", change)).isEqualTo(Status.OK);

        // the first attempt conflicted (stored version 2, sent 1); the retry re-read and sent version 2
        List<Request> updates = gateway.calls("POST", "/write/document/update");
        assertThat(updates).hasSize(2);
        assertThat(updates.get(0).json().getInt("_version")).isEqualTo(1);
        assertThat(updates.get(1).json().getInt("_version")).isEqualTo(2);
        assertThat(gateway.calls("GET", "/user/read/document")).hasSize(2);

        // the changed value survived the retry (a YCSB value can only be read once, so it is kept as a string)
        Map<String, ByteIterator> result = new HashMap<>();
        client.read(TABLE, "user1", null, result);
        assertThat(strings(result)).containsEntry("field0", "mine").containsEntry("field1", "b");
    }

    @Test
    void updateGivesUpWithErrorWhenTheVersionKeepConflicting() {
        client.insert(TABLE, "user1", values("a", "b", "c"));
        gateway.injectVersionConflicts(100);

        assertThat(client.update(TABLE, "user1", values("x", "y", "z"))).isEqualTo(Status.ERROR);
        assertThat(gateway.calls("POST", "/write/document/update")).hasSize(4); // first try + 3 retries
    }

    @Test
    void initLogsInOnceWithTheConfiguredCredentials() {
        Request login = gateway.only("POST", "/login");
        assertThat(login.authorization()).isNull();
        assertThat(login.json().getString("username")).isEqualTo(StubGateway.USER);
        assertThat(login.json().getString("password")).isEqualTo(StubGateway.PASSWORD);

        client.insert(TABLE, "user1", values("a", "b", "c"));
        client.read(TABLE, "user1", null, new HashMap<>());
        assertThat(gateway.calls("POST", "/login")).hasSize(1); // the token is cached, not re-fetched per call
    }

    @Test
    void initFailsWhenLoginIsRejected() {
        assertThatThrownBy(() -> open("wrong-password")).isInstanceOf(DBException.class);
    }

    @Test
    void anExpiredTokenIsRenewedOnceAndTheCallRetried() {
        client.insert(TABLE, "user1", values("a", "b", "c"));
        gateway.expireToken();

        Map<String, ByteIterator> result = new HashMap<>();
        assertThat(client.read(TABLE, "user1", null, result)).isEqualTo(Status.OK);

        assertThat(gateway.calls("POST", "/login")).hasSize(2);
        List<Request> queries = gateway.calls("POST", "/user/index/query");
        assertThat(queries.get(queries.size() - 1).authorization()).isEqualTo("Bearer " + gateway.currentToken());
    }

    @Test
    void initCreatesTheDatabaseSchemaAndIndexExactlyOnce() throws Exception {
        // first client (opened in start()): login, database, the schema with the key and every field, the index
        assertThat(gateway.requests().stream().map(r -> r.method() + " " + r.path()))
                .containsExactly("POST /login", "POST /admin/database/create", "POST /write/schema/new",
                        "POST /admin/index/create");
        assertThat(gateway.only("POST", "/admin/database/create").query()).containsOnly(entry("databaseName", "ycsb"));
        Request schema = gateway.only("POST", "/write/schema/new");
        assertThat(schema.query()).containsOnly(entry("database", "ycsb"));
        assertThat(schema.authorization()).isEqualTo("Bearer " + StubGateway.TOKEN);
        JSONObject definition = schema.json();
        assertThat(definition.getString("schemaName")).isEqualTo("ycsb");
        assertThat(definition.getJSONObject("schema").toMap()).containsOnly(entry("ykey", "String"),
                entry("field0", "String"), entry("field1", "String"), entry("field2", "String"));
        assertThat(gateway.only("POST", "/admin/index/create").query())
                .containsOnly(entry("database", "ycsb"), entry("schema", "ycsb"), entry("field", "ykey"));

        // a second thread's client logs in for itself but finds the setup already done
        open(StubGateway.PASSWORD);
        assertThat(gateway.calls("POST", "/login")).hasSize(2);
        assertThat(gateway.calls("POST", "/write/schema/new")).hasSize(1);
        assertThat(gateway.calls("POST", "/admin/index/create")).hasSize(1);
    }

    @Test
    void initToleratesASchemaAndIndexThatAlreadyExist() throws Exception {
        NoSqlDBClient.resetSetupGuard(); // a fresh JVM: the run phase of a benchmark whose load phase already ran
        gateway.alreadyProvisioned();

        NoSqlDBClient second = open(StubGateway.PASSWORD); // does not throw

        assertThat(gateway.calls("POST", "/write/schema/new")).hasSize(2); // it did try, and the "exists" was fine
        assertThat(second.insert(TABLE, "user9", values("a", "b", "c"))).isEqualTo(Status.OK);
    }

    @Test
    void aKeyLongerThanTheIndexCanDistinguishIsRejected() {
        String tooLong = "k".repeat(65); // the index keeps the first 64 bytes of a string; longer keys would collide

        assertThat(client.insert(TABLE, tooLong, values("a", "b", "c"))).isEqualTo(Status.BAD_REQUEST);
        assertThat(client.read(TABLE, tooLong, null, new HashMap<>())).isEqualTo(Status.BAD_REQUEST);
        assertThat(gateway.calls("POST", "/write/document/new")).isEmpty();
        assertThat(gateway.calls("POST", "/user/index/query")).isEmpty();
    }

    @Test
    void anUnreachableGatewayIsAnErrorStatusNotAnException() throws Exception {
        gateway.close();

        assertThat(client.read(TABLE, "user1", null, new HashMap<>())).isEqualTo(Status.ERROR);
        assertThat(client.insert(TABLE, "user1", values("a", "b", "c"))).isEqualTo(Status.ERROR);
        assertThat(client.update(TABLE, "user1", values("a", "b", "c"))).isEqualTo(Status.ERROR);
        assertThat(client.delete(TABLE, "user1")).isEqualTo(Status.ERROR);
        assertThat(client.scan(TABLE, "user1", 5, null, new Vector<>())).isEqualTo(Status.ERROR);
    }
}
