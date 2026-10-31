package com.database.atypon.ycsb;

import com.database.atypon.ycsb.GatewayClient.Reply;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.database.atypon.ycsb.GatewayClient.path;

/**
 * A YCSB {@link DB} binding for the document store, written as an external client of its public JSON API
 * (login, the write endpoints, the index query and read-by-id), so a benchmark exercises the real HTTP
 * path, the quorum, the replication and the B+-tree index.
 *
 * <h2>The mapping</h2>
 * YCSB addresses a record by a key it chooses ({@code user1234}); the store assigns its own integer
 * document id and can look a document up by id or through a secondary index. The key is therefore kept as
 * an ordinary document field, {@value #KEY_FIELD}, with a B+-tree index on it:
 * <ul>
 *   <li><b>table</b>: one schema (default {@code ycsb}) in one database (default {@code ycsb}). YCSB's table
 *       name is ignored: a benchmark has a single table.</li>
 *   <li><b>insert</b>: create the document {@code {ykey: key, field0: ..., field1: ...}}.</li>
 *   <li><b>read</b>: index point-query {@code ykey = key} gives the document id, then read that id.</li>
 *   <li><b>update</b>: the same lookup, then read the document, merge the changed fields into it and write
 *       it back by id at the {@code _version} that was read (the store replaces a whole document and checks
 *       the version optimistically). A version conflict, which concurrent updates of a hot key produce,
 *       re-reads and tries again, up to {@value #DEFAULT_CONFLICT_RETRIES} more times by default.</li>
 *   <li><b>delete</b>: the same lookup, then delete that id at the version that was read.</li>
 *   <li><b>scan</b>: index range query {@code ykey >= startkey}, ascending, limited to {@code recordcount},
 *       then a read of each id returned. Keys order by their UTF-8 bytes. The node builds the whole range
 *       before applying the limit, so a scan costs in proportion to the keys after the start key.</li>
 * </ul>
 *
 * <h2>Limits that come from the store</h2>
 * The index keeps the first 64 bytes of a string field, so a longer key could not be told from another and
 * is rejected with {@link Status#BAD_REQUEST}. The store does not enforce unique keys: loading the same key
 * twice creates two documents, and the binding then uses the newest (highest id).
 *
 * <h2>Properties</h2>
 * <ul>
 *   <li>{@code gateway.url} (or {@code node.url}): the base URL of the node or gateway,
 *       default {@code http://localhost:8080}</li>
 *   <li>{@code nosql.username}, {@code nosql.password}: the account to log in as; creating the database,
 *       schema and index needs an admin. Defaults to the demo cluster's seeded admin.</li>
 *   <li>{@code nosql.db} (default {@code ycsb}), {@code nosql.schema} (default {@code ycsb}): where records
 *       go. (The plain {@code db} property is taken by YCSB itself, for the binding's class name.)</li>
 *   <li>{@code nosql.setup} (default {@code true}): create the database, schema and index in {@code init()},
 *       once per JVM; "already exists" is fine, so the run phase can follow the load phase. Set
 *       {@code false} to use a schema that was provisioned by hand.</li>
 *   <li>{@code fieldcount} (default 10), {@code fieldnameprefix} (default {@code field}): the same
 *       properties YCSB's core workload uses; they name the schema's String fields.</li>
 *   <li>{@code nosql.update.retries} (default 3): re-reads after a version conflict.</li>
 *   <li>{@code nosql.timeout.ms} (default 30000): per HTTP call.</li>
 * </ul>
 * Every operation returns a {@link Status}; none throws. {@code init()} throws {@link DBException} when it
 * cannot log in or provision, since a benchmark cannot proceed.
 */
public class NoSqlDBClient extends DB {

    private static final Logger LOG = Logger.getLogger(NoSqlDBClient.class.getName());

    public static final String GATEWAY_URL = "gateway.url";
    public static final String NODE_URL = "node.url";
    public static final String USERNAME = "nosql.username";
    public static final String PASSWORD = "nosql.password";
    public static final String DATABASE = "nosql.db";
    public static final String SCHEMA = "nosql.schema";
    public static final String SETUP = "nosql.setup";
    public static final String CONFLICT_RETRIES = "nosql.update.retries";
    public static final String TIMEOUT_MS = "nosql.timeout.ms";

    static final String DEFAULT_URL = "http://localhost:8080";
    static final int DEFAULT_CONFLICT_RETRIES = 3;
    /** The seeded admin of the demo cluster (see docker-compose.yml); override for any other deployment. */
    private static final String DEFAULT_USERNAME = "mahmoud";
    private static final String DEFAULT_PASSWORD = "admin";

    /** The reserved field holding the YCSB record key, and the field the B+-tree index is built on. */
    static final String KEY_FIELD = "ykey";
    /** The index keeps this many bytes of a string field. */
    private static final int MAX_KEY_BYTES = 64;
    private static final String VERSION = "_version";
    /** What the store adds to a document; none of it is part of a YCSB record. */
    private static final Set<String> SERVER_FIELDS = Set.of(VERSION, "_vclock", "_ts", "_deleted");

    /** Database / schema / index provisioning happens once per JVM, however many client threads init(). */
    private static final Object SETUP_LOCK = new Object();
    private static final Set<String> PROVISIONED = new HashSet<>(); // guarded by SETUP_LOCK
    private static final AtomicInteger LOGGED_FAILURES = new AtomicInteger();
    private static final int MAX_LOGGED_FAILURES = 20;

    private GatewayClient gateway;
    private String database;
    private String schema;
    private int conflictRetries;

    /** Forgets what this JVM has provisioned, so the next init() runs the setup again (for tests). */
    static void resetSetupGuard() {
        synchronized (SETUP_LOCK) {
            PROVISIONED.clear();
        }
    }

    // ---- lifecycle ----

    @Override
    public void init() throws DBException {
        Properties props = getProperties();
        String url = props.getProperty(GATEWAY_URL, props.getProperty(NODE_URL, DEFAULT_URL));
        while (url.endsWith("/")) {
            url = url.substring(0, url.length() - 1);
        }
        database = props.getProperty(DATABASE, "ycsb");
        schema = props.getProperty(SCHEMA, "ycsb");
        conflictRetries = intProperty(props, CONFLICT_RETRIES, DEFAULT_CONFLICT_RETRIES);
        Duration timeout = Duration.ofMillis(intProperty(props, TIMEOUT_MS, 30_000));
        String username = props.getProperty(USERNAME, DEFAULT_USERNAME);

        GatewayClient client = new GatewayClient(url, username, props.getProperty(PASSWORD, DEFAULT_PASSWORD), timeout);
        Reply login = client.login();
        if (!login.isSuccess()) {
            throw new DBException("cannot log in to " + url + " as '" + username + "': " + login.message);
        }
        gateway = client;

        if (Boolean.parseBoolean(props.getProperty(SETUP, "true"))) {
            provision(props);
        }
    }

    @Override
    public void cleanup() {
        gateway = null;
    }

    /**
     * Creates the database, the schema (the key plus the YCSB fields, all String) and the index on the key,
     * once per (node, database, schema) in this JVM: threads that arrive while the first is still working
     * wait for it, and later ones find it done. A database, schema or index that already exists is fine.
     */
    private void provision(Properties props) throws DBException {
        int fieldCount = intProperty(props, "fieldcount", 10);
        String prefix = props.getProperty("fieldnameprefix", "field");
        String provisioned = gateway.baseUrl() + "|" + database + "|" + schema;

        synchronized (SETUP_LOCK) {
            if (PROVISIONED.contains(provisioned)) {
                return;
            }
            // The node reports an existing database only as "Failed to create database", so this answer is
            // not judged here: if the database is really missing, the schema creation says so.
            Reply databaseReply = gateway.post(path("/admin/database/create", "databaseName", database), null);

            JSONObject fields = new JSONObject().put(KEY_FIELD, "String");
            for (int i = 0; i < fieldCount; i++) {
                fields.put(prefix + i, "String");
            }
            JSONObject definition = new JSONObject().put("schemaName", schema).put("schema", fields);
            requireCreated("schema '" + schema + "'",
                    gateway.post(path("/write/schema/new", "database", database), definition.toString()),
                    "database creation answered: " + databaseReply);
            requireCreated("index on " + schema + "." + KEY_FIELD,
                    gateway.post(path("/admin/index/create", "database", database, "schema", schema,
                            "field", KEY_FIELD), null),
                    "");
            PROVISIONED.add(provisioned);
        }
    }

    private static void requireCreated(String what, Reply reply, String context) throws DBException {
        if (!reply.isSuccess() && !reply.alreadyExists()) {
            throw new DBException("cannot create " + what + ": " + reply
                    + (context.isEmpty() ? "" : " (" + context + ")")
                    + ". The account needs the admin role; to use a schema provisioned by hand set "
                    + SETUP + "=false");
        }
    }

    // ---- the DB operations ----

    @Override
    public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
        return guarded(() -> {
            requireKey(key);
            copyFields(fetch(resolveId(key)), fields, result);
            return Status.OK;
        });
    }

    @Override
    public Status scan(String table, String startkey, int recordcount, Set<String> fields,
                       Vector<HashMap<String, ByteIterator>> result) {
        return guarded(() -> {
            requireKey(startkey);
            if (recordcount <= 0) {
                return Status.OK;
            }
            JSONObject query = indexQuery("GTE", startkey).put("order", "ASC").put("limit", recordcount);
            for (int id : queryIds(query)) {
                try {
                    HashMap<String, ByteIterator> row = new HashMap<>();
                    copyFields(fetch(id), fields, row);
                    result.add(row);
                } catch (StatusException e) {
                    if (!Status.NOT_FOUND.equals(e.status)) {
                        throw e;
                    } // else deleted since the index was read: leave it out
                }
            }
            return Status.OK;
        });
    }

    @Override
    public Status insert(String table, String key, Map<String, ByteIterator> values) {
        return guarded(() -> {
            requireKey(key);
            JSONObject document = new JSONObject();
            toStrings(values).forEach(document::put);
            document.put(KEY_FIELD, key); // the key wins over a field of the same name
            Reply reply = gateway.post(path("/write/document/new", "database", database, "schema", schema),
                    document.toString());
            if (!reply.isSuccess()) {
                throw failure("insert of " + key, reply);
            }
            return Status.OK;
        });
    }

    @Override
    public Status update(String table, String key, Map<String, ByteIterator> values) {
        Map<String, String> changes = toStrings(values); // a ByteIterator can be read once; a retry must not re-read it
        return guarded(() -> mutate(key, "update", (id, current) -> {
            JSONObject document = new JSONObject(current.toString());
            document.remove("_vclock"); // the store stamps these itself
            document.remove("_ts");
            document.remove("_deleted");
            changes.forEach(document::put); // _version stays: the write is checked against it
            return gateway.post(path("/write/document/update", "database", database, "schema", schema,
                    "id", String.valueOf(id)), document.toString());
        }));
    }

    @Override
    public Status delete(String table, String key) {
        return guarded(() -> mutate(key, "delete", (id, current) ->
                gateway.post(path("/write/document/delete", "database", database, "schema", schema,
                        "id", String.valueOf(id), "version", String.valueOf(current.optInt(VERSION, 1))), null)));
    }

    // ---- lookup and fetch ----

    /** The ids of the documents whose key is {@code key}: one, unless the key was loaded more than once. */
    List<Integer> lookupIds(String key) {
        return queryIds(indexQuery("EQ", key));
    }

    private JSONObject indexQuery(String op, String value) {
        return new JSONObject().put("database", database).put("schema", schema).put("field", KEY_FIELD)
                .put("op", op).put("value", value);
    }

    private List<Integer> queryIds(JSONObject query) {
        Reply reply = gateway.post("/user/index/query", query.toString());
        if (!reply.isSuccess()) {
            throw failure("index query", reply);
        }
        List<Integer> ids = new ArrayList<>();
        if (reply.content instanceof JSONArray array) {
            for (int i = 0; i < array.length(); i++) {
                ids.add(array.getInt(i));
            }
        }
        return ids;
    }

    /** The document id for a key; the newest if a key was loaded twice. NOT_FOUND when there is none. */
    private int resolveId(String key) {
        List<Integer> ids = lookupIds(key);
        if (ids.isEmpty()) {
            throw new StatusException(Status.NOT_FOUND);
        }
        return ids.get(ids.size() - 1);
    }

    private JSONObject fetch(int id) {
        Reply reply = gateway.get(path("/user/read/document", "id", String.valueOf(id),
                "databaseName", database, "schemaName", schema));
        if (reply.isSuccess() && reply.content instanceof String content) {
            try {
                return new JSONObject(content);
            } catch (JSONException e) {
                throw failure("read of document " + id, Reply.error("not a JSON document"));
            }
        }
        if (reply.isNotFound()) {
            throw new StatusException(Status.NOT_FOUND);
        }
        throw failure("read of document " + id, reply);
    }

    /**
     * An update or a delete: find the document, then write against the version just read. A concurrent
     * writer makes that version stale, which the store reports as a version conflict; the document is then
     * read again and the write repeated, up to {@code conflictRetries} more times.
     */
    private Status mutate(String key, String what, BiFunction<Integer, JSONObject, Reply> write) {
        requireKey(key);
        int id = resolveId(key);
        for (int attempt = 0; ; attempt++) {
            Reply reply = write.apply(id, fetch(id));
            if (reply.isSuccess()) {
                return Status.OK;
            }
            if (reply.isVersionConflict() && attempt < conflictRetries) {
                continue;
            }
            if (reply.isNotFound()) {
                return Status.NOT_FOUND;
            }
            throw failure(what + " of " + key, reply);
        }
    }

    // ---- helpers ----

    /** A record's fields only: not the key, and not what the store adds. */
    private static void copyFields(JSONObject document, Set<String> wanted, Map<String, ByteIterator> into) {
        for (String name : document.keySet()) {
            if (KEY_FIELD.equals(name) || SERVER_FIELDS.contains(name)) {
                continue;
            }
            if (wanted == null || wanted.contains(name)) {
                into.put(name, new StringByteIterator(String.valueOf(document.get(name))));
            }
        }
    }

    private static Map<String, String> toStrings(Map<String, ByteIterator> values) {
        Map<String, String> strings = new LinkedHashMap<>();
        values.forEach((name, value) -> strings.put(name, value.toString()));
        return strings;
    }

    private static void requireKey(String key) {
        if (key == null || key.isEmpty() || key.getBytes(StandardCharsets.UTF_8).length > MAX_KEY_BYTES) {
            throw new StatusException(Status.BAD_REQUEST);
        }
    }

    private static int intProperty(Properties props, String name, int fallback) {
        String value = props.getProperty(name);
        return value == null ? fallback : Integer.parseInt(value.trim());
    }

    /** Runs one operation and turns every outcome into a Status: nothing is thrown to YCSB. */
    private Status guarded(Supplier<Status> operation) {
        if (gateway == null) {
            return Status.ERROR; // init() did not complete
        }
        try {
            return operation.get();
        } catch (StatusException e) {
            return e.status;
        } catch (RuntimeException e) {
            logFailure("unexpected failure: " + e, e);
            return Status.ERROR;
        }
    }

    private static StatusException failure(String what, Reply reply) {
        logFailure(what + " failed: " + reply, null);
        return new StatusException(Status.ERROR);
    }

    /** The first few failures are worth a line each; after that the Status counts in YCSB's report say it. */
    private static void logFailure(String message, Throwable cause) {
        boolean first = LOGGED_FAILURES.incrementAndGet() <= MAX_LOGGED_FAILURES;
        LOG.log(first ? Level.WARNING : Level.FINE, message, cause);
    }

    /** Ends an operation early with the Status to return; no stack trace, it is plain control flow. */
    private static final class StatusException extends RuntimeException {
        final transient Status status;

        StatusException(Status status) {
            super(status.getName(), null, false, false);
            this.status = status;
        }
    }
}
