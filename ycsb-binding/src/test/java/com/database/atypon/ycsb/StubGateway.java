package com.database.atypon.ycsb;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * An in-memory stand-in for the data node's public HTTP API: login, database / schema / index creation,
 * document create / update / delete, the index query and read-by-id, with the same URL shapes, parameter
 * names and response envelopes as the real controllers (a {@code Vector<Response>} JSON array for writes, a
 * single {@code Response} object for the index query, read-by-id and login). It enforces what the real node
 * enforces and the binding has to cope with: a Bearer token on everything but /login, every schema field
 * present on create and update, optimistic {@code _version} checks on update and delete, tombstones that
 * leave the index. It records every request so a test can assert exactly what the client sent.
 */
final class StubGateway implements AutoCloseable {

    // test values that exist only inside this stub
    static final String USER = "bench";
    static final String PASSWORD = "bench-pass";
    static final String TOKEN = "stub.jwt.token";

    /** One request as the stub received it. */
    record Request(String method, String path, Map<String, String> query, String body, String authorization) {
        JSONObject json() {
            return new JSONObject(body);
        }
    }

    private final HttpServer server;
    private final List<Request> requests = new CopyOnWriteArrayList<>();
    private final TreeMap<Integer, JSONObject> docs = new TreeMap<>(); // id -> stored doc, or a tombstone
    private int nextId;
    private Set<String> schemaFields = Set.of();
    private volatile String currentToken = TOKEN;
    private boolean preexisting;
    private int injectedConflicts;
    private boolean closed;
    private final Map<String, ForcedAnswer> forced = new ConcurrentHashMap<>();

    private record ForcedAnswer(int status, String body) {
    }

    StubGateway() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", this::handle);
        server.setExecutor(Executors.newCachedThreadPool());
        server.start();
    }

    String url() {
        return "http://127.0.0.1:" + server.getAddress().getPort();
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        server.stop(0);
        ((ExecutorService) server.getExecutor()).shutdownNow();
    }

    // ---- test controls ----

    /** The database, schema and index already exist, as on the second run of a benchmark. */
    void alreadyProvisioned() {
        preexisting = true;
    }

    /** Invalidates the issued token, as a JWT expiring would: the next login returns a different one. */
    void expireToken() {
        currentToken = currentToken + ".renewed";
    }

    String currentToken() {
        return currentToken;
    }

    /**
     * From now on answers {@code method path} with this raw HTTP status and body instead of routing it (for an
     * authenticated request): a Spring 404 page, a 500, a node envelope the real endpoints would not send.
     */
    void forceAnswer(String method, String path, int status, String body) {
        forced.put(method + " " + path, new ForcedAnswer(status, body));
    }

    void clearForcedAnswers() {
        forced.clear();
    }

    /** The next {@code n} updates find the document already bumped by a concurrent writer. */
    synchronized void injectVersionConflicts(int n) {
        injectedConflicts = n;
    }

    // ---- what was recorded / stored ----

    List<Request> requests() {
        return new ArrayList<>(requests);
    }

    List<Request> calls(String method, String path) {
        return requests.stream().filter(r -> r.method().equals(method) && r.path().equals(path)).toList();
    }

    Request only(String method, String path) {
        List<Request> found = calls(method, path);
        if (found.size() != 1) {
            throw new AssertionError("expected exactly one " + method + " " + path + " but saw " + found.size()
                    + " in " + requests);
        }
        return found.get(0);
    }

    /** The live (not deleted) documents holding this ykey. */
    synchronized List<JSONObject> liveDocumentsWithKey(String key) {
        return docs.values().stream()
                .filter(d -> !d.optBoolean("_deleted") && key.equals(d.optString("ykey")))
                .toList();
    }

    // ---- the server ----

    private void handle(HttpExchange exchange) throws IOException {
        String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
        Request request = new Request(exchange.getRequestMethod(), exchange.getRequestURI().getPath(),
                parseQuery(exchange.getRequestURI().getRawQuery()), body,
                exchange.getRequestHeaders().getFirst("Authorization"));
        requests.add(request);

        int status = 200;
        String response;
        try {
            if (request.path().equals("/login")) {
                response = login(request);
            } else if (!("Bearer " + currentToken).equals(request.authorization())) {
                status = 403;
                response = "{\"status\":403,\"error\":\"Forbidden\"}";
            } else if (forced.containsKey(request.method() + " " + request.path())) {
                ForcedAnswer answer = forced.get(request.method() + " " + request.path());
                status = answer.status();
                response = answer.body();
            } else {
                response = route(request);
            }
        } catch (RuntimeException e) {
            status = 500;
            response = "{\"status\":500,\"error\":\"" + e + "\"}";
        }
        byte[] bytes = response.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, bytes.length);
        exchange.getResponseBody().write(bytes);
        exchange.close();
    }

    private String login(Request request) {
        JSONObject credentials = request.json();
        if (USER.equals(credentials.optString("username")) && PASSWORD.equals(credentials.optString("password"))) {
            return single("SUCCESS", "Login successful", currentToken);
        }
        return single("ERROR", "Wrong password", null);
    }

    private synchronized String route(Request r) {
        String key = r.method() + " " + r.path();
        switch (key) {
            case "POST /admin/database/create":
                return preexisting ? array("ERROR", "Failed to create database", null)
                        : array("SUCCESS", "Database created successfully", null);
            case "POST /write/schema/new":
                return createSchema(r);
            case "POST /admin/index/create":
                return preexisting ? array("ERROR", "index already exists on ycsb.ykey", null)
                        : array("SUCCESS", "Index created on " + r.query().get("schema") + "." + r.query().get("field"), null);
            case "POST /write/document/new":
                return createDocument(r);
            case "POST /write/document/update":
                return updateDocument(r);
            case "POST /write/document/delete":
                return deleteDocument(r);
            case "POST /user/index/query":
                return indexQuery(r.json());
            case "GET /user/read/document":
                return readDocument(r);
            default:
                throw new IllegalStateException("no stub route for " + key);
        }
    }

    private String createSchema(Request r) {
        if (preexisting) {
            return array("ERROR", "Schema already exists", null);
        }
        schemaFields = Set.copyOf(r.json().getJSONObject("schema").keySet());
        return array("SUCCESS", "Schema created successfully", null);
    }

    private String createDocument(Request r) {
        JSONObject doc = r.json();
        String missing = missingSchemaField(doc);
        if (missing != null) {
            return array("ERROR", "Document doesn't have key: " + missing, null);
        }
        doc.put("_version", 1).put("_vclock", new JSONObject().put("stub", 1)).put("_ts", System.currentTimeMillis());
        docs.put(nextId++, doc);
        return array("SUCCESS", "Document created successfully", null);
    }

    private String updateDocument(Request r) {
        int id = Integer.parseInt(r.query().get("id"));
        JSONObject stored = docs.get(id);
        if (stored == null || stored.optBoolean("_deleted")) {
            return array("ERROR", "Document not found", null);
        }
        if (injectedConflicts > 0) { // somebody else wrote between the client's read and this update
            injectedConflicts--;
            stored.put("_version", stored.getInt("_version") + 1);
        }
        JSONObject doc = r.json();
        int storedVersion = stored.getInt("_version");
        if (doc.optInt("_version", 1) != storedVersion) {
            return array("ERROR", "Version conflict: document is at version " + storedVersion, storedVersion);
        }
        String missing = missingSchemaField(doc);
        if (missing != null) {
            return array("ERROR", "Document doesn't have key: " + missing, null);
        }
        doc.put("_version", storedVersion + 1).put("_vclock", new JSONObject().put("stub", storedVersion + 1))
                .put("_ts", System.currentTimeMillis());
        docs.put(id, doc);
        return array("SUCCESS", "Document updated successfully", storedVersion + 1);
    }

    private String deleteDocument(Request r) {
        int id = Integer.parseInt(r.query().get("id"));
        JSONObject stored = docs.get(id);
        if (stored == null || stored.optBoolean("_deleted")) {
            return array("ERROR", "Document not found", null);
        }
        int storedVersion = stored.getInt("_version");
        if (Integer.parseInt(r.query().getOrDefault("version", "0")) != storedVersion) {
            return array("ERROR", "Version conflict: document is at version " + storedVersion, storedVersion);
        }
        docs.put(id, new JSONObject().put("_deleted", true).put("_version", storedVersion + 1));
        return array("SUCCESS", "Document deleted successfully", null);
    }

    /** The index on ykey: composite (value, docId) order, tombstones not in it. */
    private String indexQuery(JSONObject q) {
        String field = q.getString("field");
        String op = q.getString("op");
        String value = q.getString("value");
        List<Map.Entry<Integer, JSONObject>> matches = new ArrayList<>();
        for (Map.Entry<Integer, JSONObject> e : docs.entrySet()) {
            JSONObject d = e.getValue();
            if (d.optBoolean("_deleted") || !d.has(field)) {
                continue;
            }
            int cmp = d.getString(field).compareTo(value);
            if (op.equals("EQ") ? cmp == 0 : op.equals("GTE") && cmp >= 0) {
                matches.add(e);
            }
        }
        Comparator<Map.Entry<Integer, JSONObject>> order = Comparator
                .comparing((Map.Entry<Integer, JSONObject> e) -> e.getValue().getString(field))
                .thenComparing(Map.Entry::getKey);
        if ("DESC".equalsIgnoreCase(q.optString("order", "ASC"))) {
            order = order.reversed();
        }
        matches.sort(order);
        int limit = q.has("limit") ? q.getInt("limit") : -1;
        JSONArray ids = new JSONArray();
        for (Map.Entry<Integer, JSONObject> e : matches) {
            if (limit >= 0 && ids.length() >= limit) {
                break;
            }
            ids.put(e.getKey());
        }
        return single("SUCCESS", "Query results", ids);
    }

    private String readDocument(Request r) {
        JSONObject stored = docs.get(Integer.parseInt(r.query().get("id")));
        if (stored == null || stored.optBoolean("_deleted")) {
            return single("ERROR", "Document not found", null);
        }
        return single("SUCCESS", "Document fetched successfully", stored.toString());
    }

    private String missingSchemaField(JSONObject doc) {
        for (String field : schemaFields) {
            if (!doc.has(field)) {
                return field;
            }
        }
        return null;
    }

    // ---- response envelopes ----

    private static String single(String type, String message, Object content) {
        JSONObject response = new JSONObject().put("responseType", type).put("message", message);
        if (content != null) {
            response.put("content", content);
        }
        return response.toString();
    }

    private static String array(String type, String message, Object content) {
        return "[" + single(type, message, content) + "]";
    }

    private static Map<String, String> parseQuery(String raw) {
        Map<String, String> query = new LinkedHashMap<>();
        if (raw == null || raw.isEmpty()) {
            return query;
        }
        for (String pair : raw.split("&")) {
            int eq = pair.indexOf('=');
            String name = eq < 0 ? pair : pair.substring(0, eq);
            String value = eq < 0 ? "" : pair.substring(eq + 1);
            query.put(URLDecoder.decode(name, StandardCharsets.UTF_8), URLDecoder.decode(value, StandardCharsets.UTF_8));
        }
        return query;
    }
}
