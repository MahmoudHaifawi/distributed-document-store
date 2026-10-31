package com.database.atypon.ycsb;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Locale;
import java.util.Objects;

/**
 * A small client for the data node's public JSON API: it logs in once, sends the JWT as a Bearer token on
 * every call, and turns each answer into a {@link Reply}. It never throws for a failed call (an unreachable
 * node, a timeout, a non-2xx status and an unparseable body all come back as an ERROR reply), so the YCSB
 * operations above it can map every outcome to a {@code Status}.
 *
 * <p>A JWT lasts an hour, so a benchmark can outlive one: when a call is rejected with 401 or 403 the client
 * logs in again (once, however many threads were rejected together) and repeats the call a single time.
 */
final class GatewayClient {

    /**
     * The node's {@code Response} envelope: {@code responseType} (SUCCESS or ERROR), a {@code message} and an
     * optional {@code content}. A write endpoint answers with an array of these (the first is the client's
     * verdict, the rest the replicas'); the index query, read-by-id and login answer with one.
     */
    static final class Reply {
        final String type;
        final String message;
        final Object content;

        Reply(String type, String message, Object content) {
            this.type = type;
            this.message = message;
            this.content = content;
        }

        static Reply error(String message) {
            return new Reply("ERROR", message, null);
        }

        boolean isSuccess() {
            return "SUCCESS".equals(type);
        }

        /** The node's wording for a document that is absent or deleted is "Document not found". */
        boolean isNotFound() {
            return !isSuccess() && lower().contains("not found");
        }

        /** An update or delete made against a stale {@code _version}. */
        boolean isVersionConflict() {
            return !isSuccess() && lower().startsWith("version conflict");
        }

        /** Creating a database, schema or index that is already there. */
        boolean alreadyExists() {
            return !isSuccess() && lower().contains("already exists");
        }

        private String lower() {
            return message == null ? "" : message.toLowerCase(Locale.ROOT);
        }

        @Override
        public String toString() {
            return type + ": " + message;
        }
    }

    private final HttpClient http;
    private final String baseUrl;
    private final String username;
    private final String password;
    private final Duration timeout;
    private final Object loginLock = new Object();
    private volatile String token;

    GatewayClient(String baseUrl, String username, String password, Duration timeout) {
        this.http = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(timeout)
                .build();
        this.baseUrl = baseUrl;
        this.username = username;
        this.password = password;
        this.timeout = timeout;
    }

    String baseUrl() {
        return baseUrl;
    }

    /** {@code path} followed by {@code ?name=value&...}, each value URL-encoded. */
    static String path(String path, String... namesAndValues) {
        StringBuilder url = new StringBuilder(path);
        for (int i = 0; i < namesAndValues.length; i += 2) {
            url.append(i == 0 ? '?' : '&').append(namesAndValues[i]).append('=')
                    .append(URLEncoder.encode(namesAndValues[i + 1], StandardCharsets.UTF_8));
        }
        return url.toString();
    }

    /** Logs in with the configured credentials and keeps the JWT it is given. */
    Reply login() {
        String body = new JSONObject().put("username", username).put("password", password).toString();
        try {
            Reply reply = parse(exchange("POST", "/login", body, null));
            if (reply.isSuccess() && reply.content instanceof String jwt) {
                token = jwt;
                return reply;
            }
            return reply.isSuccess() ? Reply.error("login answered without a token") : reply;
        } catch (IOException e) {
            return Reply.error("login failed: " + e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return Reply.error("login interrupted");
        }
    }

    Reply post(String pathAndQuery, String jsonBody) {
        return send("POST", pathAndQuery, jsonBody);
    }

    Reply get(String pathAndQuery) {
        return send("GET", pathAndQuery, null);
    }

    private Reply send(String method, String pathAndQuery, String body) {
        try {
            String used = token;
            HttpResponse<String> response = exchange(method, pathAndQuery, body, used);
            if (response.statusCode() == 401 || response.statusCode() == 403) {
                if (renewToken(used)) {
                    response = exchange(method, pathAndQuery, body, token);
                }
            }
            return parse(response);
        } catch (IOException e) {
            return Reply.error(method + " " + pathAndQuery + " failed: " + e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return Reply.error(method + " " + pathAndQuery + " interrupted");
        }
    }

    /** Logs in again unless another thread already replaced the rejected token. */
    private boolean renewToken(String rejected) {
        synchronized (loginLock) {
            if (!Objects.equals(token, rejected)) {
                return true;
            }
            return login().isSuccess();
        }
    }

    private HttpResponse<String> exchange(String method, String pathAndQuery, String body, String bearer)
            throws IOException, InterruptedException {
        HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(baseUrl + pathAndQuery))
                .timeout(timeout)
                .header("Content-Type", "application/json")
                .header("Accept", "application/json");
        if (bearer != null) {
            request.header("Authorization", "Bearer " + bearer);
        }
        if ("GET".equals(method)) {
            request.GET();
        } else {
            request.POST(body == null
                    ? HttpRequest.BodyPublishers.noBody()
                    : HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8));
        }
        return http.send(request.build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
    }

    private static Reply parse(HttpResponse<String> response) {
        String body = response.body() == null ? "" : response.body().trim();
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            return Reply.error("HTTP " + response.statusCode() + (body.isEmpty() ? "" : ": " + abbreviate(body)));
        }
        try {
            JSONObject envelope = body.startsWith("[")
                    ? new JSONArray(body).getJSONObject(0) // a write's first element is the client's verdict
                    : new JSONObject(body);
            return new Reply(envelope.optString("responseType", "ERROR"), envelope.optString("message", ""),
                    envelope.opt("content"));
        } catch (JSONException e) {
            return Reply.error("unparseable response: " + abbreviate(body));
        }
    }

    private static String abbreviate(String text) {
        return text.length() <= 200 ? text : text.substring(0, 200) + "...";
    }
}
