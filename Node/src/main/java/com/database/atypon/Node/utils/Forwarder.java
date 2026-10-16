package com.database.atypon.Node.utils;

import com.database.atypon.Node.model.Node;
import com.database.atypon.Node.security.SecurityUtils;
import com.database.atypon.Node.utils.response.Response;
import com.database.atypon.Node.utils.response.ResponseType;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.web.client.RestTemplate;

import java.util.List;
import java.util.Map;
import java.util.Vector;

/**
 * Hands a client request to the node that should serve it: a write to the schema's primary, a read to a
 * node that holds the schema. The caller's own JWT travels with the request, so the target treats it as
 * coming from that caller (for a write, as the origin that applies it and replicates it to its replicas)
 * rather than as an internal peer copy.
 */
public final class Forwarder {

    private static final RestTemplate REST = new RestTemplate();

    private Forwarder() {
    }

    /**
     * POSTs {@code body} (may be null) to {@code target.getURL() + pathWithQuery} carrying the current
     * caller's token, and returns the {@code Vector<Response>} the target answers with. Must be called on
     * the request thread, since the token is read from the thread's security context. A transport or
     * parsing failure comes back as a single ERROR response rather than an exception.
     */
    public static Vector<Response> forwardForResponses(Node target, String pathWithQuery, Object body) {
        return forwardForResponses(target, pathWithQuery, body, true);
    }

    /**
     * As {@link #forwardForResponses(Node, String, Object)}, but a GET with no body when {@code post} is
     * false, for the read endpoints that answer with a {@code Vector<Response>}.
     */
    public static Vector<Response> forwardForResponses(Node target, String pathWithQuery, Object body, boolean post) {
        try {
            Vector<?> returned = send(target, pathWithQuery, body, post, Vector.class);
            Vector<Response> responses = new Vector<>();
            if (returned != null) {
                for (Object element : returned) {
                    responses.add(toResponse((Map<?, ?>) element));
                }
            }
            return responses;
        } catch (Exception e) {
            return new Vector<>(List.of(new Response(ResponseType.ERROR, "Error forwarding request", e.getMessage())));
        }
    }

    /**
     * Sends the request to {@code target.getURL() + pathWithQuery} carrying the current caller's token and
     * returns the single {@link Response} the target answers with, for the read and query endpoints (which
     * return one {@code Response} rather than a vector). It is a POST carrying {@code body} (may be null) when
     * {@code post} is true, otherwise a GET with no body. Must be called on the request thread, since the
     * token is read from the thread's security context. A transport or parsing failure comes back as an
     * ERROR response rather than an exception.
     */
    public static Response forwardForResponse(Node target, String pathWithQuery, Object body, boolean post) {
        try {
            Map<?, ?> returned = send(target, pathWithQuery, body, post, Map.class);
            if (returned == null) {
                return new Response(ResponseType.ERROR, "Error forwarding request", "Empty response from " + target.getName());
            }
            return toResponse(returned);
        } catch (Exception e) {
            return new Response(ResponseType.ERROR, "Error forwarding request", e.getMessage());
        }
    }

    /** One HTTP call to the target with the caller's own token: a POST carrying {@code body} (if any), or a bare GET. */
    private static <T> T send(Node target, String pathWithQuery, Object body, boolean post, Class<T> responseType) {
        HttpHeaders headers = new HttpHeaders();
        headers.set("Authorization", "Bearer " + SecurityUtils.currentToken());
        HttpEntity<Object> request = post && body != null ? new HttpEntity<>(body, headers) : new HttpEntity<>(headers);
        return REST.exchange(target.getURL() + pathWithQuery, post ? HttpMethod.POST : HttpMethod.GET,
                request, responseType).getBody();
    }

    private static Response toResponse(Map<?, ?> json) {
        return new Response(
                ResponseType.valueOf((String) json.get("responseType")),
                (String) json.get("message"),
                json.get("content"));
    }
}
