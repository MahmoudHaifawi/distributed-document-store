package com.database.atypon.Node.utils;

import com.database.atypon.Node.model.Node;
import com.database.atypon.Node.security.SecurityUtils;
import com.database.atypon.Node.utils.response.Response;
import com.database.atypon.Node.utils.response.ResponseType;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.web.client.RestTemplate;

import java.util.List;
import java.util.Map;
import java.util.Vector;

/**
 * Hands a client write to the node that owns it. The caller's own JWT travels with the request, so the
 * target treats it as the origin of the write (applies it and replicates it to its replicas) rather
 * than as an internal peer copy.
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
        try {
            HttpHeaders headers = new HttpHeaders();
            headers.set("Authorization", "Bearer " + SecurityUtils.currentToken());
            HttpEntity<Object> request = body == null ? new HttpEntity<>(headers) : new HttpEntity<>(body, headers);

            Vector<?> returned = REST.postForObject(target.getURL() + pathWithQuery, request, Vector.class);
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

    private static Response toResponse(Map<?, ?> json) {
        return new Response(
                ResponseType.valueOf((String) json.get("responseType")),
                (String) json.get("message"),
                json.get("content"));
    }
}
