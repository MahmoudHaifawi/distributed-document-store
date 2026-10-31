package com.database.atypon.Node.controllers;

import com.database.atypon.Node.security.JwtService;
import com.sun.net.httpserver.HttpServer;
import org.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.TestPropertySource;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The {@code /swim/**} endpoints over real HTTP through the real security chain: INTERNAL only, and ping-req
 * really relays. It deliberately shares the application context (and JWT secret) of the other integration
 * tests: {@code ServiceTokens} is a static bridge to whichever context was created last, so a context with a
 * different secret created mid-run would break the internal calls of every cached context that runs after it.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestPropertySource(properties = "app.jwt.secret=dev-secret-change-me-please-at-least-32-bytes-long-000")
class SwimControllerTest {

    @LocalServerPort
    int port;
    @Autowired
    TestRestTemplate rest;
    @Autowired
    JwtService jwt;

    private HttpServer targetServer;
    private final AtomicInteger targetStatus = new AtomicInteger(200);
    private final List<String[]> targetRequests = Collections.synchronizedList(new ArrayList<>());

    @BeforeEach
    void startTarget() throws IOException {
        targetServer = HttpServer.create(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0), 0);
        targetServer.createContext("/", exchange -> {
            targetRequests.add(new String[]{
                    exchange.getRequestMethod(),
                    exchange.getRequestURI().getPath(),
                    exchange.getRequestHeaders().getFirst("Authorization")});
            byte[] reply = "{\"acked\":true}".getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(targetStatus.get(), reply.length);
            exchange.getResponseBody().write(reply);
            exchange.close();
        });
        targetServer.start();
    }

    @AfterEach
    void stopTarget() {
        targetServer.stop(0);
    }

    private String bearer(String role) {
        return "Bearer " + jwt.generateToken("u", role, Duration.ofMinutes(5));
    }

    private String pingReqFor(int targetPort) {
        return "{\"from\":\"NodeX\",\"target\":{\"name\":\"127.0.0.1\",\"port\":\"" + targetPort + "\"}}";
    }

    private int targetPort() {
        return targetServer.getAddress().getPort();
    }

    /** POSTs a JSON body to this node, with the given Authorization header or none. */
    private ResponseEntity<String> post(String path, String authorization, String json) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        if (authorization != null) {
            headers.set("Authorization", authorization);
        }
        return rest.exchange("http://localhost:" + port + path, HttpMethod.POST, new HttpEntity<>(json, headers), String.class);
    }

    private boolean acked(ResponseEntity<String> response) {
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        return new JSONObject(response.getBody()).getBoolean("acked");
    }

    @Test
    void pingAnswersWithAnAckForInternalCallersOnly() {
        String ping = "{\"from\":\"NodeX\"}";
        assertThat(post("/swim/ping", bearer("USER"), ping).getStatusCode()).isEqualTo(HttpStatus.FORBIDDEN);
        assertThat(post("/swim/ping", bearer("ADMIN"), ping).getStatusCode()).isEqualTo(HttpStatus.FORBIDDEN);
        assertThat(post("/swim/ping", null, ping).getStatusCode().is4xxClientError()).isTrue();

        assertThat(acked(post("/swim/ping", bearer("INTERNAL"), ping))).isTrue();
    }

    @Test
    void pingReqRequiresInternal() {
        assertThat(post("/swim/ping-req", bearer("USER"), pingReqFor(targetPort())).getStatusCode())
                .isEqualTo(HttpStatus.FORBIDDEN);
        assertThat(post("/swim/ping-req", bearer("ADMIN"), pingReqFor(targetPort())).getStatusCode())
                .isEqualTo(HttpStatus.FORBIDDEN);
        assertThat(targetRequests).as("a refused caller must not make this node ping anyone").isEmpty();
    }

    @Test
    void pingReqRelaysAPingToTheTargetAndReportsItsAck() {
        assertThat(acked(post("/swim/ping-req", bearer("INTERNAL"), pingReqFor(targetPort())))).isTrue();

        assertThat(targetRequests).hasSize(1);
        String[] relayed = targetRequests.get(0);
        assertThat(relayed[0]).isEqualTo("POST");
        assertThat(relayed[1]).isEqualTo("/swim/ping");
        assertThat(jwt.role(relayed[2].substring("Bearer ".length()))).isEqualTo("INTERNAL");
    }

    @Test
    void pingReqReportsNoAckWhenTheTargetFails() {
        targetStatus.set(500);
        assertThat(acked(post("/swim/ping-req", bearer("INTERNAL"), pingReqFor(targetPort())))).isFalse();

        int closedPort = targetPort();
        targetServer.stop(0); // now nothing is listening at all
        assertThat(acked(post("/swim/ping-req", bearer("INTERNAL"), pingReqFor(closedPort)))).isFalse();
    }

    @Test
    void pingReqWithoutATargetIsABadRequest() {
        assertThat(post("/swim/ping-req", bearer("INTERNAL"), "{\"from\":\"NodeX\"}").getStatusCode())
                .isEqualTo(HttpStatus.BAD_REQUEST);
    }
}
