package com.database.atypon.app;

import com.database.atypon.Node.security.JwtService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.actuate.metrics.AutoConfigureMetrics;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * M12 observability: every role scrapes {@code /actuator/prometheus} and probes {@code /actuator/health} without a
 * token, and nothing else of Actuator is reachable. Runs the real node security chain (the app module's role
 * launcher) end to end through MockMvc.
 */
@SpringBootTest
@AutoConfigureMockMvc
@AutoConfigureMetrics // @SpringBootTest switches metrics export off by default, which would drop the Prometheus registry
@ActiveProfiles("node")
@TestPropertySource(properties = {
        "app.jwt.secret=dev-secret-change-me-please-at-least-32-bytes-long-000",
        "NODE_NAME=TestNode"})
class ActuatorExposureTest {

    @Autowired
    MockMvc mvc;
    @Autowired
    JwtService jwt;

    @Test
    void prometheusEndpointIsPublicAndRenders() throws Exception {
        mvc.perform(get("/actuator/prometheus"))
                .andExpect(status().isOk())
                .andExpect(content().string(containsString("jvm_")));
    }

    @Test
    void healthEndpointIsPublic() throws Exception {
        mvc.perform(get("/actuator/health"))
                .andExpect(status().isOk())
                .andExpect(content().string(containsString("\"status\":\"UP\"")));
    }

    @Test
    void everyMeterCarriesTheRoleAndNodeTags() throws Exception {
        String body = mvc.perform(get("/actuator/prometheus"))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();
        List<String> samples = Arrays.stream(body.split("\n"))
                .filter(line -> line.startsWith("jvm_memory_used_bytes{"))
                .collect(Collectors.toList());
        assertFalse(samples.isEmpty(), "expected a jvm memory meter in the scrape");
        for (String line : samples) {
            assertTrue(line.contains("role=\"node\""), "missing role tag: " + line);
            assertTrue(line.contains("node=\"TestNode\""), "missing node tag: " + line);
        }
    }

    @Test
    void envEndpointIsNotExposed() throws Exception {
        // Authenticate as the most privileged caller (INTERNAL) so security cannot be what stops the request:
        // a 404 proves the endpoint is simply not exposed, where an unauthenticated 401/403 would prove nothing.
        String internal = jwt.generateToken("tester", "INTERNAL", Duration.ofMinutes(5));
        mvc.perform(get("/actuator/env").header("Authorization", "Bearer " + internal))
                .andExpect(status().isNotFound());
        mvc.perform(get("/actuator/heapdump").header("Authorization", "Bearer " + internal))
                .andExpect(status().isNotFound());
    }

    @Test
    void businessEndpointStillRequiresAuth() throws Exception {
        mvc.perform(get("/user/databases"))
                .andExpect(result -> {
                    int s = result.getResponse().getStatus();
                    assertTrue(s == 401 || s == 403, "expected 401/403 without a token but got " + s);
                });
    }
}
