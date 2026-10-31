package com.database.atypon.app;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.actuate.metrics.AutoConfigureMetrics;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;

import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/** The coordinator's JWT chain lets Prometheus scrape and probe without a token, and still gates everything else. */
@SpringBootTest
@AutoConfigureMockMvc
@AutoConfigureMetrics // @SpringBootTest switches metrics export off by default, which would drop the Prometheus registry
@ActiveProfiles("coordinator")
class ActuatorExposureCoordinatorTest {

    @Autowired
    MockMvc mvc;

    @Test
    void prometheusAndHealthArePublicAndCarryTheCoordinatorRole() throws Exception {
        mvc.perform(get("/actuator/prometheus"))
                .andExpect(status().isOk())
                .andExpect(content().string(containsString("role=\"coordinator\"")));
        mvc.perform(get("/actuator/health"))
                .andExpect(status().isOk());
    }

    @Test
    void everythingElseStillRequiresAuth() throws Exception {
        for (String path : new String[]{"/actuator/env", "/createNewUser", "/network/status"}) {
            mvc.perform(get(path)).andExpect(result -> {
                int s = result.getResponse().getStatus();
                assertTrue(s == 401 || s == 403, "expected 401/403 without a token for " + path + " but got " + s);
            });
        }
    }
}
