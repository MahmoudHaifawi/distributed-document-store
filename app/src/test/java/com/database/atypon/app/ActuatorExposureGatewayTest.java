package com.database.atypon.app;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.actuate.metrics.AutoConfigureMetrics;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;

import static org.hamcrest.Matchers.containsString;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.redirectedUrlPattern;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/** The gateway's form-login chain lets Prometheus scrape and probe without logging in, and still gates everything else. */
@SpringBootTest
@AutoConfigureMockMvc
@AutoConfigureMetrics // @SpringBootTest switches metrics export off by default, which would drop the Prometheus registry
@ActiveProfiles("gateway")
class ActuatorExposureGatewayTest {

    @Autowired
    MockMvc mvc;

    @Test
    void prometheusAndHealthArePublicAndCarryTheGatewayRole() throws Exception {
        mvc.perform(get("/actuator/prometheus"))
                .andExpect(status().isOk())
                .andExpect(content().string(containsString("role=\"gateway\"")));
        mvc.perform(get("/actuator/health"))
                .andExpect(status().isOk());
    }

    @Test
    void everythingElseStillRedirectsToLogin() throws Exception {
        mvc.perform(get("/actuator/env"))
                .andExpect(status().is3xxRedirection())
                .andExpect(redirectedUrlPattern("**/login"));
        mvc.perform(get("/dashboard"))
                .andExpect(status().is3xxRedirection())
                .andExpect(redirectedUrlPattern("**/login"));
    }
}
