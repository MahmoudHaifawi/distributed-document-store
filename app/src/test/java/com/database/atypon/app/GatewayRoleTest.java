package com.database.atypon.app;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("gateway")
class GatewayRoleTest {

    @Autowired
    ApplicationContext ctx;
    @Autowired
    MockMvc mvc;

    @Test
    void gatewayBeansLoadAndNodeBeansDoNot() {
        assertThat(ctx.getBeanNamesForType(com.database.atypon.DBMS.controller.DashboardController.class)).isNotEmpty();
        assertThat(ctx.getBeanNamesForType(com.database.atypon.Node.controllers.read.ReadController.class)).isEmpty();
    }

    @Test
    void gatewayRunsNoTimers() {
        // scheduling is enabled for the node role alone (it drives the SWIM detector)
        assertThat(ctx.getBeanNamesForType(org.springframework.scheduling.annotation.ScheduledAnnotationBeanPostProcessor.class)).isEmpty();
    }

    @Test
    void loginPageRendersFromTemplatesOnTheClasspath() throws Exception {
        mvc.perform(get("/login")).andExpect(status().isOk());
    }
}
