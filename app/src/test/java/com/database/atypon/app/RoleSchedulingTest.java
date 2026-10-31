package com.database.atypon.app;

import org.junit.jupiter.api.Test;
import org.springframework.scheduling.annotation.EnableScheduling;

import static org.assertj.core.api.Assertions.assertThat;

/** Timers belong to the node role only: it runs the SWIM detector; the coordinator and gateway have none. */
class RoleSchedulingTest {

    @Test
    void onlyTheNodeRoleEnablesScheduling() {
        assertThat(RoleConfigurations.NodeRole.class.isAnnotationPresent(EnableScheduling.class)).isTrue();
        assertThat(RoleConfigurations.CoordinatorRole.class.isAnnotationPresent(EnableScheduling.class)).isFalse();
        assertThat(RoleConfigurations.GatewayRole.class.isAnnotationPresent(EnableScheduling.class)).isFalse();
    }
}
