package com.database.atypon.app;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.EnableScheduling;

/** Role-selecting configurations: the active profile decides which module's beans are scanned. */
public class RoleConfigurations {

    /** Scheduling is on for the node role only: it is what runs the SWIM failure detector's periods. */
    @Configuration
    @Profile("node")
    @ComponentScan("com.database.atypon.Node")
    @EnableScheduling
    static class NodeRole {
    }

    @Configuration
    @Profile("coordinator")
    @ComponentScan("com.database.atypon.BootstrappingNode")
    static class CoordinatorRole {
    }

    @Configuration
    @Profile("gateway")
    @ComponentScan("com.database.atypon.DBMS")
    static class GatewayRole {
    }
}
