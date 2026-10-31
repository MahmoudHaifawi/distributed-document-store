package com.database.atypon.app;

import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.actuate.autoconfigure.metrics.MeterRegistryCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;

/**
 * Observability wiring shared by every role: the Actuator exposure properties and the common tags stamped on every
 * meter, so one Prometheus can tell the gateway, the coordinator and each data node apart.
 *
 * <p>Tag keys {@code role} and {@code node} are reserved for these common tags; custom meters must not reuse them.
 */
@Configuration
@PropertySource("classpath:observability.properties")
public class ObservabilityConfig {

    static final String UNKNOWN = "unknown";

    /** Tags every meter with {@code role} (node|coordinator|gateway) and {@code node} (the stable {@code NODE_NAME}). */
    @Bean
    MeterRegistryCustomizer<MeterRegistry> commonTags(Environment env) {
        return registry -> registry.config().commonTags("role", role(env), "node", nodeName(env));
    }

    /** The single active role profile, or {@code unknown} when none of the role profiles is active. */
    static String role(Environment env) {
        for (String profile : env.getActiveProfiles()) {
            if (RoleResolver.ROLES.contains(profile)) {
                return profile;
            }
        }
        return UNKNOWN;
    }

    /** This process's {@code NODE_NAME}, or {@code unknown} when unset or blank (the coordinator and gateway have none). */
    static String nodeName(Environment env) {
        String name = env.getProperty("NODE_NAME");
        return name == null || name.isBlank() ? UNKNOWN : name.trim();
    }
}
