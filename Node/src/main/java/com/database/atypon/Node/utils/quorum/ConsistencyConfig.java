package com.database.atypon.Node.utils.quorum;

import java.util.Locale;

/**
 * Cluster-wide consistency parameters, read from the environment so every node of a deployment
 * agrees on them without touching code.
 */
public final class ConsistencyConfig {

    private static final String WRITE_CONSISTENCY_ENV = "WRITE_CONSISTENCY";
    private static final String READ_CONSISTENCY_ENV = "READ_CONSISTENCY";
    private static final ConsistencyLevel DEFAULT_LEVEL = ConsistencyLevel.QUORUM;

    private ConsistencyConfig() {
    }

    /** Replicas that must acknowledge a write: {@code WRITE_CONSISTENCY}, default {@code QUORUM}. */
    public static ConsistencyLevel writeConsistency() {
        return levelFromEnv(WRITE_CONSISTENCY_ENV, DEFAULT_LEVEL);
    }

    /** Replicas that must answer a read: {@code READ_CONSISTENCY}, default {@code QUORUM}. */
    public static ConsistencyLevel readConsistency() {
        return levelFromEnv(READ_CONSISTENCY_ENV, DEFAULT_LEVEL);
    }

    private static ConsistencyLevel levelFromEnv(String name, ConsistencyLevel defaultValue) {
        String raw = System.getenv(name);
        if (raw == null || raw.isBlank()) {
            return defaultValue;
        }
        try {
            return ConsistencyLevel.valueOf(raw.trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException(name + " must be one of ONE, QUORUM or ALL but was '" + raw + "'", e);
        }
    }
}
