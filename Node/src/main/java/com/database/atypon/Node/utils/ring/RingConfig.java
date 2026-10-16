package com.database.atypon.Node.utils.ring;

/**
 * Cluster-wide sharding parameters, read from the environment so every node of a deployment
 * agrees on them without touching code.
 */
public final class RingConfig {

    private static final String REPLICATION_FACTOR_ENV = "REPLICATION_FACTOR";
    private static final String VNODES_PER_NODE_ENV = "VNODES_PER_NODE";
    private static final int DEFAULT_REPLICATION_FACTOR = 3;
    private static final int DEFAULT_VNODES_PER_NODE = 150;

    private RingConfig() {
    }

    /** Number of nodes that hold a copy of each schema: {@code REPLICATION_FACTOR}, default 3. */
    public static int replicationFactor() {
        return intFromEnv(REPLICATION_FACTOR_ENV, DEFAULT_REPLICATION_FACTOR);
    }

    /** Virtual nodes each physical node places on the ring: {@code VNODES_PER_NODE}, default 150. */
    public static int vnodesPerNode() {
        return intFromEnv(VNODES_PER_NODE_ENV, DEFAULT_VNODES_PER_NODE);
    }

    private static int intFromEnv(String name, int defaultValue) {
        String raw = System.getenv(name);
        if (raw == null || raw.isBlank()) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(raw.trim());
        } catch (NumberFormatException e) {
            throw new IllegalStateException(name + " must be an integer but was '" + raw + "'", e);
        }
    }
}
