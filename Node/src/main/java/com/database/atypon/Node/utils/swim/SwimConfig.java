package com.database.atypon.Node.utils.swim;

import com.database.atypon.Node.model.Node;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

/**
 * SWIM / gossip membership parameters, read from the environment so every node of a
 * deployment agrees on them without touching code.
 */
public final class SwimConfig {

    private static final String GOSSIP_ENABLED_ENV = "GOSSIP_ENABLED";
    private static final String SEED_NODES_ENV = "SEED_NODES";
    private static final String CLUSTER_NODES_ENV = "CLUSTER_NODES";
    private static final String NODE_NAME_ENV = "NODE_NAME";
    private static final String PERIOD_MS_ENV = "SWIM_PERIOD_MS";
    private static final String PING_TIMEOUT_MS_ENV = "SWIM_PING_TIMEOUT_MS";
    private static final String INDIRECT_K_ENV = "SWIM_INDIRECT_K";
    private static final String SUSPECT_TIMEOUT_MS_ENV = "SWIM_SUSPECT_TIMEOUT_MS";
    private static final String GOSSIP_LAMBDA_ENV = "SWIM_GOSSIP_LAMBDA";
    private static final String GOSSIP_MAX_ENV = "SWIM_GOSSIP_MAX";

    private static final boolean DEFAULT_GOSSIP_ENABLED = true;
    private static final long DEFAULT_PERIOD_MS = 1000L;
    private static final long DEFAULT_PING_TIMEOUT_MS = 500L;
    private static final int DEFAULT_INDIRECT_K = 3;
    private static final long DEFAULT_SUSPECT_TIMEOUT_MS = 5000L;
    private static final int DEFAULT_GOSSIP_LAMBDA = 3;
    private static final int DEFAULT_GOSSIP_MAX = 6;
    private static final String DEFAULT_PORT = "8080";

    /** Environment lookup; replaceable so tests need not mutate the real process environment. */
    static Function<String, String> env = System::getenv;

    private SwimConfig() {
    }

    /** Whether SWIM dynamic membership is on: {@code GOSSIP_ENABLED}, default true. */
    public static boolean gossipEnabled() {
        return boolFromEnv(GOSSIP_ENABLED_ENV, DEFAULT_GOSSIP_ENABLED);
    }

    /**
     * Contact points used to join the cluster: {@code SEED_NODES} as {@code host:port,...},
     * falling back to {@code CLUSTER_NODES}, or empty when neither is set.
     */
    public static List<Node> seeds() {
        String raw = env.apply(SEED_NODES_ENV);
        if (raw == null || raw.isBlank()) {
            raw = env.apply(CLUSTER_NODES_ENV);
        }
        if (raw == null || raw.isBlank()) {
            return Collections.emptyList();
        }
        return parseNodes(raw);
    }

    /**
     * The name this node goes by in the cluster: {@code NODE_NAME}, or {@code null} when it is unset or
     * blank (the caller then falls back to an identity it was assigned, or to the host name). It must be
     * spelled the way the other nodes reach this one, as in {@code SEED_NODES} / {@code CLUSTER_NODES}.
     */
    public static String nodeName() {
        String raw = env.apply(NODE_NAME_ENV);
        return (raw == null || raw.isBlank()) ? null : raw.trim();
    }

    /** Probe interval {@code T}: {@code SWIM_PERIOD_MS}, default 1000. */
    public static long periodMs() {
        return longFromEnv(PERIOD_MS_ENV, DEFAULT_PERIOD_MS);
    }

    /** Direct-ping ack timeout: {@code SWIM_PING_TIMEOUT_MS}, default 500. */
    public static long pingTimeoutMs() {
        return longFromEnv(PING_TIMEOUT_MS_ENV, DEFAULT_PING_TIMEOUT_MS);
    }

    /** Number of peers asked to probe indirectly: {@code SWIM_INDIRECT_K}, default 3. */
    public static int indirectK() {
        return intFromEnv(INDIRECT_K_ENV, DEFAULT_INDIRECT_K);
    }

    /** Time a suspect has to refute before being declared dead: {@code SWIM_SUSPECT_TIMEOUT_MS}, default 5000. */
    public static long suspectTimeoutMs() {
        return longFromEnv(SUSPECT_TIMEOUT_MS_ENV, DEFAULT_SUSPECT_TIMEOUT_MS);
    }

    /** Gossip retransmit multiplier lambda: {@code SWIM_GOSSIP_LAMBDA}, default 3. */
    public static int gossipLambda() {
        return intFromEnv(GOSSIP_LAMBDA_ENV, DEFAULT_GOSSIP_LAMBDA);
    }

    /** Max updates piggybacked on one message: {@code SWIM_GOSSIP_MAX}, default 6. */
    public static int gossipMax() {
        return intFromEnv(GOSSIP_MAX_ENV, DEFAULT_GOSSIP_MAX);
    }

    private static List<Node> parseNodes(String spec) {
        List<Node> nodes = new ArrayList<>();
        for (String part : spec.split(",")) {
            String entry = part.trim();
            if (entry.isEmpty()) {
                continue;
            }
            int sep = entry.lastIndexOf(':');
            String host = (sep < 0) ? entry : entry.substring(0, sep).trim();
            String port = (sep < 0) ? "" : entry.substring(sep + 1).trim();
            nodes.add(new Node(host, port.isEmpty() ? DEFAULT_PORT : port));
        }
        return Collections.unmodifiableList(nodes);
    }

    private static int intFromEnv(String name, int defaultValue) {
        String raw = env.apply(name);
        if (raw == null || raw.isBlank()) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(raw.trim());
        } catch (NumberFormatException e) {
            throw new IllegalStateException(name + " must be an integer but was '" + raw + "'", e);
        }
    }

    private static long longFromEnv(String name, long defaultValue) {
        String raw = env.apply(name);
        if (raw == null || raw.isBlank()) {
            return defaultValue;
        }
        try {
            return Long.parseLong(raw.trim());
        } catch (NumberFormatException e) {
            throw new IllegalStateException(name + " must be an integer but was '" + raw + "'", e);
        }
    }

    private static boolean boolFromEnv(String name, boolean defaultValue) {
        String raw = env.apply(name);
        if (raw == null || raw.isBlank()) {
            return defaultValue;
        }
        return Boolean.parseBoolean(raw.trim());
    }
}
