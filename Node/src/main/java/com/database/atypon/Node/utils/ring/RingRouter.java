package com.database.atypon.Node.utils.ring;

import com.database.atypon.Node.model.Network;
import com.database.atypon.Node.model.Node;

import java.util.List;

/**
 * Turns the cluster's {@link HashRing} into routing decisions for a schema: which nodes hold it, which
 * one is its primary, and what this node's part in that is. A schema is placed by the key
 * {@code database + "." + schema}.
 *
 * <p>A node with no peers owns every schema, so each question is answered without consulting the
 * ring. That keeps a standalone node working before (or without) any cluster membership being set.
 */
public final class RingRouter {

    private RingRouter() {
    }

    /** The nodes that hold a copy of the schema, primary first: at most the replication factor, never more than the cluster size. */
    public static List<Node> replicasFor(String database, String schema) {
        if (isSingleNode()) {
            Node me = Network.self;
            return me == null ? List.of() : List.of(me);
        }
        HashRing ring = currentRing();
        int rf = Math.min(RingConfig.replicationFactor(), ring.physicalNodeCount());
        return ring.preferenceList(keyOf(database, schema), rf);
    }

    /** The node that owns the schema and serialises its writes. */
    public static Node primaryFor(String database, String schema) {
        if (isSingleNode()) {
            return Network.self;
        }
        return currentRing().primary(keyOf(database, schema));
    }

    /** Whether this node is the schema's primary. A node with no peers is the primary of everything. */
    public static boolean isPrimary(String database, String schema) {
        if (isSingleNode()) {
            return true;
        }
        Node primary = primaryFor(database, schema);
        return primary != null && isSelf(primary);
    }

    /** Whether this node holds a copy of the schema. A node with no peers holds everything. */
    public static boolean isReplica(String database, String schema) {
        if (isSingleNode()) {
            return true;
        }
        return replicasFor(database, schema).stream().anyMatch(RingRouter::isSelf);
    }

    /** The schema's replicas other than this node, i.e. the peers a write here must be copied to. */
    public static List<Node> otherReplicas(String database, String schema) {
        if (isSingleNode()) {
            return List.of();
        }
        return replicasFor(database, schema).stream().filter(node -> !isSelf(node)).toList();
    }

    private static String keyOf(String database, String schema) {
        return database + "." + schema;
    }

    private static boolean isSingleNode() {
        List<Node> peers = Network.nodes;
        return peers == null || peers.isEmpty();
    }

    private static boolean isSelf(Node node) {
        Node me = Network.self;
        return me != null && me.getName().equals(node.getName());
    }

    /** The shared ring, built on first use if membership was set without rebuilding it. */
    private static HashRing currentRing() {
        HashRing ring = Network.ring;
        if (ring == null) {
            Network.rebuildRing();
            ring = Network.ring;
        }
        if (ring == null) {
            throw new IllegalStateException("No ring: the cluster has no members");
        }
        return ring;
    }
}
