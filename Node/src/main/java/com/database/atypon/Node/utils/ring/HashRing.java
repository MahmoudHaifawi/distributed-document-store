package com.database.atypon.Node.utils.ring;

import com.database.atypon.Node.model.Node;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * An immutable consistent-hash ring. Every physical node owns {@code vnodesPerNode} points on the
 * ring, which spreads load evenly and means adding or removing a node only moves the keys that
 * fall next to its points. Placement depends only on node names, never on the order they are
 * supplied in, so every node of the cluster computes the same ring.
 */
public final class HashRing {

    private final NavigableMap<Long, Node> ring = new TreeMap<>();
    private final int physicalNodeCount;

    public HashRing(List<Node> nodes, int vnodesPerNode) {
        if (vnodesPerNode < 1) {
            throw new IllegalArgumentException("vnodesPerNode must be at least 1 but was " + vnodesPerNode);
        }
        for (Node node : nodes) {
            for (int vnode = 0; vnode < vnodesPerNode; vnode++) {
                ring.put(hash(node.getName() + "#" + vnode), node);
            }
        }
        this.physicalNodeCount = (int) nodes.stream().map(Node::getName).distinct().count();
    }

    /**
     * The distinct physical nodes found walking clockwise from {@code key}'s position on the ring,
     * closest first. Its size is {@code min(replicationFactor, physicalNodeCount())}.
     */
    public List<Node> preferenceList(String key, int replicationFactor) {
        int wanted = Math.min(replicationFactor, physicalNodeCount);
        long position = hash(key);
        Map<String, Node> owners = new LinkedHashMap<>();
        collectOwners(ring.tailMap(position, true).values(), owners, wanted);
        collectOwners(ring.headMap(position, false).values(), owners, wanted);
        return List.copyOf(owners.values());
    }

    /** The node that owns {@code key}: the first entry of its preference list. */
    public Node primary(String key) {
        return preferenceList(key, 1).get(0);
    }

    public int physicalNodeCount() {
        return physicalNodeCount;
    }

    /** First 8 bytes of the MD5 of {@code s}, read big-endian. Unlike {@code hashCode()}, stable across JVMs. */
    public static long hash(String s) {
        try {
            byte[] digest = MessageDigest.getInstance("MD5").digest(s.getBytes(StandardCharsets.UTF_8));
            return ByteBuffer.wrap(digest, 0, Long.BYTES).getLong();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("MD5 is required of every Java platform", e);
        }
    }

    private static void collectOwners(Collection<Node> vnodeOwners, Map<String, Node> owners, int wanted) {
        for (Node owner : vnodeOwners) {
            if (owners.size() >= wanted) {
                return;
            }
            owners.putIfAbsent(owner.getName(), owner);
        }
    }
}
