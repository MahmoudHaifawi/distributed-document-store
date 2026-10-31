package com.database.atypon.Node.model;

import com.database.atypon.Node.utils.ring.HashRing;
import com.database.atypon.Node.utils.ring.RingConfig;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

@Service
public class Network {

    public static List<Node> nodes;
    public static Node self;

    /**
     * The consistent-hash ring over every member of the cluster (peers plus this node). It is an
     * immutable snapshot, replaced wholesale by {@link #rebuildRing()} whenever membership changes.
     */
    public static volatile HashRing ring;

    /**
     * Serialises every change to the peer list and the ring so a rebuild never interleaves with a
     * membership projection. A dedicated lock, not the peer list itself: {@link #setLiveMembers}
     * replaces that list wholesale, so it cannot be the monitor guarding its own replacement.
     */
    private static final Object MEMBERSHIP_LOCK = new Object();

    public Network(){
        nodes = Collections.synchronizedList(new LinkedList<Node>());
    }

    public void addNode(Node node){
        nodes.add(node);
        rebuildRing();
    }
    public void setNodes(List<Node> nodes){
        this.nodes = Collections.synchronizedList(nodes);
        rebuildRing();
    }

    public List<Node> getNodes(){
        return nodes;
    }

    public void assignSelf(Node node) {
        this.self = node;
        rebuildRing();
    }

    public static Node getSelf() {
        return self;
    }

    /**
     * Rebuilds {@link #ring} from the known peers plus this node, so every member of the cluster
     * places schemas on the same ring. With no members at all there is nothing to place and the
     * ring is cleared.
     */
    public static void rebuildRing() {
        synchronized (MEMBERSHIP_LOCK) {
            Map<String, Node> members = new LinkedHashMap<>();
            List<Node> peers = nodes;
            if (peers != null) {
                synchronized (peers) {
                    for (Node peer : peers) {
                        members.putIfAbsent(peer.getName(), peer);
                    }
                }
            }
            Node me = self;
            if (me != null) {
                members.putIfAbsent(me.getName(), me);
            }
            ring = buildRing(members);
        }
    }

    /**
     * Projects the gossip membership table onto this class: {@code live} is every member currently
     * considered part of the cluster (alive or merely suspected), this node included. The peer list
     * {@link #nodes} becomes those members other than this node, and {@link #ring} is replaced by a
     * brand-new {@link HashRing} built from them. The ring is never mutated in place, so a reader that
     * grabbed {@code Network.ring} lock-free (as {@code RingRouter} does) sees either the whole old
     * ring or the whole new one, never a half-built one.
     *
     * <p>Public because the caller, {@code MembershipTable}, lives in another package.
     */
    public static void setLiveMembers(List<Node> live) {
        synchronized (MEMBERSHIP_LOCK) {
            Node me = self;
            Map<String, Node> members = new LinkedHashMap<>();
            List<Node> peers = new ArrayList<>();
            for (Node member : live) {
                if (members.putIfAbsent(member.getName(), member) != null) {
                    continue;
                }
                if (me == null || !me.getName().equals(member.getName())) {
                    peers.add(member);
                }
            }
            if (me != null) {
                members.putIfAbsent(me.getName(), me);
            }
            nodes = Collections.synchronizedList(peers);
            ring = buildRing(members);
        }
    }

    private static HashRing buildRing(Map<String, Node> members) {
        return members.isEmpty() ? null : new HashRing(List.copyOf(members.values()), RingConfig.vnodesPerNode());
    }
}
