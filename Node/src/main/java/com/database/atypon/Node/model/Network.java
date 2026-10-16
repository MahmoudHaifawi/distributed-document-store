package com.database.atypon.Node.model;

import com.database.atypon.Node.utils.ring.HashRing;
import com.database.atypon.Node.utils.ring.RingConfig;
import org.springframework.stereotype.Service;

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
        ring = members.isEmpty() ? null : new HashRing(List.copyOf(members.values()), RingConfig.vnodesPerNode());
    }
}
