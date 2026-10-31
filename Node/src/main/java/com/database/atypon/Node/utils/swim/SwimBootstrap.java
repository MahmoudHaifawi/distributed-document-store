package com.database.atypon.Node.utils.swim;

import com.database.atypon.Node.model.Network;
import com.database.atypon.Node.model.Node;
import com.database.atypon.Node.utils.swim.dto.MemberUpdate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.annotation.Profile;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * How a data node enters and leaves the cluster (node role only).
 *
 * <p><b>Join.</b> Once the application is ready to serve, the node registers itself in the
 * {@link MembershipTable} as {@code ALIVE} at incarnation zero, then asks the configured seeds
 * ({@link SwimConfig#seeds()}), in order, to admit it, until one answers. The answer is that seed's whole
 * member list, which the node adopts, and so the ring is built; the seed has queued the newcomer for gossip,
 * so the rest of the cluster hears of it from the seed's next probes without any broadcast. With no seed
 * reachable, or none configured, the node simply stays a cluster of one: others that start later find it
 * through their own join. Nothing here can stop the node starting.
 *
 * <p><b>Leave.</b> On shutdown the node hands its {@code LEFT} record to a few live peers, over a call of
 * its own ({@code /swim/leave}), so the ring drops it at once instead of waiting out a suspicion. Two
 * things are deliberately not done. The node does not merge {@code LEFT(self)} into its own table: the table
 * refutes any claim that this node is not alive, which would bump its incarnation and undo the leave. And the
 * record does not travel as ordinary gossip: every message built from the gossip queue also carries this
 * node's {@code ALIVE}, at the same incarnation, which would contradict the {@code LEFT} it rides with.
 *
 * <p>With {@code GOSSIP_ENABLED=false} neither happens: the membership table is not started, and the
 * coordinator's one-shot wiring remains in charge of the cluster.
 */
@Component
@Profile("node")
public class SwimBootstrap {

    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(SwimBootstrap.class);

    /** How many live peers are told of a departure: enough that one of them is up, few enough to be quick. */
    static final int LEAVE_FANOUT = 3;

    private final MembershipTable membership;
    private final SwimClient client;
    private final String serverPort;

    /** This node as it announced itself; null until {@link #join()} has got as far as knowing who it is. */
    private volatile Node self;
    private final AtomicBoolean left = new AtomicBoolean();

    @Autowired
    public SwimBootstrap(MembershipTable membership, SwimClient client, @Value("${server.port:8080}") String serverPort) {
        this.membership = membership;
        this.client = client;
        this.serverPort = serverPort;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void onReady() {
        join();
    }

    /**
     * Starts this node's membership and joins through the first seed that answers. True if a seed answered
     * and its member list was adopted; false if the node is on its own (gossip off, no identity, no seeds or
     * none reachable). Never throws.
     */
    public boolean join() {
        try {
            return joinCluster();
        } catch (RuntimeException e) {
            log.warn("SWIM: join failed, staying a single node: {}", e.toString());
            return false;
        }
    }

    private boolean joinCluster() {
        if (!SwimConfig.gossipEnabled()) {
            log.info("SWIM: GOSSIP_ENABLED=false, not joining; membership stays as the coordinator wires it");
            return false;
        }
        Node me = identity();
        if (me == null) {
            log.warn("SWIM: this node cannot tell what it is called (set NODE_NAME), so it cannot join a cluster");
            return false;
        }
        self = me;
        // The table tells this node from its peers by Network.self, so that is assigned first. A node has exactly
        // one identity: the ring is keyed by the name it announces, so if an identity was assigned under another
        // name it is replaced, not kept beside this one (that would put one machine on the ring twice, and make it
        // route to itself over HTTP).
        Node assigned = Network.self;
        if (assigned != null && !sameAddress(assigned, me)) {
            log.warn("SWIM: this node is configured as {}:{} but was already assigned the identity {}:{}; it uses {} only",
                    me.getName(), me.getPort(), assigned.getName(), assigned.getPort(), me.getName());
        }
        if (assigned == null || !sameAddress(assigned, me)) {
            Network.self = me;
        }
        membership.initSelf(me);
        MemberUpdate hello = MemberUpdate.from(membership.get(me.getName()));

        List<Node> seeds = SwimConfig.seeds();
        if (seeds.isEmpty()) {
            log.info("SWIM: no seeds configured (SEED_NODES / CLUSTER_NODES), {} starts as a cluster of one", me.getName());
            return false;
        }
        for (Node seed : seeds) {
            if (seed.getName().equals(me.getName()) && seed.getPort().equals(me.getPort())) {
                continue; // a seed list that names every node, this one included, is not a reason to join oneself
            }
            Optional<List<MemberUpdate>> view = client.join(seed, hello);
            if (view.isPresent() && namesAnyoneElse(view.get(), me)) {
                membership.mergeGossip(view.get());
                log.info("SWIM: {} joined the cluster through {}, which knows {} member(s)",
                        me.getName(), seed.getName(), view.get().size());
                return true;
            }
            // Either no answer, or an answer that is only this node: a seed address that leads back here under
            // another name (an alias, a load balancer). Neither is a cluster to join; there may be another seed.
            log.info("SWIM: seed {} did not lead to a cluster", seed.getName());
        }
        log.info("SWIM: no seed reachable, {} starts as a cluster of one", me.getName());
        return false;
    }

    private static boolean namesAnyoneElse(List<MemberUpdate> view, Node me) {
        for (MemberUpdate member : view) {
            if (member != null && member.getName() != null && !member.getName().equals(me.getName())) {
                return true;
            }
        }
        return false;
    }

    /** Best-effort goodbye to a few live peers; see the class comment for why it is a call of its own. */
    @PreDestroy
    public void leave() {
        Node me = self;
        if (me == null || !SwimConfig.gossipEnabled() || !left.compareAndSet(false, true)) {
            return;
        }
        try {
            Member mine = membership.get(me.getName());
            // At the incarnation this node has reached: a LEFT at a lower one would lose to what peers hold.
            MemberUpdate departure = new MemberUpdate(me.getName(), me.getPort(), MemberState.LEFT,
                    mine == null ? 0 : mine.getIncarnation());
            List<Node> peers = alivePeers(me);
            Collections.shuffle(peers);
            int told = 0;
            for (Node peer : peers.subList(0, Math.min(LEAVE_FANOUT, peers.size()))) {
                if (client.leave(peer, departure)) {
                    told++;
                }
            }
            log.info("SWIM: {} is leaving, {} peer(s) told", me.getName(), told);
        } catch (RuntimeException e) {
            log.warn("SWIM: leave failed, the detector will notice instead: {}", e.toString());
        }
    }

    /** Peers worth telling: believed alive, not merely suspected (a suspect is the likelier to be down). */
    private List<Node> alivePeers(Node me) {
        List<Node> peers = new ArrayList<>();
        for (Member member : membership.snapshot()) {
            if (member.getState() == MemberState.ALIVE && !member.getNode().getName().equals(me.getName())) {
                peers.add(member.getNode());
            }
        }
        return peers;
    }

    private static boolean sameAddress(Node a, Node b) {
        return Objects.equals(a.getName(), b.getName()) && Objects.equals(a.getPort(), b.getPort());
    }

    /**
     * Who this node is: the configured {@code NODE_NAME} (which wins, and replaces any identity assigned under
     * another name), else the identity it was already assigned, else its host name, on the port the server
     * listens on. Null only if all of that fails.
     *
     * <p>The name is what the ring is keyed by, so it has to be the same every time this node starts: a
     * different name moves every one of its virtual nodes, and data already on its disk is then looked for on
     * other nodes. {@code NODE_NAME} (or an assigned identity) is stable; the host name is not (under
     * docker-compose it is the container id, new for every recreated container), so falling back to it is
     * allowed, to let a bare node start, but is logged as a warning.
     */
    private Node identity() {
        String name = SwimConfig.nodeName();
        if (name == null) {
            Node assigned = Network.self;
            if (assigned != null) {
                return assigned;
            }
            name = hostName();
            if (name != null) {
                log.warn("SWIM: NODE_NAME is not set and no identity was assigned, so this node announces itself by its host "
                        + "name '{}'. That name is not stable (under docker-compose it is the container id, which changes when "
                        + "the container is recreated): the ring places data by node name, so after a restart under another "
                        + "name the data already on this node's disk may not be found. Set NODE_NAME.", name);
            }
        }
        return name == null ? null : new Node(name, serverPort);
    }

    private static String hostName() {
        try {
            String host = InetAddress.getLocalHost().getHostName();
            return (host == null || host.isBlank()) ? null : host;
        } catch (UnknownHostException e) {
            return null;
        }
    }
}
