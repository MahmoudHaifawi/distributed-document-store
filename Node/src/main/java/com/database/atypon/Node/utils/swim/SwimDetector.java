package com.database.atypon.Node.utils.swim;

import com.database.atypon.Node.model.Network;
import com.database.atypon.Node.model.Node;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntUnaryOperator;

/**
 * This node's SWIM failure detector. One call to {@link #tick} is one protocol period: it probes a
 * single peer and, if the peer cannot be shown to be alive, suspects it.
 *
 * <pre>
 * tick(now, rng):
 *   promote every suspect whose window has run out to DEAD
 *   M = one live peer other than this node, chosen by rng
 *   PING M directly                                   ack -> M is alive
 *   no ack: PING-REQ M through k other live peers     any ack -> M is alive
 *   no ack at all: M becomes SUSPECT (stamped now)
 * </pre>
 *
 * <p>The indirect probe is what keeps one bad link from condemning a healthy peer: a peer that this
 * node cannot reach but others can is left alone. The ping-reqs go out in parallel so that asking
 * {@code k} peers costs one timeout, not {@code k}.
 *
 * <p>Neither the clock nor the randomness is read here. The caller passes {@code now} and an
 * {@code rng} (given a bound {@code n}, it returns an index in {@code [0, n)}), so a test, or the
 * scheduler that drives the real protocol, decides both. The detector owns no timer.
 *
 * <p>A suspect is only ever <em>declared dead</em> here. Clearing a suspicion is the suspect's own
 * business: it refutes by raising its incarnation, which reaches this node as gossip, not as a
 * successful ping.
 */
@Service
public class SwimDetector {

    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(SwimDetector.class);

    /** A stable order over nodes, so that "the i-th peer" means the same thing on every tick. */
    private static final Comparator<Node> BY_ADDRESS =
            Comparator.comparing(Node::getName).thenComparing(Node::getPort);

    private final MembershipTable membership;
    private final SwimClient client;
    private final ExecutorService probePool;

    @Autowired
    public SwimDetector(MembershipTable membership, SwimClient client) {
        this.membership = membership;
        this.client = client;
        AtomicInteger sequence = new AtomicInteger();
        this.probePool = Executors.newCachedThreadPool(task -> {
            Thread thread = new Thread(task, "swim-ping-req-" + sequence.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        });
    }

    /**
     * Runs one protocol period as of {@code now}. Never throws: a failure anywhere in probing is logged
     * and the period ends, so a scheduler calling this repeatedly is never cancelled by it.
     */
    public void tick(long now, IntUnaryOperator rng) {
        try {
            // First, so that a peer whose window has run out is not probed again, only to be buried.
            membership.promoteExpiredSuspectsToDead(now);
            probeOnePeer(now, rng);
        } catch (RuntimeException e) {
            log.warn("SWIM tick failed: {}", e.toString());
        }
    }

    private void probeOnePeer(long now, IntUnaryOperator rng) {
        Node self = Network.self;
        if (self == null) {
            return; // not part of a cluster yet, so there is no telling peers from this node
        }
        List<Node> peers = livePeers(self);
        if (peers.isEmpty()) {
            return;
        }

        Node target = peers.remove(pick(rng, peers.size()));
        if (client.ping(target)) {
            return;
        }
        List<Node> helpers = pickHelpers(rng, peers);
        if (indirectlyAcked(target, helpers)) {
            return;
        }
        if (Thread.currentThread().isInterrupted()) {
            return; // shutting down mid-probe says nothing about the target
        }
        log.info("SWIM: {} did not ack a direct ping or {} indirect probes, suspecting it", target.getName(), helpers.size());
        membership.markSuspect(target.getName(), now);
    }

    /** Every live member other than this node, in a stable order. */
    private List<Node> livePeers(Node self) {
        List<Node> peers = new ArrayList<>();
        for (Node node : membership.liveNodes()) {
            if (!node.getName().equals(self.getName())) {
                peers.add(node);
            }
        }
        peers.sort(BY_ADDRESS);
        return peers;
    }

    /** Up to {@code k} of {@code candidates}, chosen by a partial shuffle driven by {@code rng}. */
    private List<Node> pickHelpers(IntUnaryOperator rng, List<Node> candidates) {
        List<Node> pool = new ArrayList<>(candidates);
        int wanted = Math.min(Math.max(SwimConfig.indirectK(), 0), pool.size());
        for (int i = 0; i < wanted; i++) {
            int j = i + pick(rng, pool.size() - i);
            Node chosen = pool.get(j);
            pool.set(j, pool.get(i));
            pool.set(i, chosen);
        }
        return new ArrayList<>(pool.subList(0, wanted));
    }

    /** True as soon as any of {@code via} reports that it reached {@code target}; false once all have failed. */
    private boolean indirectlyAcked(Node target, List<Node> via) {
        if (via.isEmpty()) {
            return false;
        }
        CompletionService<Boolean> answers = new ExecutorCompletionService<>(probePool);
        for (Node helper : via) {
            answers.submit(() -> client.pingReq(helper, target));
        }
        try {
            // each call is bounded by the client's own timeouts, so this wait is too
            for (int answered = 0; answered < via.size(); answered++) {
                if (reachedTarget(answers.take())) {
                    return true;
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return false;
    }

    private boolean reachedTarget(Future<Boolean> answer) throws InterruptedException {
        try {
            return Boolean.TRUE.equals(answer.get());
        } catch (ExecutionException e) {
            log.debug("SWIM indirect probe failed: {}", e.toString());
            return false;
        }
    }

    /** A valid index into a list of {@code size} elements, whatever {@code rng} returns. */
    private static int pick(IntUnaryOperator rng, int size) {
        return Math.floorMod(rng.applyAsInt(size), size);
    }

    /** Stops the threads that carry ping-reqs; a probe still in flight is abandoned. */
    @PreDestroy
    public void close() {
        probePool.shutdownNow();
    }
}
