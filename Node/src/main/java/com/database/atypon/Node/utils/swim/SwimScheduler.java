package com.database.atypon.Node.utils.swim;

import com.database.atypon.Node.model.Network;
import com.database.atypon.Node.model.Node;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntUnaryOperator;

/**
 * The clock of the failure detector (node role only): one protocol period every {@code SWIM_PERIOD_MS}.
 * {@link SwimDetector} owns no timer, so this is where real time and real randomness come in; tests drive
 * {@link #tick(long, IntUnaryOperator)} with their own. Scheduling is switched on for the node role alone
 * (see {@code RoleConfigurations}); the coordinator and the gateway run no timers.
 *
 * <p>With no peers a period is a no-op, so a node that has not joined anyone yet, or runs on its own, costs
 * nothing.
 *
 * <p><b>Probing the dead.</b> The detector probes only members it believes alive or suspected, and a member
 * that is written off stops being gossiped about after a few sends. Two halves of a cluster that were cut
 * off from each other long enough therefore each hold the other as {@code DEAD}, and nobody would ever
 * contact anybody again. So every {@link #HEAL_EVERY}th period this also pings one {@code DEAD} member, in turn
 * (never one that {@code LEFT}: it went on purpose). If it answers, the partition has healed, and the
 * exchange does the rest: the answer carries what that node believes about us, if it holds us as dead, and
 * ours what we believe about it ({@link MembershipTable#replyGossip}), each side refutes, and both rings
 * re-merge. A member that really is gone costs one unanswered ping now and then.
 */
@Component
@Profile("node")
public class SwimScheduler {

    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(SwimScheduler.class);

    /** One period in this many also probes a dead member. */
    static final int HEAL_EVERY = 5;

    private final SwimDetector detector;
    private final MembershipTable membership;
    private final SwimClient client;
    private final Random random = new Random();
    private final AtomicInteger periods = new AtomicInteger();
    private final AtomicInteger nextDead = new AtomicInteger();

    @Autowired
    public SwimScheduler(SwimDetector detector, MembershipTable membership, SwimClient client) {
        this.detector = detector;
        this.membership = membership;
        this.client = client;
    }

    /** One protocol period, on the real clock. Spring's scheduler never overlaps two runs of a fixed-rate task. */
    @Scheduled(initialDelayString = "#{T(com.database.atypon.Node.utils.swim.SwimConfig).periodMs()}",
            fixedRateString = "#{T(com.database.atypon.Node.utils.swim.SwimConfig).periodMs()}")
    public void tick() {
        tick(System.currentTimeMillis(), random::nextInt);
    }

    void tick(long now, IntUnaryOperator rng) {
        if (!SwimConfig.gossipEnabled()) {
            return;
        }
        detector.tick(now, rng);
        if (periods.incrementAndGet() % HEAL_EVERY == 0) {
            probeADeadMember();
        }
    }

    private void probeADeadMember() {
        try {
            Node me = Network.self;
            List<Node> dead = new ArrayList<>();
            for (Member member : membership.snapshot()) {
                if (member.getState() == MemberState.DEAD && (me == null || !member.getNode().getName().equals(me.getName()))) {
                    dead.add(member.getNode());
                }
            }
            if (dead.isEmpty()) {
                return;
            }
            Node target = dead.get(Math.floorMod(nextDead.getAndIncrement(), dead.size()));
            if (client.ping(target)) {
                log.info("SWIM: {} is held dead but answered a probe; the cluster is re-merging", target.getName());
            }
        } catch (RuntimeException e) {
            log.warn("SWIM heal probe failed: {}", e.toString());
        }
    }
}
