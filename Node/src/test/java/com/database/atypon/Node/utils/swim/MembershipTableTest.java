package com.database.atypon.Node.utils.swim;

import com.database.atypon.Node.model.Network;
import com.database.atypon.Node.model.Node;
import com.database.atypon.Node.utils.ring.HashRing;
import com.database.atypon.Node.utils.swim.dto.MemberUpdate;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

class MembershipTableTest {

    private static final String SELF = "Node0";

    private List<Node> savedNodes;
    private Node savedSelf;
    private HashRing savedRing;

    private AtomicLong clock;
    private MembershipTable table;

    @BeforeEach
    void setUp() {
        savedNodes = Network.nodes;
        savedSelf = Network.self;
        savedRing = Network.ring;

        Network.nodes = Collections.synchronizedList(new ArrayList<>());
        Network.self = n(SELF);
        Network.ring = null;

        clock = new AtomicLong(1_000L);
        table = new MembershipTable(clock::get);
        table.initSelf(n(SELF));
    }

    @AfterEach
    void restoreMembership() {
        Network.nodes = savedNodes;
        Network.self = savedSelf;
        Network.ring = savedRing;
    }

    // ---- merge rule ---------------------------------------------------------------------------

    @Test
    void mergeIsHigherIncarnationWins() {
        assertThat(table.merge(m("Node1", MemberState.ALIVE, 1))).isTrue();

        // A lower incarnation never overrides, even with "worse" news.
        assertThat(table.merge(m("Node1", MemberState.DEAD, 0))).isFalse();
        assertThat(table.get("Node1").getState()).isEqualTo(MemberState.ALIVE);
        assertThat(table.get("Node1").getIncarnation()).isEqualTo(1);

        // A higher incarnation overrides, even though it carries "better" news than DEAD.
        assertThat(table.merge(m("Node1", MemberState.DEAD, 2))).isTrue();
        assertThat(table.merge(m("Node1", MemberState.ALIVE, 3))).isTrue();
        assertThat(table.get("Node1").getState()).isEqualTo(MemberState.ALIVE);
        assertThat(table.get("Node1").getIncarnation()).isEqualTo(3);
    }

    @Test
    void mergeStatePrecedenceAtEqualIncarnation() {
        table.merge(m("Node1", MemberState.ALIVE, 4));

        assertThat(table.merge(m("Node1", MemberState.ALIVE, 4))).as("identical update is a no-op").isFalse();
        assertThat(table.merge(m("Node1", MemberState.SUSPECT, 4))).as("SUSPECT beats ALIVE").isTrue();
        assertThat(table.merge(m("Node1", MemberState.DEAD, 4))).as("DEAD beats SUSPECT").isTrue();
        assertThat(table.get("Node1").getState()).isEqualTo(MemberState.DEAD);

        assertThat(table.merge(m("Node1", MemberState.ALIVE, 4))).as("ALIVE does not beat DEAD").isFalse();
        assertThat(table.merge(m("Node1", MemberState.SUSPECT, 4))).as("SUSPECT does not beat DEAD").isFalse();
        assertThat(table.get("Node1").getState()).isEqualTo(MemberState.DEAD);
        assertThat(table.get("Node1").getIncarnation()).isEqualTo(4);

        // DEAD beats ALIVE directly, with no SUSPECT in between.
        table.merge(m("Node2", MemberState.ALIVE, 7));
        assertThat(table.merge(m("Node2", MemberState.DEAD, 7))).isTrue();
        assertThat(table.get("Node2").getState()).isEqualTo(MemberState.DEAD);

        // LEFT carries DEAD's precedence, so it also beats ALIVE at an equal incarnation.
        table.merge(m("Node3", MemberState.ALIVE, 2));
        assertThat(table.merge(m("Node3", MemberState.LEFT, 2))).isTrue();
        assertThat(table.get("Node3").getState()).isEqualTo(MemberState.LEFT);
    }

    @Test
    void mergeIsOrderIndependent() {
        List<Member> updates = List.of(
                m("Node1", MemberState.ALIVE, 0),
                m("Node1", MemberState.SUSPECT, 0),
                m("Node1", MemberState.ALIVE, 1),
                m("Node1", MemberState.SUSPECT, 1),
                m("Node2", MemberState.ALIVE, 0),
                m("Node2", MemberState.DEAD, 0),
                m("Node2", MemberState.SUSPECT, 0),
                m("Node3", MemberState.ALIVE, 5),
                m("Node3", MemberState.LEFT, 3),
                m("Node3", MemberState.DEAD, 4),
                m("Node4", MemberState.SUSPECT, 2),
                m("Node4", MemberState.ALIVE, 2),
                m("Node4", MemberState.ALIVE, 1),
                // Claims about this node itself: refuted, and the refutation must land on the same
                // incarnation whichever order the claims arrive in.
                m(SELF, MemberState.SUSPECT, 2),
                m(SELF, MemberState.DEAD, 3),
                m(SELF, MemberState.ALIVE, 1),
                m(SELF, MemberState.SUSPECT, 1));

        TreeMap<String, String> expected = new TreeMap<>();
        expected.put("Node1", "SUSPECT@1");
        expected.put("Node2", "DEAD@0");
        expected.put("Node3", "ALIVE@5");
        expected.put("Node4", "SUSPECT@2");
        expected.put(SELF, "ALIVE@4");

        Random random = new Random(42);
        for (int run = 0; run < 200; run++) {
            List<Member> shuffled = new ArrayList<>(updates);
            Collections.shuffle(shuffled, random);

            MembershipTable fresh = new MembershipTable(clock::get);
            fresh.initSelf(n(SELF));
            shuffled.forEach(fresh::merge);

            assertThat(finalMap(fresh)).as("run %d, order %s", run, describe(shuffled)).isEqualTo(expected);
        }
    }

    // ---- refutation ---------------------------------------------------------------------------

    @Test
    void selfSuspicionIsRefutedWithHigherIncarnation() {
        assertThat(table.merge(m(SELF, MemberState.SUSPECT, 2))).isTrue();

        Member me = table.get(SELF);
        assertThat(me.getState()).isEqualTo(MemberState.ALIVE);
        assertThat(me.getIncarnation()).isEqualTo(3);

        // Being declared dead at the incarnation we just reached is refuted the same way.
        table.merge(m(SELF, MemberState.DEAD, 3));
        assertThat(table.get(SELF).getState()).isEqualTo(MemberState.ALIVE);
        assertThat(table.get(SELF).getIncarnation()).isEqualTo(4);

        // A stale claim (lower incarnation than the one we already hold) is simply ignored.
        assertThat(table.merge(m(SELF, MemberState.SUSPECT, 1))).isFalse();
        assertThat(table.get(SELF).getIncarnation()).isEqualTo(4);
    }

    @Test
    void refutationKeepsSelfLiveAndInTheRing() {
        table.merge(m("Node1", MemberState.ALIVE, 0));

        table.merge(m(SELF, MemberState.DEAD, 0));

        assertThat(names(table.liveNodes())).contains(SELF);
        assertThat(Network.ring.physicalNodeCount()).isEqualTo(2);
    }

    @Test
    void refutationIsQueuedForGossip() {
        table.merge(m(SELF, MemberState.SUSPECT, 2));

        List<Member> gossip = table.drainGossip(10);

        assertThat(gossip).hasSize(1);
        assertThat(gossip.get(0).getNode().getName()).isEqualTo(SELF);
        assertThat(gossip.get(0).getState()).isEqualTo(MemberState.ALIVE);
        assertThat(gossip.get(0).getIncarnation()).isEqualTo(3);
    }

    @Test
    void enqueuedUpdatesAreDrainedAtMostMaxAtATime() {
        table.enqueue(m("Node1", MemberState.ALIVE, 0));
        table.enqueue(m("Node2", MemberState.ALIVE, 0));
        table.enqueue(m("Node3", MemberState.ALIVE, 0));

        // Updates stay queued for several sends each (retransmit counters, see GossipTest), so what a
        // drain returns is capped by max, not by how many are left.
        assertThat(table.drainGossip(2)).hasSize(2);
        assertThat(table.drainGossip(2)).hasSize(2);
        assertThat(table.drainGossip(10)).hasSize(3);
    }

    // ---- live set -----------------------------------------------------------------------------

    @Test
    void deadAndLeftLeaveTheLiveSet() {
        table.merge(m("Node1", MemberState.ALIVE, 0));
        table.merge(m("Node2", MemberState.ALIVE, 0));
        assertThat(names(table.liveNodes())).containsExactlyInAnyOrder(SELF, "Node1", "Node2");
        assertThat(Network.ring.physicalNodeCount()).isEqualTo(3);

        table.merge(m("Node1", MemberState.DEAD, 0));
        table.merge(m("Node2", MemberState.LEFT, 0));

        assertThat(names(table.liveNodes())).containsExactly(SELF);
        assertThat(Network.ring.physicalNodeCount()).isEqualTo(1);
        assertThat(Network.nodes).isEmpty();

        // The tombstones stay in the table, so a stale ALIVE at the old incarnation cannot resurrect them...
        assertThat(table.snapshot()).extracting(x -> x.getNode().getName()).contains("Node1", "Node2");
        assertThat(table.merge(m("Node1", MemberState.ALIVE, 0))).isFalse();
        assertThat(names(table.liveNodes())).containsExactly(SELF);

        // ...but a rejoin at a higher incarnation does.
        assertThat(table.merge(m("Node1", MemberState.ALIVE, 1))).isTrue();
        assertThat(names(table.liveNodes())).containsExactlyInAnyOrder(SELF, "Node1");
        assertThat(Network.ring.physicalNodeCount()).isEqualTo(2);
    }

    @Test
    void liveSetIsAlivePlusSuspect() {
        table.merge(m("Node1", MemberState.ALIVE, 0));
        table.merge(m("Node2", MemberState.SUSPECT, 0));
        table.merge(m("Node3", MemberState.DEAD, 0));
        table.merge(m("Node4", MemberState.LEFT, 0));

        assertThat(names(table.liveNodes())).containsExactlyInAnyOrder(SELF, "Node1", "Node2");
        assertThat(Network.ring.physicalNodeCount()).isEqualTo(3);
        // Network.nodes is the peer list: the live members other than this node.
        assertThat(names(Network.nodes)).containsExactlyInAnyOrder("Node1", "Node2");
    }

    // ---- ring projection ----------------------------------------------------------------------

    @Test
    void ringProjectionSwapsAtomically() {
        HashRing alone = Network.ring;
        assertThat(alone).isNotNull();
        assertThat(alone.physicalNodeCount()).isEqualTo(1);

        table.merge(m("Node1", MemberState.ALIVE, 0));
        HashRing pair = Network.ring;

        assertThat(pair).isNotSameAs(alone);
        assertThat(pair.physicalNodeCount()).isEqualTo(2);
        // The earlier snapshot was replaced, not mutated in place: it is still the complete 1-node ring.
        assertThat(alone.physicalNodeCount()).isEqualTo(1);
        assertThat(alone.preferenceList("shop.products", 3)).hasSize(1);
        assertThat(pair.preferenceList("shop.products", 3)).hasSize(2);

        table.merge(m("Node1", MemberState.DEAD, 0));
        HashRing backToAlone = Network.ring;

        assertThat(backToAlone).isNotSameAs(pair);
        assertThat(backToAlone.physicalNodeCount()).isEqualTo(1);
        assertThat(pair.physicalNodeCount()).isEqualTo(2);
    }

    @Test
    void suspectingAMemberDoesNotTouchTheRing() {
        table.merge(m("Node1", MemberState.ALIVE, 0));
        HashRing before = Network.ring;

        table.markSuspect("Node1");

        assertThat(table.get("Node1").getState()).isEqualTo(MemberState.SUSPECT);
        assertThat(Network.ring).isSameAs(before);
        assertThat(Network.ring.physicalNodeCount()).isEqualTo(2);
    }

    @Test
    void ringReadersNeverSeeAPartialRing() throws Exception {
        AtomicBoolean done = new AtomicBoolean(false);
        AtomicReference<String> torn = new AtomicReference<>();

        Thread reader = new Thread(() -> {
            while (!done.get()) {
                HashRing ring = Network.ring;
                if (ring == null) {
                    torn.compareAndSet(null, "ring was null mid-change");
                    return;
                }
                // A complete ring answers a full-width preference list for every key.
                int members = ring.physicalNodeCount();
                int answered = ring.preferenceList("shop.products", 10).size();
                if (members < 1 || answered != members) {
                    torn.compareAndSet(null, "ring claims " + members + " members but placed " + answered);
                    return;
                }
            }
        });
        reader.start();
        try {
            for (int round = 0; round < 20; round++) {
                for (int i = 1; i <= 4; i++) {
                    table.merge(m("Node" + i, MemberState.ALIVE, round * 2));
                }
                for (int i = 1; i <= 4; i++) {
                    table.merge(m("Node" + i, MemberState.DEAD, round * 2));
                }
            }
        } finally {
            done.set(true);
            reader.join(5_000);
        }

        assertThat(torn.get()).isNull();
        assertThat(Network.ring.physicalNodeCount()).isEqualTo(1);
    }

    @Test
    void initSelfAloneProjectsASingleNodeRing() {
        assertThat(table.liveNodes()).hasSize(1);
        assertThat(Network.nodes).isEmpty();
        assertThat(Network.ring.physicalNodeCount()).isEqualTo(1);
        assertThat(table.get(SELF).getState()).isEqualTo(MemberState.ALIVE);
        assertThat(table.get(SELF).getIncarnation()).isZero();
    }

    // ---- suspicion window ---------------------------------------------------------------------

    @Test
    void suspectTimesOutToDead() {
        long timeout = SwimConfig.suspectTimeoutMs();
        table.merge(m("Node1", MemberState.ALIVE, 0));
        assertThat(Network.ring.physicalNodeCount()).isEqualTo(2);

        table.markSuspect("Node1");
        long suspectedAt = clock.get();

        table.promoteExpiredSuspectsToDead(suspectedAt + timeout - 1);
        assertThat(table.get("Node1").getState()).isEqualTo(MemberState.SUSPECT);
        assertThat(Network.ring.physicalNodeCount()).isEqualTo(2);

        table.promoteExpiredSuspectsToDead(suspectedAt + timeout);
        assertThat(table.get("Node1").getState()).isEqualTo(MemberState.DEAD);
        assertThat(table.get("Node1").getIncarnation()).isZero();
        assertThat(names(table.liveNodes())).containsExactly(SELF);
        assertThat(Network.ring.physicalNodeCount()).isEqualTo(1);
    }

    @Test
    void suspicionWindowIsMeasuredFromTheSuspectStampNotTheAliveStamp() {
        long timeout = SwimConfig.suspectTimeoutMs();

        // Node1 has been ALIVE since t=1000, long before anyone suspects it.
        table.merge(m("Node1", MemberState.ALIVE, 0));
        long aliveStamp = table.get("Node1").getLastChange();
        clock.set(aliveStamp + 10 * timeout);

        table.markSuspect("Node1");
        long suspectStamp = clock.get();
        assertThat(table.get("Node1").getLastChange()).isEqualTo(suspectStamp);
        assertThat(suspectStamp - aliveStamp).isGreaterThan(timeout);

        // The very next sweep is far past the ALIVE stamp + timeout, but only just after the SUSPECT stamp:
        // the node must still have its refutation window.
        table.promoteExpiredSuspectsToDead(suspectStamp + 1);
        assertThat(table.get("Node1").getState()).isEqualTo(MemberState.SUSPECT);
        assertThat(names(table.liveNodes())).contains("Node1");

        table.promoteExpiredSuspectsToDead(suspectStamp + timeout);
        assertThat(table.get("Node1").getState()).isEqualTo(MemberState.DEAD);
    }

    @Test
    void suspicionGossipedFromAPeerStartsTheLocalWindowOnReceipt() {
        long timeout = SwimConfig.suspectTimeoutMs();
        table.merge(m("Node1", MemberState.ALIVE, 0));

        clock.set(50_000L);
        // The sender's own timestamp (here ancient) means nothing locally; the window starts when we learn of it.
        table.merge(new Member(n("Node1"), MemberState.SUSPECT, 0, 1L));

        assertThat(table.get("Node1").getLastChange()).isEqualTo(50_000L);
        table.promoteExpiredSuspectsToDead(50_000L + timeout - 1);
        assertThat(table.get("Node1").getState()).isEqualTo(MemberState.SUSPECT);
    }

    @Test
    void remarkingASuspectDoesNotExtendItsWindow() {
        long timeout = SwimConfig.suspectTimeoutMs();
        table.merge(m("Node1", MemberState.ALIVE, 0));
        table.markSuspect("Node1");
        long firstSuspectStamp = clock.get();

        clock.set(firstSuspectStamp + timeout - 1);
        table.markSuspect("Node1");

        assertThat(table.get("Node1").getLastChange()).isEqualTo(firstSuspectStamp);
        table.promoteExpiredSuspectsToDead(firstSuspectStamp + timeout);
        assertThat(table.get("Node1").getState()).isEqualTo(MemberState.DEAD);
    }

    @Test
    void markSuspectWithAnExplicitTimeStampsThatTime() {
        long timeout = SwimConfig.suspectTimeoutMs();
        table.merge(m("Node1", MemberState.ALIVE, 0));

        table.markSuspect("Node1", 777_000L);

        assertThat(table.get("Node1").getLastChange()).isEqualTo(777_000L);
        table.promoteExpiredSuspectsToDead(777_000L + timeout - 1);
        assertThat(table.get("Node1").getState()).isEqualTo(MemberState.SUSPECT);
        table.promoteExpiredSuspectsToDead(777_000L + timeout);
        assertThat(table.get("Node1").getState()).isEqualTo(MemberState.DEAD);
    }

    @Test
    void refutedSuspicionIsNotPromotedToDead() {
        long timeout = SwimConfig.suspectTimeoutMs();
        table.merge(m("Node1", MemberState.ALIVE, 0));
        table.markSuspect("Node1");
        long suspectedAt = clock.get();

        // Node1 refutes with a higher incarnation before the window closes.
        table.merge(m("Node1", MemberState.ALIVE, 1));
        table.promoteExpiredSuspectsToDead(suspectedAt + 10 * timeout);

        assertThat(table.get("Node1").getState()).isEqualTo(MemberState.ALIVE);
        assertThat(table.get("Node1").getIncarnation()).isEqualTo(1);
        assertThat(names(table.liveNodes())).contains("Node1");
    }

    @Test
    void markSuspectOnlyAffectsAliveRemotePeers() {
        table.merge(m("Node1", MemberState.DEAD, 0));

        table.markSuspect("Node1");
        table.markSuspect("Unknown");
        table.markSuspect(SELF);

        assertThat(table.get("Node1").getState()).isEqualTo(MemberState.DEAD);
        assertThat(table.get("Unknown")).isNull();
        // Suspecting ourselves would trigger our own refutation; it must simply be refused.
        assertThat(table.get(SELF).getState()).isEqualTo(MemberState.ALIVE);
        assertThat(table.get(SELF).getIncarnation()).isZero();
    }

    @Test
    void promoteNeverTouchesSelf() {
        table.promoteExpiredSuspectsToDead(clock.get() + 100 * SwimConfig.suspectTimeoutMs());

        assertThat(table.get(SELF).getState()).isEqualTo(MemberState.ALIVE);
        assertThat(Network.ring.physicalNodeCount()).isEqualTo(1);
    }

    // ---- change listeners ---------------------------------------------------------------------

    @Test
    void aLiveSetChangeNotifiesTheListenerOnce() {
        AtomicInteger notified = new AtomicInteger();
        table.addChangeListener(notified::incrementAndGet);

        table.merge(m("Node1", MemberState.ALIVE, 0));
        assertThat(notified.get()).as("a member joined the live set").isEqualTo(1);

        table.merge(m("Node1", MemberState.DEAD, 0));
        assertThat(notified.get()).as("a member left the live set").isEqualTo(2);
    }

    @Test
    void nothingIsNotifiedWhenTheLiveSetStaysTheSame() {
        table.merge(m("Node1", MemberState.ALIVE, 0));
        AtomicInteger notified = new AtomicInteger();
        table.addChangeListener(notified::incrementAndGet);

        table.merge(m("Node1", MemberState.ALIVE, 0));        // nothing new
        table.merge(m("Node1", MemberState.SUSPECT, 0));      // changes the table, but a suspect is still on the ring
        table.merge(m("Ghost", MemberState.DEAD, 3));         // a tombstone for a node that was never live
        table.markSuspect("Node0");                           // this node never suspects itself

        assertThat(notified.get()).isZero();
    }

    @Test
    void aSuspectDeclaredDeadNotifiesTheListener() {
        table.merge(m("Node1", MemberState.ALIVE, 0));
        table.markSuspect("Node1", clock.get());
        AtomicInteger notified = new AtomicInteger();
        table.addChangeListener(notified::incrementAndGet);

        table.promoteExpiredSuspectsToDead(clock.get() + SwimConfig.suspectTimeoutMs());

        assertThat(notified.get()).isEqualTo(1);
    }

    @Test
    void aBatchOfGossipNotifiesOncePerBatch() {
        AtomicInteger notified = new AtomicInteger();
        table.addChangeListener(notified::incrementAndGet);

        table.mergeGossip(List.of(
                new MemberUpdate("Node1", "8080", MemberState.ALIVE, 0),
                new MemberUpdate("Node2", "8080", MemberState.ALIVE, 0)));

        assertThat(table.liveNodes()).hasSize(3);
        assertThat(notified.get()).as("the listener sees the whole batch, once").isEqualTo(1);
    }

    @Test
    void listenersRunAfterTheTableLockIsReleased() throws Exception {
        AtomicBoolean tableReadableMeanwhile = new AtomicBoolean();
        table.addChangeListener(() -> {
            // A pull started from here must be free to use the table (and whoever else wants it must not queue
            // behind this thread): reading it from another thread proves the lock is not held.
            Thread reader = new Thread(() -> {
                table.snapshot();
                tableReadableMeanwhile.set(true);
            });
            reader.start();
            try {
                reader.join(2_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        table.merge(m("Node1", MemberState.ALIVE, 0));

        assertThat(tableReadableMeanwhile.get()).isTrue();
    }

    @Test
    void theRingIsAlreadyProjectedWhenTheListenerRuns() {
        AtomicInteger ringSize = new AtomicInteger();
        table.addChangeListener(() -> ringSize.set(Network.ring.physicalNodeCount()));

        table.merge(m("Node1", MemberState.ALIVE, 0));

        assertThat(ringSize.get()).isEqualTo(2);
    }

    @Test
    void aFailingListenerDoesNotBreakTheTableOrTheOtherListeners() {
        AtomicInteger notified = new AtomicInteger();
        table.addChangeListener(() -> {
            throw new IllegalStateException("boom");
        });
        table.addChangeListener(notified::incrementAndGet);

        assertThat(table.merge(m("Node1", MemberState.ALIVE, 0))).isTrue();

        assertThat(notified.get()).isEqualTo(1);
        assertThat(table.get("Node1").getState()).isEqualTo(MemberState.ALIVE);
    }

    @Test
    void aRemovedListenerIsNotNotified() {
        AtomicInteger notified = new AtomicInteger();
        Runnable listener = notified::incrementAndGet;
        table.addChangeListener(listener);
        table.removeChangeListener(listener);

        table.merge(m("Node1", MemberState.ALIVE, 0));

        assertThat(notified.get()).isZero();
    }

    // ---- helpers ------------------------------------------------------------------------------

    private static Node n(String name) {
        return new Node(name, "8080");
    }

    private static Member m(String name, MemberState state, int incarnation) {
        return new Member(n(name), state, incarnation, 0L);
    }

    private static List<String> names(List<Node> nodes) {
        List<Node> copy;
        synchronized (nodes) {
            copy = new ArrayList<>(nodes);
        }
        return copy.stream().map(Node::getName).collect(Collectors.toList());
    }

    private static TreeMap<String, String> finalMap(MembershipTable t) {
        TreeMap<String, String> map = new TreeMap<>();
        for (Member member : t.snapshot()) {
            map.put(member.getNode().getName(), member.getState() + "@" + member.getIncarnation());
        }
        return map;
    }

    private static String describe(List<Member> updates) {
        return updates.stream()
                .map(u -> u.getNode().getName() + ":" + u.getState() + "@" + u.getIncarnation())
                .collect(Collectors.joining(", ", "[", "]"));
    }
}
