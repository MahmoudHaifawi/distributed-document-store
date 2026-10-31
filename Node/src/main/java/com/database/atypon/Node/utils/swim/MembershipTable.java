package com.database.atypon.Node.utils.swim;

import com.database.atypon.Node.model.Network;
import com.database.atypon.Node.model.Node;
import com.database.atypon.Node.utils.swim.dto.MemberUpdate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.LongSupplier;

/**
 * The single, locked source of truth for who is in the cluster. Every detector tick, gossip
 * message, join and leave funnels through here, and {@link Network#nodes}/{@link Network#ring} are
 * only ever a projection of the live set computed below.
 *
 * <p>Competing claims about one member are resolved by {@link #merge(Member)}: the higher
 * incarnation wins, and at an equal incarnation the "worse" state wins ({@code ALIVE < SUSPECT <
 * DEAD = LEFT}). That rule is commutative, associative and idempotent, so any two nodes that have
 * seen the same set of updates, in any order, hold the same table. Only a member itself can raise its
 * own incarnation, which is how a wrongly suspected node clears its name: told it is suspect or dead
 * at incarnation {@code i}, it re-enters as {@code ALIVE} at {@code i + 1}, which dominates.
 *
 * <p>Every update that changes the table is also queued for gossip, from inside the same lock that
 * applied it (see {@link #merge(Member)}). Each queued update carries a retransmit counter,
 * {@code ceil(lambda * log2(N + 1))} for {@code N} live members, and {@link #outgoingGossip(int)} hands out
 * the updates with the most sends left, a few per probe, until each has been sent that many times:
 * infection-style dissemination that reaches everyone in O(log N) periods without any broadcast.
 *
 * <p>All state is guarded by one lock, and the ring projection is published from inside it, so
 * concurrent updates cannot reorder their effects on {@link Network}. {@link Network} never calls
 * back into this class, so the lock order is always table then network.
 *
 * <p>{@link Network#self} must be assigned (as the coordinator already does) before
 * {@link #initSelf(Node)}, otherwise the projection cannot tell this node from its peers.
 */
@Service
public class MembershipTable {

    private final Object lock = new Object();
    private final LongSupplier clock;

    /** Every member ever heard of, keyed by node name. DEAD and LEFT records stay, as tombstones. */
    private final Map<String, Member> members = new LinkedHashMap<>();

    /**
     * Updates waiting to be piggybacked on outgoing messages, keyed by node name, oldest first. At most
     * one per node: a stronger claim replaces the queued one and a weaker or equal one is ignored.
     */
    private final Map<String, Pending> pendingGossip = new LinkedHashMap<>();

    private String selfName;
    private int selfIncarnation;

    @Autowired
    public MembershipTable() {
        this(System::currentTimeMillis);
    }

    /** Test seam: {@code clock} supplies the epoch-millis stamped on every record the table adopts. */
    MembershipTable(LongSupplier clock) {
        this.clock = clock;
    }

    /**
     * Registers this node as the first member, {@code ALIVE} at incarnation zero, and projects it
     * onto {@link Network}. Calling it again keeps the incarnation already reached.
     */
    public void initSelf(Node self) {
        synchronized (lock) {
            Set<String> liveBefore = liveKeys();
            selfName = self.getName();
            members.put(selfName, new Member(self, MemberState.ALIVE, selfIncarnation, clock.getAsLong()));
            projectIfLiveSetChanged(liveBefore);
        }
    }

    /**
     * Folds one claim about a member into the table and returns whether the table changed.
     *
     * <pre>
     * existing == null                                        -> store
     * update.incarnation  &gt; existing.incarnation               -> store
     * equal incarnation and update precedence &gt; existing's     -> store
     * otherwise                                               -> ignore
     * a stored, non-ALIVE record about this node is refuted:
     *     selfIncarnation = max(selfIncarnation, record.incarnation) + 1, stored as ALIVE
     * </pre>
     *
     * The adopted record is stamped with the local clock rather than the sender's timestamp: it records
     * when <em>this</em> node came to believe it, which is what the suspicion window is measured from.
     * If the live set changes, the new ring is published to {@link Network}.
     *
     * <p>A change is also queued for gossip, here, in the same critical section that stored it. A caller
     * that merged and then enqueued as two steps could be overtaken between them by another thread's
     * stronger update, and the queue would end up holding the weaker one; doing it once, under the lock,
     * means the queue can never disagree with the table about which claim is the latest.
     */
    public boolean merge(Member update) {
        synchronized (lock) {
            Set<String> liveBefore = liveKeys();
            boolean changed = mergeLocked(update, clock.getAsLong());
            if (changed) {
                projectIfLiveSetChanged(liveBefore);
            }
            return changed;
        }
    }

    /** Every member in the table, whatever its state, in the order first heard of. */
    public List<Member> snapshot() {
        synchronized (lock) {
            return new ArrayList<>(members.values());
        }
    }

    /** The nodes that belong on the ring: every member that is {@code ALIVE} or {@code SUSPECT}, this node included. */
    public List<Node> liveNodes() {
        synchronized (lock) {
            return liveNodesLocked();
        }
    }

    /** The record held for {@code name}, or {@code null} if it has never been heard of. */
    public Member get(String name) {
        synchronized (lock) {
            return members.get(name);
        }
    }

    /**
     * Queues an update to be piggybacked on outgoing messages, with a fresh budget of {@code
     * ceil(lambda * log2(N + 1))} sends for {@code N} live members. {@link #merge(Member)} already does
     * this for everything it changes, so this is for updates that did not come through the merge rule.
     *
     * <p>An update for a node that already has one queued replaces it only if it supersedes it by the
     * merge rule (higher incarnation, or equal incarnation and a worse state). A weaker or repeated
     * update is dropped, so a stale write can never push a {@code DEAD} out of the queue.
     */
    public void enqueue(Member update) {
        synchronized (lock) {
            enqueueLocked(update);
        }
    }

    /**
     * Hands out up to {@code max} queued updates, the ones with the most sends left first (the older
     * update wins a tie), and spends one send of each. An update whose sends run out leaves the queue.
     * A {@code max} of zero or less returns nothing and spends nothing.
     */
    public List<Member> drainGossip(int max) {
        synchronized (lock) {
            return drainLocked(max);
        }
    }

    /**
     * The gossip for one outgoing probe or ack, at most {@code max} updates in all: the queued updates
     * with the most sends left, and this node's own {@code ALIVE} at its current incarnation.
     *
     * <p>The own {@code ALIVE} goes out on every message, whatever is queued. It is how a peer that
     * suspected this node hears it is up: told this node is {@code SUSPECT} at incarnation {@code i},
     * this node answers {@code ALIVE} at {@code i + 1}, and the merge rule lets that clear the suspicion.
     * It takes one of the {@code max} slots (so with {@code max == 1} nothing else is sent), and if the
     * queue already holds an update about this node, that one is the slot, refreshed to the current record.
     */
    public List<MemberUpdate> outgoingGossip(int max) {
        synchronized (lock) {
            if (max <= 0) {
                return new ArrayList<>();
            }
            Member self = selfName == null ? null : members.get(selfName);
            boolean announceSelf = self != null && self.getState() == MemberState.ALIVE;
            List<Member> chosen = drainLocked(announceSelf ? max - 1 : max);
            if (announceSelf) {
                boolean queued = false;
                for (int i = 0; i < chosen.size(); i++) {
                    if (chosen.get(i).getNode().getName().equals(selfName)) {
                        chosen.set(i, self);
                        queued = true;
                    }
                }
                if (!queued) {
                    chosen.add(self);
                }
            }
            List<MemberUpdate> wire = new ArrayList<>(chosen.size());
            for (Member member : chosen) {
                wire.add(MemberUpdate.from(member));
            }
            return wire;
        }
    }

    /**
     * The gossip for an answer to a probe from {@code sender}: {@link #outgoingGossip(int)} plus, when
     * this node currently holds the sender as {@code DEAD} or {@code SUSPECT}, that belief itself, whatever
     * is left of its retransmit budget. At most {@code max} updates in all, the belief first in line for a slot.
     *
     * <p>This is what lets a partition re-merge. A node written off while it was cut off is not probed by
     * anyone, and the news that it was written off stops being gossiped after a handful of sends, so it
     * would never learn it. But a node that is written off and still talking is, by definition, one that
     * can reach us: when it does, we tell it. Told it is {@code DEAD} or {@code SUSPECT} at incarnation
     * {@code i}, it refutes with {@code ALIVE} at {@code i + 1} (see {@link #merge(Member)}), which every
     * message of its then carries, and we take it back.
     *
     * <p>{@code LEFT} is deliberately not reported: the sender departed on purpose, and telling a departing
     * node it has left would only make it refute that and undo its own leave. An {@code ALIVE} or unknown
     * sender has no belief to correct.
     */
    public List<MemberUpdate> replyGossip(int max, String sender) {
        synchronized (lock) {
            if (max <= 0) {
                return new ArrayList<>();
            }
            Member belief = (sender == null || sender.equals(selfName)) ? null : members.get(sender);
            boolean correct = belief != null
                    && (belief.getState() == MemberState.DEAD || belief.getState() == MemberState.SUSPECT);
            if (!correct) {
                return outgoingGossip(max);
            }
            List<MemberUpdate> wire = max > 1 ? outgoingGossip(max - 1) : new ArrayList<>();
            wire.removeIf(update -> sender.equals(update.getName())); // the belief below supersedes any older word of it
            wire.add(MemberUpdate.from(belief));
            return wire;
        }
    }

    /**
     * Applies the gossip of one inbound message, update by update, through {@link #merge(Member)}; every
     * update that changed this table is thereby queued to be passed on, and one that changed nothing is
     * not, which is what stops gossip from circulating forever. The whole message is applied as one
     * step, so no outgoing message can carry half of it. {@code null} gossip, and entries that are
     * {@code null} or not {@linkplain MemberUpdate#wellFormed() well formed}, are skipped.
     */
    public void mergeGossip(List<MemberUpdate> gossip) {
        if (gossip == null) {
            return;
        }
        synchronized (lock) {
            for (MemberUpdate update : gossip) {
                if (update != null && update.wellFormed()) {
                    merge(update.toMember());
                }
            }
        }
    }

    /** Suspects {@code name} as of now; see {@link #markSuspect(String, long)}. */
    public void markSuspect(String name) {
        markSuspect(name, clock.getAsLong());
    }

    /**
     * Moves an {@code ALIVE} member to {@code SUSPECT} at its current incarnation (and queues that for
     * gossip, so the suspect itself gets to hear of it and refute), stamping {@code now}
     * as the start of its suspicion window. The stamp matters: {@link #promoteExpiredSuspectsToDead}
     * measures the window from it, so keeping the stamp from when the member was last ALIVE would let a
     * long-healthy node be declared dead on the first sweep, with no time to refute. Anything that is
     * not an {@code ALIVE} peer is left alone: an already-suspect member keeps its original window, a
     * dead or departed one stays so, and this node never suspects itself (that would only trigger its
     * own refutation).
     */
    public void markSuspect(String name, long now) {
        synchronized (lock) {
            Member existing = members.get(name);
            if (existing == null || name.equals(selfName) || existing.getState() != MemberState.ALIVE) {
                return;
            }
            Set<String> liveBefore = liveKeys();
            Member suspected = new Member(existing.getNode(), MemberState.SUSPECT, existing.getIncarnation(), now);
            if (mergeLocked(suspected, now)) {
                projectIfLiveSetChanged(liveBefore);
            }
        }
    }

    /**
     * Declares {@code DEAD}, at their current incarnation, every suspect whose window has run its course:
     * {@code now - lastChange >= suspectTimeoutMs}, with {@code lastChange} the moment it was suspected.
     * A suspect that refuted in time is {@code ALIVE} again by now and is not touched. Each death is
     * queued for gossip.
     */
    public void promoteExpiredSuspectsToDead(long now) {
        synchronized (lock) {
            long timeout = SwimConfig.suspectTimeoutMs();
            Set<String> liveBefore = liveKeys();
            boolean changed = false;
            for (Member member : new ArrayList<>(members.values())) {
                if (member.getState() == MemberState.SUSPECT && now - member.getLastChange() >= timeout) {
                    Member dead = new Member(member.getNode(), MemberState.DEAD, member.getIncarnation(), now);
                    changed |= mergeLocked(dead, now);
                }
            }
            if (changed) {
                projectIfLiveSetChanged(liveBefore);
            }
        }
    }

    /**
     * Applies the merge rule and, if the table changed, queues what it now holds for gossip. Every
     * local change goes through here ({@link #merge}, {@link #markSuspect}, {@link #promoteExpiredSuspectsToDead}),
     * so every one of them is gossiped, under the lock that applied it.
     */
    private boolean mergeLocked(Member update, long now) {
        String name = update.getNode().getName();
        Member existing = members.get(name);

        if (existing != null && !supersedes(update, existing)) {
            return false;
        }

        // For this node's own record, trust the Node it registered with over whatever a peer claims.
        boolean aboutSelf = name.equals(selfName);
        Node node = aboutSelf && existing != null ? existing.getNode() : update.getNode();
        Member record = new Member(node, update.getState(), update.getIncarnation(), now);
        members.put(name, record);

        if (aboutSelf) {
            selfIncarnation = Math.max(selfIncarnation, record.getIncarnation());
            if (record.getState() != MemberState.ALIVE) {
                selfIncarnation++;
                // What gets gossiped is the refutation, not the claim it refutes.
                record = new Member(node, MemberState.ALIVE, selfIncarnation, now);
                members.put(name, record);
            }
        }
        enqueueLocked(record);
        return true;
    }

    /** The merge rule: a higher incarnation wins, and at an equal one the worse state does. */
    private static boolean supersedes(Member update, Member existing) {
        return update.getIncarnation() > existing.getIncarnation()
                || (update.getIncarnation() == existing.getIncarnation()
                && update.getState().precedence() > existing.getState().precedence());
    }

    private void enqueueLocked(Member update) {
        String name = update.getNode().getName();
        Pending queued = pendingGossip.get(name);
        if (queued != null && !supersedes(update, queued.update)) {
            return;
        }
        pendingGossip.remove(name); // re-added at the end: a replacement is the youngest entry
        pendingGossip.put(name, new Pending(update, retransmitBudgetLocked()));
    }

    private List<Member> drainLocked(int max) {
        List<Member> drained = new ArrayList<>();
        if (max <= 0 || pendingGossip.isEmpty()) {
            return drained;
        }
        // The sort is stable and the queue is oldest first, so equal counters go to the older update.
        List<Pending> ranked = new ArrayList<>(pendingGossip.values());
        ranked.sort(Comparator.comparingInt((Pending queued) -> queued.remaining).reversed());
        for (Pending queued : ranked.subList(0, Math.min(max, ranked.size()))) {
            drained.add(queued.update);
            if (--queued.remaining <= 0) {
                pendingGossip.remove(queued.update.getNode().getName());
            }
        }
        return drained;
    }

    /**
     * {@code ceil(lambda * log2(N + 1))} sends for {@code N} live members as of now, never fewer than
     * one. The small epsilon keeps floating-point noise in the logarithm (3.0000000000000004 for an
     * exact 3) from tipping the ceiling to the next whole number.
     */
    private int retransmitBudgetLocked() {
        int live = 0;
        for (Member member : members.values()) {
            if (member.getState().isLive()) {
                live++;
            }
        }
        double log2 = Math.log(live + 1.0) / Math.log(2.0);
        return Math.max(1, (int) Math.ceil(SwimConfig.gossipLambda() * log2 - 1e-9));
    }

    private List<Node> liveNodesLocked() {
        List<Node> live = new ArrayList<>();
        for (Member member : members.values()) {
            if (member.getState().isLive()) {
                live.add(member.getNode());
            }
        }
        return live;
    }

    /** Identity of the live set, port included so a live member that moved still counts as a change. */
    private Set<String> liveKeys() {
        Set<String> keys = new HashSet<>();
        for (Member member : members.values()) {
            if (member.getState().isLive()) {
                keys.add(member.getNode().getName() + ":" + member.getNode().getPort());
            }
        }
        return keys;
    }

    private void projectIfLiveSetChanged(Set<String> liveBefore) {
        if (!liveBefore.equals(liveKeys())) {
            Network.setLiveMembers(liveNodesLocked());
        }
    }

    /** A queued update and the number of sends it has left; only ever touched under {@link #lock}. */
    private static final class Pending {
        final Member update;
        int remaining;

        Pending(Member update, int remaining) {
            this.update = update;
            this.remaining = remaining;
        }
    }
}
