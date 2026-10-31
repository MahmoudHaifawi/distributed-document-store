package com.database.atypon.Node.utils.swim;

import com.database.atypon.Node.model.Network;
import com.database.atypon.Node.model.Node;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
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
     * Updates waiting to be piggybacked on outgoing messages, keyed by node name (the newest wins).
     * This is only the store; retransmit counting is layered on by the gossip task.
     */
    private final Map<String, Member> pendingGossip = new LinkedHashMap<>();

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

    /** Queues an update to be piggybacked on outgoing messages; a newer one for the same node replaces it. */
    public void enqueue(Member update) {
        synchronized (lock) {
            enqueueLocked(update);
        }
    }

    /** Removes and returns up to {@code max} queued updates, oldest first. */
    public List<Member> drainGossip(int max) {
        synchronized (lock) {
            List<Member> drained = new ArrayList<>();
            Iterator<Member> pending = pendingGossip.values().iterator();
            while (drained.size() < max && pending.hasNext()) {
                drained.add(pending.next());
                pending.remove();
            }
            return drained;
        }
    }

    /** Suspects {@code name} as of now; see {@link #markSuspect(String, long)}. */
    public void markSuspect(String name) {
        markSuspect(name, clock.getAsLong());
    }

    /**
     * Moves an {@code ALIVE} member to {@code SUSPECT} at its current incarnation, stamping {@code now}
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
     * A suspect that refuted in time is {@code ALIVE} again by now and is not touched.
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

    private boolean mergeLocked(Member update, long now) {
        String name = update.getNode().getName();
        Member existing = members.get(name);

        boolean supersedes = existing == null
                || update.getIncarnation() > existing.getIncarnation()
                || (update.getIncarnation() == existing.getIncarnation()
                && update.getState().precedence() > existing.getState().precedence());
        if (!supersedes) {
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
                Member refutation = new Member(node, MemberState.ALIVE, selfIncarnation, now);
                members.put(name, refutation);
                // Only this table knows the refutation exists, so only it can queue it for the cluster.
                enqueueLocked(refutation);
            }
        }
        return true;
    }

    private void enqueueLocked(Member update) {
        pendingGossip.put(update.getNode().getName(), update);
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
}
