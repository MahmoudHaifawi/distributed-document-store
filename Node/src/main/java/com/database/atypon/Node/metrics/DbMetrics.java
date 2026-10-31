package com.database.atypon.Node.metrics;

import com.database.atypon.Node.utils.vclock.VectorClock;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.DoubleSupplier;
import java.util.function.ToDoubleFunction;

/**
 * The domain meters that tell the write / read / consistency story, recorded where each operation completes.
 *
 * <p>Two rules keep this off the critical path of a request. Every update goes through {@link #safe}, which
 * swallows anything a meter throws, so a metrics failure can never break or slow a request; and an update is
 * made only once the operation's result is known, never ahead of it. There is no locking here: a meter is
 * looked up in the registry's lock-free map and bumped atomically.
 *
 * <p>The names and tags below are a contract: the Grafana dashboard queries them verbatim. {@code role} and
 * {@code node} are reserved for the common tags the application stamps on every meter, so no meter here uses
 * those keys.
 *
 * <p>Most meters are counters bumped where an operation completes. The membership ones are different in kind: the
 * ring size and the per-state member counts are gauges bound once at startup ({@link #bindMembership}) to a supplier
 * that reads the live state when the registry is scraped, so tracking them costs the membership, probe and request
 * paths nothing.
 *
 * <p>The registry is injected optionally. This module's own tests boot without one (the real registry is wired
 * in the application module), so when none is present the meters go to a private {@link SimpleMeterRegistry}
 * instead: they still record, nothing scrapes them.
 */
@Component
public class DbMetrics {

    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(DbMetrics.class);

    /** Set once the first meter failure has been reported, so a registry that stays broken logs once, not per request. */
    private static final AtomicBoolean FAILURE_REPORTED = new AtomicBoolean();

    /** Client writes this node completed: {@code result}, {@code type}, {@code origin}. */
    public static final String WRITE_TOTAL = "db_write_total";
    /** Replicas a write was copied to (the replication fan-out), one sample per write this node originated. */
    public static final String WRITE_REPLICAS = "db_write_replicas";
    /** Client reads this node served: {@code path}. */
    public static final String READ_TOTAL = "db_read_total";
    /** Read-repairs triggered because a read found a replica whose copy differs from the winner. */
    public static final String READ_REPAIR_TOTAL = "db_read_repair_total";
    /** Replicas a read-repair brought up to date. */
    public static final String READ_REPAIR_DOCS_TOTAL = "db_read_repair_docs_total";
    /**
     * Replicated or repaired copies reconciled against a different local copy: {@code outcome}.
     *
     * <p><b>Read {@code outcome} before alerting on this.</b> {@code outcome=descends} ({@link ConflictOutcome#DESCENDS})
     * is NORMAL replication traffic, not a conflict: every update or delete a client makes fires it about N-1 times
     * (once on each replica that applies the dominating copy over the older one it holds). Only
     * {@code outcome=concurrent} ({@link ConflictOutcome#CONCURRENT}) is a true conflict, two writes that neither
     * follows the other.
     */
    public static final String RECONCILE_CONFLICTS_TOTAL = "db_reconcile_conflicts_total";
    /** Tombstones stored on this node. */
    public static final String TOMBSTONE_WRITTEN_TOTAL = "db_tombstone_written_total";
    /** Gauge: physical nodes currently on the ring, as this node sees it. */
    public static final String RING_SIZE = "db_ring_size";
    /** Gauge: members this node holds in each SWIM state: {@code state}. */
    public static final String SWIM_MEMBERS = "db_swim_members";
    /** Probes this node's failure detector issued: {@code kind}. */
    public static final String SWIM_PROBES_TOTAL = "db_swim_probes_total";
    /** Messages that carried membership gossip, out of or into this node: {@code direction}. */
    public static final String SWIM_GOSSIP_TOTAL = "db_swim_gossip_total";
    /** State handoff pulls of a schema snapshot from a peer. */
    public static final String HANDOFF_PULLS_TOTAL = "db_handoff_pulls_total";
    /** Documents state handoff applied here (changed this node's copy). */
    public static final String HANDOFF_DOCS_TOTAL = "db_handoff_docs_total";

    /** The {@code result} tag of {@link #WRITE_TOTAL}: whether the write reached its write quorum. */
    public enum WriteResult {
        SUCCESS, QUORUM_FAILED
    }

    /** The {@code type} tag of {@link #WRITE_TOTAL}. */
    public enum WriteType {
        CREATE, UPDATE, DELETE
    }

    /** The {@code origin} tag of {@link #WRITE_TOTAL}: applied here as the primary, or handed on to the primary. */
    public enum WriteOrigin {
        LOCAL, FORWARDED
    }

    /** The {@code path} tag of {@link #READ_TOTAL}. */
    public enum ReadPath {
        BY_ID, ALL, INDEX
    }

    /** The {@code outcome} tag of {@link #RECONCILE_CONFLICTS_TOTAL}. */
    public enum ConflictOutcome {
        /**
         * One copy causally follows the other, so the older one is simply stale. This is NORMAL replication
         * traffic, NOT a real conflict: it fires about N-1 times per client update or delete, as each replica applies
         * the dominating copy over its older one. Only {@link #CONCURRENT} is a true conflict.
         */
        DESCENDS,
        /** Neither copy follows the other: a true conflict, settled by timestamp. */
        CONCURRENT
    }

    /** The {@code state} tag of {@link #SWIM_MEMBERS}: the states a member is counted in (a departed one is in none). */
    public enum SwimState {
        ALIVE, SUSPECT, DEAD
    }

    /** The {@code kind} tag of {@link #SWIM_PROBES_TOTAL}. */
    public enum ProbeKind {
        /** A ping sent straight to the member being probed. */
        DIRECT,
        /** A ping-req asking another member to ping the one being probed (one per helper asked). */
        INDIRECT
    }

    /** The {@code direction} tag of {@link #SWIM_GOSSIP_TOTAL}. */
    public enum GossipDirection {
        /** A message leaving this node with membership gossip attached. */
        SENT,
        /** A message arriving with membership gossip that was merged. */
        RECEIVED
    }

    private final MeterRegistry registry;

    /** What Spring uses: the application's registry when there is one, else a private fallback. */
    @Autowired
    public DbMetrics(ObjectProvider<MeterRegistry> registry) {
        this(resolve(registry));
    }

    /** Records into {@code registry}. */
    public DbMetrics(MeterRegistry registry) {
        this.registry = registry;
        safe(this::registerAll);
    }

    /** Records into a private registry nobody scrapes, for callers that do not care about metrics. */
    public static DbMetrics detached() {
        return new DbMetrics(new SimpleMeterRegistry());
    }

    /** The registry the meters land in (the fallback one when the context supplied none). */
    public MeterRegistry registry() {
        return registry;
    }

    // ---- writes --------------------------------------------------------------------------------------------

    /** A client write this node completed, with the quorum verdict it reached (or was handed back). */
    public void recordWrite(WriteResult result, WriteType type, WriteOrigin origin) {
        safe(() -> writeCounter(result, type, origin).increment());
    }

    /** How many replicas a write this node originated was copied to; 0 on a lone node. */
    public void recordWriteFanout(int replicas) {
        safe(() -> writeFanout().record(replicas));
    }

    // ---- reads ---------------------------------------------------------------------------------------------

    /** A client read this node served. */
    public void recordRead(ReadPath path) {
        safe(() -> readCounter(path).increment());
    }

    /**
     * A read found replicas to repair and tried to. The repair is counted once, however many replicas it
     * touched, and {@code replicasRepaired} is how many of them took it.
     */
    public void recordReadRepair(int replicasRepaired) {
        safe(() -> {
            readRepairCounter().increment();
            if (replicasRepaired > 0) {
                readRepairDocsCounter().increment(replicasRepaired);
            }
        });
    }

    // ---- consistency ---------------------------------------------------------------------------------------

    /**
     * Two different copies of a document were reconciled; {@code relation} is how the stored copy and the
     * incoming one relate causally. Identical copies, or a missing one, are not a conflict and count for nothing.
     */
    public void recordReconcile(VectorClock.Ordering relation) {
        ConflictOutcome outcome = outcomeOf(relation);
        if (outcome != null) {
            safe(() -> reconcileCounter(outcome).increment());
        }
    }

    /** A tombstone was stored on this node. */
    public void recordTombstoneWritten() {
        safe(() -> tombstoneCounter().increment());
    }

    // ---- membership ----------------------------------------------------------------------------------------

    /**
     * Binds the membership gauges, once, at startup: {@link #RING_SIZE} to {@code ringSize}, and {@link #SWIM_MEMBERS}
     * for each {@link SwimState} to {@code membersIn}. Neither is a stored value: the registry calls the supplier
     * when it is scraped, so the state is read at that moment and nothing is recomputed as membership changes.
     *
     * <p>The suppliers are held by a strong reference, because nothing else holds them: a registry keeps a gauge's
     * source weakly by default, and a collected one would read as NaN. A supplier that throws reads as NaN for that
     * scrape. Binding is a no-throw call like every other update here, and binding a gauge that is already
     * registered keeps the first, so a second bind to one registry changes nothing.
     */
    public void bindMembership(DoubleSupplier ringSize, ToDoubleFunction<SwimState> membersIn) {
        safe(() -> {
            Gauge.builder(RING_SIZE, ringSize, DoubleSupplier::getAsDouble)
                    .description("Physical nodes currently on the ring, as this node sees it")
                    .strongReference(true)
                    .register(registry);
            for (SwimState state : SwimState.values()) {
                Gauge.builder(SWIM_MEMBERS, state, membersIn::applyAsDouble)
                        .description("Members this node holds in each SWIM state")
                        .tag("state", tag(state))
                        .strongReference(true)
                        .register(registry);
            }
        });
    }

    /**
     * The failure detector issued a probe: a ping to a member, or (indirect) a ping-req asking another member to ping
     * it. Counted when issued, whether or not it is answered.
     */
    public void recordSwimProbe(ProbeKind kind) {
        safe(() -> probeCounter(kind).increment());
    }

    /**
     * One message carried membership gossip: out of this node with gossip attached, or into it with gossip that was
     * merged. Counted per message, not per update inside it; a message with no gossip is not counted.
     */
    public void recordSwimGossip(GossipDirection direction) {
        safe(() -> gossipCounter(direction).increment());
    }

    // ---- handoff -------------------------------------------------------------------------------------------

    /** State handoff pulled a schema snapshot from a peer. */
    public void recordHandoffPull() {
        safe(() -> handoffPullCounter().increment());
    }

    /** State handoff applied {@code documents} documents of a pulled snapshot here (those that changed this node's copy). */
    public void recordHandoffDocs(int documents) {
        if (documents > 0) {
            safe(() -> handoffDocsCounter().increment(documents));
        }
    }

    // ---- plumbing ------------------------------------------------------------------------------------------

    /**
     * Runs a meter update, swallowing anything it throws. A metrics failure must never break or slow a
     * request, so no update calls the registry outside this. The first failure is logged at WARN, later ones
     * at DEBUG.
     */
    public static void safe(Runnable update) {
        try {
            update.run();
        } catch (Throwable failure) {
            if (FAILURE_REPORTED.compareAndSet(false, true)) {
                log.warn("a metrics update failed and was ignored; further failures are logged at DEBUG", failure);
            } else {
                log.debug("a metrics update failed and was ignored", failure);
            }
        }
    }

    private static ConflictOutcome outcomeOf(VectorClock.Ordering relation) {
        if (relation == null) {
            return null;
        }
        return switch (relation) {
            case EQUAL -> null;
            case DESCENDS, DESCENDED_BY -> ConflictOutcome.DESCENDS;
            case CONCURRENT -> ConflictOutcome.CONCURRENT;
        };
    }

    private static MeterRegistry resolve(ObjectProvider<MeterRegistry> provider) {
        try {
            MeterRegistry registry = provider.getIfUnique();
            if (registry != null) {
                return registry;
            }
        } catch (Throwable failure) {
            log.warn("could not resolve the meter registry; metrics go to a private fallback", failure);
        }
        return new SimpleMeterRegistry();
    }

    private Counter writeCounter(WriteResult result, WriteType type, WriteOrigin origin) {
        return Counter.builder(WRITE_TOTAL)
                .description("Client writes completed on this node, by quorum result, type and origin")
                .tag("result", tag(result)).tag("type", tag(type)).tag("origin", tag(origin))
                .register(registry);
    }

    private DistributionSummary writeFanout() {
        return DistributionSummary.builder(WRITE_REPLICAS)
                .description("Replicas each write originated here was copied to")
                .register(registry);
    }

    private Counter readCounter(ReadPath path) {
        return Counter.builder(READ_TOTAL)
                .description("Client reads served by this node, by read path")
                .tag("path", tag(path))
                .register(registry);
    }

    private Counter reconcileCounter(ConflictOutcome outcome) {
        return Counter.builder(RECONCILE_CONFLICTS_TOTAL)
                .description("Replicated or repaired copies reconciled against a different local copy, by outcome")
                .tag("outcome", tag(outcome))
                .register(registry);
    }

    private Counter readRepairCounter() {
        return Counter.builder(READ_REPAIR_TOTAL)
                .description("Read-repairs triggered by a read that found a replica differing from the winner")
                .register(registry);
    }

    private Counter readRepairDocsCounter() {
        return Counter.builder(READ_REPAIR_DOCS_TOTAL)
                .description("Replicas a read-repair brought up to date")
                .register(registry);
    }

    private Counter tombstoneCounter() {
        return Counter.builder(TOMBSTONE_WRITTEN_TOTAL)
                .description("Tombstones stored on this node")
                .register(registry);
    }

    private Counter probeCounter(ProbeKind kind) {
        return Counter.builder(SWIM_PROBES_TOTAL)
                .description("Probes this node's failure detector issued, by kind (direct ping or indirect ping-req)")
                .tag("kind", tag(kind))
                .register(registry);
    }

    private Counter gossipCounter(GossipDirection direction) {
        return Counter.builder(SWIM_GOSSIP_TOTAL)
                .description("Messages that carried membership gossip, by direction")
                .tag("direction", tag(direction))
                .register(registry);
    }

    private Counter handoffPullCounter() {
        return Counter.builder(HANDOFF_PULLS_TOTAL)
                .description("State handoff pulls of a schema snapshot from a peer")
                .register(registry);
    }

    private Counter handoffDocsCounter() {
        return Counter.builder(HANDOFF_DOCS_TOTAL)
                .description("Documents state handoff applied here")
                .register(registry);
    }

    /**
     * Registers every meter at zero, so Prometheus sees each series from startup rather than from its first
     * event (a counter that first appears at 1 has no rate for that increment).
     */
    private void registerAll() {
        for (WriteResult result : WriteResult.values()) {
            for (WriteType type : WriteType.values()) {
                for (WriteOrigin origin : WriteOrigin.values()) {
                    writeCounter(result, type, origin);
                }
            }
        }
        writeFanout();
        for (ReadPath path : ReadPath.values()) {
            readCounter(path);
        }
        for (ConflictOutcome outcome : ConflictOutcome.values()) {
            reconcileCounter(outcome);
        }
        readRepairCounter();
        readRepairDocsCounter();
        tombstoneCounter();
        for (ProbeKind kind : ProbeKind.values()) {
            probeCounter(kind);
        }
        for (GossipDirection direction : GossipDirection.values()) {
            gossipCounter(direction);
        }
        handoffPullCounter();
        handoffDocsCounter();
    }

    /** The tag value for an enum constant: its name, lower-case ({@code QUORUM_FAILED} becomes {@code quorum_failed}). */
    private static String tag(Enum<?> value) {
        return value.name().toLowerCase(Locale.ROOT);
    }
}
