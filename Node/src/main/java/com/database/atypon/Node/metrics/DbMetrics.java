package com.database.atypon.Node.metrics;

import com.database.atypon.Node.utils.vclock.VectorClock;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;

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
    /** Replicated or repaired copies reconciled against a different local copy: {@code outcome}. */
    public static final String RECONCILE_CONFLICTS_TOTAL = "db_reconcile_conflicts_total";
    /** Tombstones stored on this node. */
    public static final String TOMBSTONE_WRITTEN_TOTAL = "db_tombstone_written_total";

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
        /** One copy causally follows the other, so the older one is simply stale. */
        DESCENDS,
        /** Neither copy follows the other: a true conflict, settled by timestamp. */
        CONCURRENT
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
    }

    /** The tag value for an enum constant: its name, lower-case ({@code QUORUM_FAILED} becomes {@code quorum_failed}). */
    private static String tag(Enum<?> value) {
        return value.name().toLowerCase(Locale.ROOT);
    }
}
