package com.database.atypon.Node.utils.vclock;

import org.json.JSONObject;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Immutable vector clock: a map from node id to a per-node event counter. A node id absent from
 * the map counts as 0. Every mutating-looking operation ({@link #increment}, {@link #merge})
 * returns a new clock and leaves the receiver untouched.
 *
 * <p>Entries are held in a {@link TreeMap}, so {@link #toJson()} and {@link #canonical()} are
 * naturally ordered by node id and deterministic regardless of insertion order.
 */
public final class VectorClock {

    /** Result of comparing {@code this} against another clock. */
    public enum Ordering {
        /** Both clocks are identical. */
        EQUAL,
        /** {@code this} strictly dominates {@code other}: it has seen everything {@code other} has, and more. */
        DESCENDS,
        /** {@code other} strictly dominates {@code this}. */
        DESCENDED_BY,
        /** Neither clock dominates the other: each has seen events the other has not. */
        CONCURRENT
    }

    private final TreeMap<String, Integer> entries;

    /** Creates an empty clock. */
    public VectorClock() {
        this.entries = new TreeMap<>();
    }

    /** Creates a clock holding a defensive copy of {@code entries} (a null map is treated as empty). */
    public VectorClock(Map<String, Integer> entries) {
        this.entries = entries == null ? new TreeMap<>() : new TreeMap<>(entries);
    }

    /**
     * Parses a clock from a JSON object of {@code {"nodeId": counter, ...}}. A null object (absent
     * {@code _vclock}) yields an empty clock.
     */
    public static VectorClock fromJson(JSONObject vclockOrNull) {
        VectorClock clock = new VectorClock();
        if (vclockOrNull == null) {
            return clock;
        }
        for (String node : vclockOrNull.keySet()) {
            clock.entries.put(node, vclockOrNull.getInt(node));
        }
        return clock;
    }

    /** Serialises to a JSON object of {@code {"nodeId": counter, ...}}; empty clock gives {@code {}}. */
    public JSONObject toJson() {
        JSONObject json = new JSONObject();
        entries.forEach(json::put);
        return json;
    }

    /** Returns a new clock identical to this one except {@code node}'s counter is one higher. */
    public VectorClock increment(String node) {
        Objects.requireNonNull(node, "node");
        VectorClock next = new VectorClock(entries);
        next.entries.merge(node, 1, Integer::sum);
        return next;
    }

    /** Returns a new clock holding the pointwise maximum of this clock and {@code other}. */
    public VectorClock merge(VectorClock other) {
        Objects.requireNonNull(other, "other");
        VectorClock merged = new VectorClock(entries);
        other.entries.forEach((node, count) -> merged.entries.merge(node, count, Math::max));
        return merged;
    }

    /**
     * Compares this clock against {@code other} over the union of their node ids (missing = 0).
     * {@link Ordering#DESCENDS} means this strictly dominates other; {@link Ordering#DESCENDED_BY}
     * is the reverse; {@link Ordering#CONCURRENT} means neither dominates.
     */
    public Ordering compare(VectorClock other) {
        Objects.requireNonNull(other, "other");
        Set<String> nodes = new TreeSet<>(entries.keySet());
        nodes.addAll(other.entries.keySet());

        boolean thisAhead = false;
        boolean otherAhead = false;
        for (String node : nodes) {
            int mine = entries.getOrDefault(node, 0);
            int theirs = other.entries.getOrDefault(node, 0);
            if (mine > theirs) {
                thisAhead = true;
            } else if (theirs > mine) {
                otherAhead = true;
            }
        }

        if (thisAhead && otherAhead) {
            return Ordering.CONCURRENT;
        }
        if (thisAhead) {
            return Ordering.DESCENDS;
        }
        if (otherAhead) {
            return Ordering.DESCENDED_BY;
        }
        return Ordering.EQUAL;
    }

    /**
     * Deterministic string form, entries sorted by node id (e.g. {@code A=1,B=2}). Used as the
     * tiebreak key when two clocks are concurrent and a stable winner must be chosen.
     */
    public String canonical() {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, Integer> e : entries.entrySet()) {
            if (sb.length() > 0) {
                sb.append(',');
            }
            sb.append(e.getKey()).append('=').append(e.getValue());
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        return "VectorClock{" + canonical() + "}";
    }
}
