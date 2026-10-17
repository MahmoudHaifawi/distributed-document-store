package com.database.atypon.Node.utils.vclock;

import com.database.atypon.Node.utils.JsonKeys;
import org.json.JSONObject;

/**
 * Pure, stateless causal reconciliation of two versions of the same document, used by replication
 * and read-repair. No I/O and no shared state: given two documents it deterministically picks the
 * one that should survive.
 *
 * <p>A tombstone ({@code _deleted:true}) is an ordinary participant. It is classified by its vector
 * clock like any live document, so a delete only wins over a version it causally descends, and a
 * later re-create that descends the tombstone resurrects the document.
 */
public final class Reconcile {

    private Reconcile() {
    }

    /** True when {@code doc} is a delete marker ({@code _deleted:true}); a null document is not. */
    public static boolean isTombstone(JSONObject doc) {
        return doc != null && doc.optBoolean(JsonKeys.DELETED, false);
    }

    /**
     * Picks the surviving version of {@code a} and {@code b}.
     *
     * <ul>
     *   <li>A null operand yields the other operand unchanged.</li>
     *   <li>Clocks {@code EQUAL} or {@code a} descends {@code b}: {@code a} wins.</li>
     *   <li>{@code b} descends {@code a}: {@code b} wins, even with a lower {@code _ts}.</li>
     *   <li>{@code CONCURRENT}: the higher {@code _ts} wins; on a tie, the document whose clock has
     *       the lexicographically greater {@link VectorClock#canonical()} wins, so the choice does
     *       not depend on argument order.</li>
     * </ul>
     *
     * <p>When both operands are non-null the result is a copy of the winner whose {@code _vclock}
     * is the pointwise merge of both clocks, so it dominates (or equals) each input. Neither input
     * is modified.
     */
    public static JSONObject winner(JSONObject a, JSONObject b) {
        if (a == null) {
            return b;
        }
        if (b == null) {
            return a;
        }

        VectorClock clockA = VectorClock.fromJson(a.optJSONObject(JsonKeys.VCLOCK));
        VectorClock clockB = VectorClock.fromJson(b.optJSONObject(JsonKeys.VCLOCK));

        JSONObject chosen;
        switch (clockA.compare(clockB)) {
            case EQUAL:
            case DESCENDS:
                chosen = a;
                break;
            case DESCENDED_BY:
                chosen = b;
                break;
            default: // CONCURRENT
                chosen = concurrentWinner(a, clockA, b, clockB);
                break;
        }

        // Snapshot the chosen document so stamping the merged clock never touches a caller's object.
        JSONObject result = new JSONObject(chosen.toString());
        result.put(JsonKeys.VCLOCK, clockA.merge(clockB).toJson());
        return result;
    }

    private static JSONObject concurrentWinner(JSONObject a, VectorClock clockA, JSONObject b, VectorClock clockB) {
        long tsA = a.optLong(JsonKeys.TS, 0);
        long tsB = b.optLong(JsonKeys.TS, 0);
        if (tsA != tsB) {
            return tsA > tsB ? a : b;
        }
        return clockA.canonical().compareTo(clockB.canonical()) >= 0 ? a : b;
    }
}
