package com.database.atypon.Node.utils.vclock;

import org.json.JSONObject;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ReconcileTest {

    private static JSONObject doc(String json) {
        return new JSONObject(json);
    }

    @Test
    void descendsKeepsNewer() {
        JSONObject a = doc("{\"_vclock\":{\"A\":1},\"_ts\":10,\"n\":1}");
        JSONObject b = doc("{\"_vclock\":{\"A\":2},\"_ts\":5,\"n\":2}");   // b descends a, even with lower _ts
        assertThat(Reconcile.winner(a, b).getInt("n")).isEqualTo(2);
    }

    @Test
    void descendsKeepsNewerRegardlessOfArgumentOrder() {
        JSONObject a = doc("{\"_vclock\":{\"A\":2},\"_ts\":5,\"n\":2}");
        JSONObject b = doc("{\"_vclock\":{\"A\":1},\"_ts\":10,\"n\":1}");
        assertThat(Reconcile.winner(a, b).getInt("n")).isEqualTo(2);
    }

    @Test
    void concurrentResolvesByTimestampThenCanonical() {
        JSONObject a = doc("{\"_vclock\":{\"A\":1},\"_ts\":10,\"n\":1}");
        JSONObject b = doc("{\"_vclock\":{\"B\":1},\"_ts\":20,\"n\":2}");  // concurrent; b later
        JSONObject w = Reconcile.winner(a, b);
        assertThat(w.getInt("n")).isEqualTo(2);
        assertThat(w.getJSONObject("_vclock").getInt("A")).isEqualTo(1);   // merged clock
        assertThat(w.getJSONObject("_vclock").getInt("B")).isEqualTo(1);
    }

    @Test
    void concurrentWithEqualTimestampBreaksTieByGreaterCanonical() {
        JSONObject a = doc("{\"_vclock\":{\"A\":1},\"_ts\":10,\"n\":1}");   // canonical "A=1"
        JSONObject b = doc("{\"_vclock\":{\"B\":1},\"_ts\":10,\"n\":2}");   // canonical "B=1" > "A=1"
        assertThat(Reconcile.winner(a, b).getInt("n")).isEqualTo(2);
        assertThat(Reconcile.winner(b, a).getInt("n")).isEqualTo(2);        // symmetric / deterministic
    }

    @Test
    void concurrentWinnerCarriesMergedClockEvenWhenFirstOperandWins() {
        JSONObject a = doc("{\"_vclock\":{\"A\":1},\"_ts\":20,\"n\":1}");
        JSONObject b = doc("{\"_vclock\":{\"B\":3},\"_ts\":10,\"n\":2}");
        JSONObject w = Reconcile.winner(a, b);
        assertThat(w.getInt("n")).isEqualTo(1);
        assertThat(w.getJSONObject("_vclock").getInt("A")).isEqualTo(1);
        assertThat(w.getJSONObject("_vclock").getInt("B")).isEqualTo(3);
    }

    @Test
    void tombstoneWinsOrLosesByClock() {
        JSONObject live = doc("{\"_vclock\":{\"A\":1},\"_ts\":10,\"n\":1}");
        JSONObject tomb = doc("{\"_vclock\":{\"A\":2},\"_ts\":11,\"_deleted\":true}");  // tomb descends live
        assertThat(Reconcile.isTombstone(Reconcile.winner(live, tomb))).isTrue();
        assertThat(Reconcile.isTombstone(Reconcile.winner(tomb, live))).isTrue();
    }

    @Test
    void liveDescendingTombstoneResurrects() {
        JSONObject tomb = doc("{\"_vclock\":{\"A\":1},\"_ts\":99,\"_deleted\":true}");
        JSONObject live = doc("{\"_vclock\":{\"A\":2},\"_ts\":1,\"n\":7}");   // re-create descends the delete
        JSONObject w = Reconcile.winner(tomb, live);
        assertThat(Reconcile.isTombstone(w)).isFalse();
        assertThat(w.getInt("n")).isEqualTo(7);
    }

    @Test
    void equalClocksKeepFirstOperand() {
        JSONObject a = doc("{\"_vclock\":{\"A\":1},\"_ts\":10,\"n\":1}");
        JSONObject b = doc("{\"_vclock\":{\"A\":1},\"_ts\":99,\"n\":2}");
        assertThat(Reconcile.winner(a, b).getInt("n")).isEqualTo(1);
    }

    @Test
    void nullOperandReturnsTheOther() {
        assertThat(Reconcile.winner(null, doc("{\"_vclock\":{\"A\":1},\"n\":1}")).getInt("n")).isEqualTo(1);
        assertThat(Reconcile.winner(doc("{\"_vclock\":{\"A\":1},\"n\":1}"), null).getInt("n")).isEqualTo(1);
        assertThat(Reconcile.winner(null, null)).isNull();
    }

    @Test
    void winnerDoesNotMutateEitherInput() {
        JSONObject a = doc("{\"_vclock\":{\"A\":1},\"_ts\":10,\"n\":1}");
        JSONObject b = doc("{\"_vclock\":{\"B\":1},\"_ts\":20,\"n\":2}");
        Reconcile.winner(a, b);
        assertThat(a.similar(doc("{\"_vclock\":{\"A\":1},\"_ts\":10,\"n\":1}"))).isTrue();
        assertThat(b.similar(doc("{\"_vclock\":{\"B\":1},\"_ts\":20,\"n\":2}"))).isTrue();
    }

    @Test
    void missingClocksAreTreatedAsEmpty() {
        JSONObject legacy = doc("{\"n\":1}");                                     // pre-M8 doc, no _vclock
        JSONObject stamped = doc("{\"_vclock\":{\"A\":1},\"_ts\":5,\"n\":2}");
        assertThat(Reconcile.winner(legacy, stamped).getInt("n")).isEqualTo(2);   // stamped descends empty
        assertThat(Reconcile.winner(stamped, legacy).getInt("n")).isEqualTo(2);
    }

    @Test
    void isTombstoneIsNullSafeAndFlagDriven() {
        assertThat(Reconcile.isTombstone(null)).isFalse();
        assertThat(Reconcile.isTombstone(doc("{\"n\":1}"))).isFalse();
        assertThat(Reconcile.isTombstone(doc("{\"_deleted\":false}"))).isFalse();
        assertThat(Reconcile.isTombstone(doc("{\"_deleted\":true}"))).isTrue();
    }
}
