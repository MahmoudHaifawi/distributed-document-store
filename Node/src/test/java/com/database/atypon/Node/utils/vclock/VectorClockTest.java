package com.database.atypon.Node.utils.vclock;

import com.database.atypon.Node.utils.JsonKeys;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class VectorClockTest {

    @Test
    void incrementRaisesOneEntry() {
        VectorClock c = new VectorClock().increment("A").increment("A").increment("B");
        assertThat(c.toJson().getInt("A")).isEqualTo(2);
        assertThat(c.toJson().getInt("B")).isEqualTo(1);
    }

    @Test
    void incrementReturnsNewClockAndLeavesOriginalUntouched() {
        VectorClock base = new VectorClock().increment("A");
        VectorClock next = base.increment("A");
        assertThat(base.toJson().getInt("A")).isEqualTo(1);
        assertThat(next.toJson().getInt("A")).isEqualTo(2);
    }

    @Test
    void mergeIsPointwiseMax() {
        VectorClock a = VectorClock.fromJson(new JSONObject("{\"A\":2,\"B\":1}"));
        VectorClock b = VectorClock.fromJson(new JSONObject("{\"A\":1,\"B\":3,\"C\":1}"));
        JSONObject m = a.merge(b).toJson();
        assertThat(m.getInt("A")).isEqualTo(2);
        assertThat(m.getInt("B")).isEqualTo(3);
        assertThat(m.getInt("C")).isEqualTo(1);
    }

    @Test
    void compareClassifiesOrdering() {
        VectorClock a1 = VectorClock.fromJson(new JSONObject("{\"A\":1}"));
        VectorClock a2 = VectorClock.fromJson(new JSONObject("{\"A\":2}"));
        assertThat(a2.compare(a1)).isEqualTo(VectorClock.Ordering.DESCENDS);
        assertThat(a1.compare(a2)).isEqualTo(VectorClock.Ordering.DESCENDED_BY);
        assertThat(a1.compare(a1)).isEqualTo(VectorClock.Ordering.EQUAL);
        VectorClock x = VectorClock.fromJson(new JSONObject("{\"A\":1,\"B\":1}"));
        VectorClock y = VectorClock.fromJson(new JSONObject("{\"A\":2}"));   // A higher, B lower -> neither dominates
        assertThat(x.compare(y)).isEqualTo(VectorClock.Ordering.CONCURRENT);
    }

    @Test
    void missingEntryCountsAsZero() {
        VectorClock empty = new VectorClock();
        VectorClock a1 = new VectorClock().increment("A");
        assertThat(a1.compare(empty)).isEqualTo(VectorClock.Ordering.DESCENDS);
        assertThat(empty.compare(a1)).isEqualTo(VectorClock.Ordering.DESCENDED_BY);
        assertThat(empty.compare(new VectorClock())).isEqualTo(VectorClock.Ordering.EQUAL);
    }

    @Test
    void fromJsonToJsonRoundTrips() {
        VectorClock empty = VectorClock.fromJson(null);
        assertThat(empty.toJson().isEmpty()).isTrue();
        assertThat(VectorClock.fromJson(empty.toJson()).compare(empty)).isEqualTo(VectorClock.Ordering.EQUAL);

        JSONObject src = new JSONObject("{\"A\":3,\"B\":1,\"C\":7}");
        JSONObject out = VectorClock.fromJson(src).toJson();
        assertThat(out.similar(src)).isTrue();
        assertThat(out.toMap()).isEqualTo(src.toMap());
    }

    @Test
    void canonicalIsSortedAndDeterministic() {
        Map<String, Integer> forward = new LinkedHashMap<>();
        forward.put("A", 1);
        forward.put("B", 2);
        forward.put("C", 3);
        Map<String, Integer> reverse = new LinkedHashMap<>();
        reverse.put("C", 3);
        reverse.put("B", 2);
        reverse.put("A", 1);

        String f = new VectorClock(forward).canonical();
        String r = new VectorClock(reverse).canonical();
        assertThat(f).isEqualTo(r);
        assertThat(f.indexOf("A")).isLessThan(f.indexOf("B"));
        assertThat(f.indexOf("B")).isLessThan(f.indexOf("C"));
    }

    @Test
    void reservedKeysAreDefined() {
        assertThat(JsonKeys.VCLOCK).isEqualTo("_vclock");
        assertThat(JsonKeys.TS).isEqualTo("_ts");
        assertThat(JsonKeys.DELETED).isEqualTo("_deleted");
    }
}
