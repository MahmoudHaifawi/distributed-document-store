package com.database.atypon.Node.services.read;

import com.database.atypon.Node.services.read.QuorumReadService.Copy;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The read path folds {@code Reconcile.winner} across the replicas' copies. That fold is not commutative
 * when three or more copies are mutually concurrent, so it must run in a stable order (by source node)
 * or two replicas gathering the same copies in a different order would repair to different winners.
 */
class QuorumReadServiceTest {

    // Pairwise concurrent: each clock has an entry the other two lack and lacks one they have, yet the merge
    // of any two covers the third. So a left fold lets whichever two copies come first decide the winner.
    private static final Copy A = copy("Node1", 10, 1_000, "{\"X\":1,\"Y\":1}");
    private static final Copy B = copy("Node2", 20, 2_000, "{\"Y\":1,\"Z\":1}");
    private static final Copy C = copy("Node3", 30, 3_000, "{\"X\":1,\"Z\":1}");

    @Test
    void theWinnerOfMutuallyConcurrentCopiesDoesNotDependOnTheOrderTheyWereGathered() {
        List<List<Copy>> orders = List.of(
                List.of(A, B, C), List.of(A, C, B),
                List.of(B, A, C), List.of(B, C, A),
                List.of(C, A, B), List.of(C, B, A));

        JSONObject first = QuorumReadService.winnerOf(orders.get(0));
        for (List<Copy> order : orders) {
            JSONObject winner = QuorumReadService.winnerOf(order);
            assertThat(winner.similar(first))
                    .as("winner for %s: %s, expected %s", order.stream().map(Copy::node).toList(), winner, first)
                    .isTrue();
        }
    }

    @Test
    void theWinnerIsTheLeftFoldOverTheCopiesOrderedByNodeName() {
        // by node name the fold is A, B, C: B beats A on timestamp, and C is then covered by the merged clock
        JSONObject winner = QuorumReadService.winnerOf(List.of(C, B, A));

        assertThat(winner.getInt("n")).isEqualTo(20);
        JSONObject clock = winner.getJSONObject("_vclock");
        assertThat(clock.getInt("X")).isEqualTo(1);
        assertThat(clock.getInt("Y")).isEqualTo(1);
        assertThat(clock.getInt("Z")).isEqualTo(1);
    }

    @Test
    void absentCopiesNeverWinAndTheInputOrderIsLeftAlone() {
        List<Copy> gathered = new ArrayList<>(List.of(new Copy("Node9", null), C, new Copy("Node0", null)));

        JSONObject winner = QuorumReadService.winnerOf(gathered);

        assertThat(winner.getInt("n")).isEqualTo(30);
        assertThat(gathered).extracting(Copy::node).containsExactly("Node9", "Node3", "Node0");
        assertThat(QuorumReadService.winnerOf(List.of(new Copy("Node1", null)))).isNull();
        assertThat(QuorumReadService.winnerOf(List.of())).isNull();
    }

    private static Copy copy(String node, int n, long ts, String clock) {
        return new Copy(node, new JSONObject()
                .put("n", n).put("_ts", ts).put("_vclock", new JSONObject(clock)));
    }
}
