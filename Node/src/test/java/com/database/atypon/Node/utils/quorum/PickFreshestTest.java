package com.database.atypon.Node.utils.quorum;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class PickFreshestTest {

    @Test
    void picksHighestPresentVersion() {
        ReplicaDoc r = Quorum.pickFreshest(List.of(
                new ReplicaDoc("A", true, 1, "{\"_version\":1}"),
                new ReplicaDoc("B", true, 3, "{\"_version\":3}"),
                new ReplicaDoc("C", false, 0, null)));
        assertThat(r.node).isEqualTo("B");
        assertThat(r.version).isEqualTo(3);
    }

    @Test
    void returnsNullWhenNonePresent() {
        assertThat(Quorum.pickFreshest(List.of(new ReplicaDoc("A", false, 0, null)))).isNull();
    }

    @Test
    void returnsNullForNoReplicasAtAll() {
        assertThat(Quorum.pickFreshest(List.of())).isNull();
    }

    @Test
    void repairTargetsArePresentReplicasBelowWinner() {
        // repair goes to replicas that are present AND older than the winner: not the winner itself ("B") and
        // not a not-found replica ("C"), which must never be re-created
        List<ReplicaDoc> docs = List.of(
                new ReplicaDoc("A", true, 1, "{\"_version\":1}"),
                new ReplicaDoc("B", true, 3, "{\"_version\":3}"),
                new ReplicaDoc("C", false, 0, null));

        List<ReplicaDoc> targets = Quorum.staleTargets(docs, Quorum.pickFreshest(docs));

        assertThat(targets).extracting(d -> d.node).containsExactly("A");
        assertThat(targets).extracting(d -> d.node).doesNotContain("B", "C");
    }

    @Test
    void repairTargetsKeepEveryOlderPresentReplicaInOrder() {
        List<ReplicaDoc> docs = List.of(
                new ReplicaDoc("A", true, 2, "{\"_version\":2}"),
                new ReplicaDoc("B", true, 5, "{\"_version\":5}"),
                new ReplicaDoc("C", true, 1, "{\"_version\":1}"),
                new ReplicaDoc("D", true, 5, "{\"_version\":5}"));

        assertThat(Quorum.staleTargets(docs, Quorum.pickFreshest(docs)))
                .extracting(d -> d.node).containsExactly("A", "C");
    }

    @Test
    void noRepairTargetsWhenNoReplicaHoldsTheDocument() {
        List<ReplicaDoc> docs = List.of(
                new ReplicaDoc("A", false, 0, null),
                new ReplicaDoc("B", false, 0, null));

        ReplicaDoc winner = Quorum.pickFreshest(docs);

        assertThat(winner).isNull();
        assertThat(Quorum.staleTargets(docs, winner)).isEmpty();
    }
}
