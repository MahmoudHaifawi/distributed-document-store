package com.database.atypon.Node.utils.quorum;

import org.junit.jupiter.api.Test;

import java.util.List;

import static java.util.stream.Collectors.toList;
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
        // the service repairs docs that are present AND version < winner; a not-found replica is excluded
        List<ReplicaDoc> docs = List.of(
                new ReplicaDoc("A", true, 1, "{\"_version\":1}"),
                new ReplicaDoc("B", true, 3, "{\"_version\":3}"),
                new ReplicaDoc("C", false, 0, null));
        ReplicaDoc winner = Quorum.pickFreshest(docs);
        List<ReplicaDoc> repair = docs.stream().filter(d -> d.present && d.version < winner.version).collect(toList());
        assertThat(repair).extracting(d -> d.node).containsExactly("A");   // not C (not-found, no resurrection)
    }
}
