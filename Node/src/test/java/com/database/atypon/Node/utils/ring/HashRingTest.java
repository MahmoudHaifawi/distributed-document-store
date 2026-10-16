package com.database.atypon.Node.utils.ring;

import com.database.atypon.Node.model.Node;
import org.junit.jupiter.api.Test;

import java.util.Comparator;
import java.util.List;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

class HashRingTest {

    @Test
    void preferenceListHasRequestedSizeOfDistinctNodes() {
        HashRing ring = new HashRing(nodes(5), 150);

        List<Node> pref = ring.preferenceList("shop.products", 3);

        assertThat(pref).hasSize(3);
        assertThat(pref.stream().map(Node::getName).distinct().count()).isEqualTo(3);
    }

    @Test
    void replicationFactorClampsToClusterSize() {
        assertThat(new HashRing(nodes(2), 150).preferenceList("k", 3)).hasSize(2);
        assertThat(new HashRing(nodes(1), 150).preferenceList("k", 3)).hasSize(1);
    }

    @Test
    void placementIsDeterministicRegardlessOfNodeInputOrder() {
        List<Node> a = List.of(n("A"), n("B"), n("C"));
        List<Node> b = List.of(n("C"), n("A"), n("B"));

        assertThat(names(new HashRing(a, 150).preferenceList("shop.products", 2)))
                .isEqualTo(names(new HashRing(b, 150).preferenceList("shop.products", 2)));
    }

    @Test
    void keyPastLastVnodeWrapsToRingStart() {
        // One vnode per node makes the ring boundaries known: the vnode of node X hashes "X#0".
        List<Node> nodes = List.of(n("A"), n("B"), n("C"));
        HashRing ring = new HashRing(nodes, 1);
        long lastVnode = nodes.stream().mapToLong(node -> HashRing.hash(node.getName() + "#0")).max().orElseThrow();
        Node firstOnRing = nodes.stream()
                .min(Comparator.comparingLong(node -> HashRing.hash(node.getName() + "#0")))
                .orElseThrow();
        String keyPastLastVnode = IntStream.iterate(0, i -> i + 1)
                .mapToObj(i -> "key" + i)
                .filter(k -> HashRing.hash(k) > lastVnode)
                .findFirst()
                .orElseThrow();

        List<Node> pref = ring.preferenceList(keyPastLastVnode, 3);

        assertThat(pref).hasSize(3);
        assertThat(pref.get(0).getName()).isEqualTo(firstOnRing.getName());
        assertThat(ring.primary(keyPastLastVnode).getName()).isEqualTo(firstOnRing.getName());
    }

    @Test
    void addingANodeMovesAboutOneNthOfKeys() {
        HashRing before = new HashRing(nodes(4), 150);
        HashRing after = new HashRing(nodes(5), 150);
        int moved = 0;
        int keys = 5000;
        for (int i = 0; i < keys; i++) {
            String k = "s" + i;
            if (!before.primary(k).getName().equals(after.primary(k).getName())) {
                moved++;
            }
        }
        // ~1/5 expected, well under the ~4/5 a naive modulo placement would move
        assertThat((double) moved / keys).isLessThan(2.0 / 5);
    }

    @Test
    void configDefaultsWhenEnvUnset() {
        assertThat(RingConfig.replicationFactor()).isEqualTo(3);
        assertThat(RingConfig.vnodesPerNode()).isEqualTo(150);
    }

    private static List<Node> nodes(int count) {
        return IntStream.range(0, count).mapToObj(i -> n("Node" + i)).toList();
    }

    private static Node n(String name) {
        return new Node(name, "8080");
    }

    private static List<String> names(List<Node> nodes) {
        return nodes.stream().map(Node::getName).toList();
    }
}
