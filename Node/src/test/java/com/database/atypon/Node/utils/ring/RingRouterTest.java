package com.database.atypon.Node.utils.ring;

import com.database.atypon.Node.model.Network;
import com.database.atypon.Node.model.Node;
import com.database.atypon.Node.utils.AffinityLoadBalancer;
import com.database.atypon.Node.utils.response.Response;
import com.database.atypon.Node.utils.response.ResponseType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class RingRouterTest {

    private List<Node> savedNodes;
    private Node savedSelf;
    private HashRing savedRing;

    @BeforeEach
    void saveMembership() {
        savedNodes = Network.nodes;
        savedSelf = Network.self;
        savedRing = Network.ring;
    }

    @AfterEach
    void restoreMembership() {
        Network.nodes = savedNodes;
        Network.self = savedSelf;
        Network.ring = savedRing;
    }

    @Test
    void singleNodeOwnsEverything() {
        Network.nodes = Collections.synchronizedList(new ArrayList<>());
        Network.self = n("Node0");
        Network.rebuildRing();

        assertThat(RingRouter.isPrimary("shop", "products")).isTrue();
        assertThat(RingRouter.isReplica("shop", "products")).isTrue();
        assertThat(RingRouter.otherReplicas("shop", "products")).isEmpty();
    }

    @Test
    void singleNodeOwnsEverythingEvenWhenNodesIsNull() {
        Network.nodes = null;
        Network.self = n("Node0");
        Network.rebuildRing();

        assertThat(RingRouter.isPrimary("shop", "products")).isTrue();
        assertThat(RingRouter.isReplica("shop", "products")).isTrue();
        assertThat(RingRouter.otherReplicas("shop", "products")).isEmpty();
    }

    @Test
    void replicasMatchRingPreferenceListAndExcludeSelfInOtherReplicas() {
        fiveNodeCluster();
        int rf = Math.min(RingConfig.replicationFactor(), 5);

        List<Node> replicas = RingRouter.replicasFor("shop", "products");

        assertThat(replicas).hasSize(rf);
        assertThat(replicas).isEqualTo(Network.ring.preferenceList("shop.products", rf));
        assertThat(RingRouter.otherReplicas("shop", "products"))
                .allSatisfy(nd -> assertThat(nd.getName()).isNotEqualTo(Network.self.getName()));
    }

    @Test
    void otherReplicasIsPreferenceListMinusSelf() {
        fiveNodeCluster();

        for (int i = 0; i < 50; i++) {
            String schema = "schema" + i;
            List<String> expected = names(RingRouter.replicasFor("shop", schema)).stream()
                    .filter(name -> !name.equals(Network.self.getName()))
                    .toList();

            assertThat(names(RingRouter.otherReplicas("shop", schema))).isEqualTo(expected);
        }
    }

    @Test
    void primaryForIsFirstReplica() {
        fiveNodeCluster();

        assertThat(RingRouter.primaryFor("shop", "products").getName())
                .isEqualTo(RingRouter.replicasFor("shop", "products").get(0).getName());
        assertThat(RingRouter.primaryFor("shop", "products").getName())
                .isEqualTo(Network.ring.primary("shop.products").getName());
    }

    @Test
    void isReplicaTrueIffSelfInPreferenceList() {
        fiveNodeCluster();

        boolean sawReplica = false;
        boolean sawNonReplica = false;
        for (int i = 0; i < 100; i++) {
            String schema = "schema" + i;
            boolean selfInList = names(RingRouter.replicasFor("shop", schema)).contains(Network.self.getName());

            assertThat(RingRouter.isReplica("shop", schema)).isEqualTo(selfInList);
            sawReplica |= selfInList;
            sawNonReplica |= !selfInList;
        }

        // 5 nodes, RF 3: over 100 schemas self must be a replica for some and not for others.
        assertThat(sawReplica).isTrue();
        assertThat(sawNonReplica).isTrue();
    }

    @Test
    void exactlyOneNodeIsPrimaryForEachSchema() {
        fiveNodeCluster();
        List<Node> members = new ArrayList<>(Network.nodes);
        members.add(Network.self);

        for (int i = 0; i < 50; i++) {
            String schema = "schema" + i;
            long primaries = 0;
            for (Node member : members) {
                Network.self = member;
                if (RingRouter.isPrimary("shop", schema)) {
                    primaries++;
                }
            }
            assertThat(primaries).isEqualTo(1);
        }
    }

    @Test
    void checkAffinityIsSuccessOnSingleNode() {
        Network.nodes = Collections.synchronizedList(new ArrayList<>());
        Network.self = n("Node0");
        Network.rebuildRing();

        Response response = AffinityLoadBalancer.checkAffinity("shop", "products");

        assertThat(response.getResponseType()).isEqualTo(ResponseType.SUCCESS);
    }

    @Test
    void checkAffinityPointsAtThePrimaryWhenSelfIsNotPrimary() {
        fiveNodeCluster();

        boolean sawSuccess = false;
        boolean sawError = false;
        for (int i = 0; i < 100; i++) {
            String schema = "schema" + i;
            Response response = AffinityLoadBalancer.checkAffinity("shop", schema);

            if (RingRouter.isPrimary("shop", schema)) {
                assertThat(response.getResponseType()).isEqualTo(ResponseType.SUCCESS);
                sawSuccess = true;
            } else {
                assertThat(response.getResponseType()).isEqualTo(ResponseType.ERROR);
                assertThat(((Node) response.getContent()).getName())
                        .isEqualTo(RingRouter.primaryFor("shop", schema).getName());
                sawError = true;
            }
        }

        assertThat(sawSuccess).isTrue();
        assertThat(sawError).isTrue();
    }

    @Test
    void ringIsRebuiltFromNodesAndSelf() {
        fiveNodeCluster();

        assertThat(Network.ring.physicalNodeCount()).isEqualTo(5);
    }

    @Test
    void setNodesAndAssignSelfBuildTheRingWithoutAnExplicitRebuild() {
        Network network = new Network();
        Network.self = null;
        Network.ring = null;

        network.setNodes(new ArrayList<>(List.of(n("Node1"), n("Node2"), n("Node3"), n("Node4"))));

        assertThat(Network.ring).isNotNull();
        assertThat(Network.ring.physicalNodeCount()).isEqualTo(4);
        HashRing afterSetNodes = Network.ring;

        network.assignSelf(n("Node0"));

        assertThat(Network.ring).isNotNull().isNotSameAs(afterSetNodes);
        assertThat(Network.ring.physicalNodeCount()).isEqualTo(5);
        // Routing reflects the 5-node membership: the same placement a ring built by hand gives.
        HashRing expected = new HashRing(
                List.of(n("Node0"), n("Node1"), n("Node2"), n("Node3"), n("Node4")), RingConfig.vnodesPerNode());
        int rf = Math.min(RingConfig.replicationFactor(), 5);
        for (int i = 0; i < 50; i++) {
            String schema = "schema" + i;
            assertThat(names(RingRouter.replicasFor("shop", schema)))
                    .isEqualTo(names(expected.preferenceList("shop." + schema, rf)));
        }
    }

    @Test
    void addNodeRebuildsTheRingWithoutAnExplicitRebuild() {
        Network network = new Network();
        Network.self = null;
        Network.ring = null;
        network.setNodes(new ArrayList<>(List.of(n("Node1"), n("Node2"), n("Node3"), n("Node4"))));
        network.assignSelf(n("Node0"));
        HashRing before = Network.ring;
        assertThat(before.physicalNodeCount()).isEqualTo(5);

        network.addNode(n("Node5"));

        assertThat(Network.ring).isNotNull().isNotSameAs(before);
        assertThat(Network.ring.physicalNodeCount()).isEqualTo(6);
        // The new node now takes part in placement: it holds a replica of some schemas.
        boolean newNodePlaced = false;
        for (int i = 0; i < 100 && !newNodePlaced; i++) {
            newNodePlaced = names(RingRouter.replicasFor("shop", "schema" + i)).contains("Node5");
        }
        assertThat(newNodePlaced).isTrue();
    }

    private static void fiveNodeCluster() {
        Network.nodes = Collections.synchronizedList(
                new ArrayList<>(List.of(n("Node1"), n("Node2"), n("Node3"), n("Node4"))));
        Network.self = n("Node0");
        Network.rebuildRing();
    }

    private static Node n(String name) {
        return new Node(name, "8080");
    }

    private static List<String> names(List<Node> nodes) {
        return nodes.stream().map(Node::getName).toList();
    }
}
