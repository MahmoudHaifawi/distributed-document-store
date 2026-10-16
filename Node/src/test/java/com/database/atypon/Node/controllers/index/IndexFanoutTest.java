package com.database.atypon.Node.controllers.index;

import com.database.atypon.Node.model.Network;
import com.database.atypon.Node.model.Node;
import com.database.atypon.Node.services.index.IndexManager;
import com.database.atypon.Node.utils.response.Response;
import com.database.atypon.Node.utils.response.ResponseType;
import com.database.atypon.Node.utils.ring.HashRing;
import com.database.atypon.Node.utils.ring.RingRouter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * An index built or dropped by a client is applied on the schema's primary and copied only to the
 * schema's other replicas on the ring, never to every peer. The invariant under test is "index fan-out
 * targets == RingRouter.otherReplicas(db, schema)", checked on the target list itself and on the peers
 * the create and drop calls actually reach.
 */
class IndexFanoutTest {

    private static final String DB = "shop";

    private List<Node> savedNodes;
    private Node savedSelf;
    private HashRing savedRing;

    /** Peer names that a create or drop reached, in no particular order. */
    private final List<String> created = new CopyOnWriteArrayList<>();
    private final List<String> dropped = new CopyOnWriteArrayList<>();

    private IndexController controller;

    @BeforeEach
    void fiveNodeCluster() {
        savedNodes = Network.nodes;
        savedSelf = Network.self;
        savedRing = Network.ring;

        Network.nodes = Collections.synchronizedList(new ArrayList<>(List.of(
                peer("Node1"), peer("Node2"), peer("Node3"), peer("Node4"))));
        Network.self = new Node("Node0", "8080");
        Network.rebuildRing();

        controller = new IndexController(mock(IndexManager.class));
    }

    @AfterEach
    void restoreMembership() {
        SecurityContextHolder.clearContext();
        Network.nodes = savedNodes;
        Network.self = savedSelf;
        Network.ring = savedRing;
    }

    @Test
    void fanOutTargetsAreTheOtherReplicasNotAllPeers() {
        String schema = schemaWhere(true, false);

        List<String> fannedOut = namesOf(controller.replicaTargets(DB, schema));

        assertThat(fannedOut).isEqualTo(namesOf(RingRouter.otherReplicas(DB, schema)));
        assertThat(fannedOut).hasSizeLessThan(Network.nodes.size());
    }

    @Test
    void targetsExcludeSelfWhetherOrNotSelfHoldsTheSchema() {
        for (boolean selfIsReplica : new boolean[]{true, false}) {
            String schema = schemaWhere(selfIsReplica, false);

            List<String> targets = namesOf(controller.replicaTargets(DB, schema));

            assertThat(targets).doesNotContain(Network.self.getName());
            assertThat(targets).hasSize(selfIsReplica ? 2 : 3); // RF 3 of 5, minus self when it holds a copy
        }
    }

    @Test
    void createIndexByAdminOriginReachesExactlyTheOtherReplicas() {
        String schema = schemaWhere(true, true);
        actAs("ROLE_ADMIN");

        Vector<Response> responses = controller.createIndex(DB, schema, "Age");

        List<String> expected = namesOf(RingRouter.otherReplicas(DB, schema));
        assertThat(created).containsExactlyInAnyOrderElementsOf(expected);
        assertThat(dropped).isEmpty();
        assertThat(responses).hasSize(1 + expected.size()); // applied here + one answer per replica
        assertThat(responses.get(0).getResponseType()).isEqualTo(ResponseType.SUCCESS);
    }

    @Test
    void dropIndexByAdminOriginReachesExactlyTheOtherReplicas() {
        String schema = schemaWhere(true, true);
        actAs("ROLE_ADMIN");

        Vector<Response> responses = controller.dropIndex(DB, schema, "Age");

        List<String> expected = namesOf(RingRouter.otherReplicas(DB, schema));
        assertThat(dropped).containsExactlyInAnyOrderElementsOf(expected);
        assertThat(created).isEmpty();
        assertThat(responses).hasSize(1 + expected.size());
        assertThat(responses.get(0).getResponseType()).isEqualTo(ResponseType.SUCCESS);
    }

    @Test
    void internalPeerCopyIsAppliedLocallyWithoutFanningOutAgain() {
        String schema = schemaWhere(true, true);
        actAs("ROLE_INTERNAL");

        Vector<Response> created = controller.createIndex(DB, schema, "Age");
        Vector<Response> dropped = controller.dropIndex(DB, schema, "Age");

        assertThat(this.created).isEmpty();
        assertThat(this.dropped).isEmpty();
        assertThat(created).hasSize(1);
        assertThat(dropped).hasSize(1);
    }

    @Test
    void singleNodeFansOutToNobody() {
        Network.nodes = Collections.synchronizedList(new ArrayList<>());
        Network.rebuildRing();
        actAs("ROLE_ADMIN");

        assertThat(controller.replicaTargets(DB, "products")).isEmpty();
        Vector<Response> responses = controller.createIndex(DB, "products", "Age");

        assertThat(responses).hasSize(1); // applied here, nothing to copy to, nothing forwarded
        assertThat(responses.get(0).getResponseType()).isEqualTo(ResponseType.SUCCESS);
        assertThat(created).isEmpty();
    }

    private static void actAs(String authority) {
        SecurityContextHolder.getContext().setAuthentication(new UsernamePasswordAuthenticationToken(
                "tester", "token", List.of(new SimpleGrantedAuthority(authority))));
    }

    /**
     * A schema (by scan) whose preference list does or does not include this node, optionally also
     * requiring this node to be its primary.
     */
    private static String schemaWhere(boolean selfIsReplica, boolean selfIsPrimary) {
        for (int i = 0; i < 1000; i++) {
            String schema = "s" + i;
            if (RingRouter.isReplica(DB, schema) == selfIsReplica
                    && (!selfIsPrimary || RingRouter.isPrimary(DB, schema))) {
                return schema;
            }
        }
        throw new AssertionError("no schema found with selfIsReplica=" + selfIsReplica
                + " selfIsPrimary=" + selfIsPrimary);
    }

    private Node peer(String name) {
        return new Node(name, "8080") {
            @Override
            public Response createIndex(String database, String schema, String field) {
                created.add(getName());
                return new Response(ResponseType.SUCCESS, "ok");
            }

            @Override
            public Response dropIndex(String database, String schema, String field) {
                dropped.add(getName());
                return new Response(ResponseType.SUCCESS, "ok");
            }
        };
    }

    private static List<String> namesOf(List<Node> nodes) {
        return nodes.stream().map(Node::getName).toList();
    }
}
