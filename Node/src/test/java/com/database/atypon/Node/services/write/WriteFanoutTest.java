package com.database.atypon.Node.services.write;

import com.database.atypon.Node.model.Network;
import com.database.atypon.Node.model.Node;
import com.database.atypon.Node.operations.write.WriteOperation;
import com.database.atypon.Node.utils.JsonKeys;
import com.database.atypon.Node.utils.response.Response;
import com.database.atypon.Node.utils.response.ResponseType;
import com.database.atypon.Node.utils.ring.HashRing;
import com.database.atypon.Node.utils.ring.RingRouter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * A write is copied only to the schema's other replicas on the ring, never to every peer. The invariant
 * under test is "fan-out targets == RingRouter.otherReplicas(db, schema)", checked both on the target
 * list itself and on the nodes each broadcast method actually calls.
 */
class WriteFanoutTest {

    private static final String DB = "shop";

    private List<Node> savedNodes;
    private Node savedSelf;
    private HashRing savedRing;

    /** Peer names that a broadcast reached, in no particular order (broadcasts run in parallel). */
    private final List<String> reached = new CopyOnWriteArrayList<>();

    private WriteService service;

    @BeforeEach
    void fiveNodeCluster() {
        savedNodes = Network.nodes;
        savedSelf = Network.self;
        savedRing = Network.ring;

        Network.nodes = Collections.synchronizedList(new ArrayList<>(List.of(
                peer("Node1"), peer("Node2"), peer("Node3"), peer("Node4"))));
        Network.self = new Node("Node0", "8080");
        Network.rebuildRing();

        service = new WriteService(mock(WriteOperation.class));
    }

    @AfterEach
    void restoreMembership() {
        Network.nodes = savedNodes;
        Network.self = savedSelf;
        Network.ring = savedRing;
    }

    @Test
    void documentBroadcastTargetsOtherReplicasNotAllPeers() {
        String schema = schemaWhere(true);

        List<String> fannedOut = namesOf(service.replicaTargets(DB, schema));

        assertThat(fannedOut).isEqualTo(namesOf(RingRouter.otherReplicas(DB, schema)));
        assertThat(fannedOut).hasSizeLessThan(Network.nodes.size());
    }

    @Test
    void targetsExcludeSelfWhetherOrNotSelfHoldsTheSchema() {
        for (boolean selfIsReplica : new boolean[]{true, false}) {
            String schema = schemaWhere(selfIsReplica);

            List<String> targets = namesOf(service.replicaTargets(DB, schema));

            assertThat(targets).doesNotContain(Network.self.getName());
            assertThat(targets).hasSize(selfIsReplica ? 2 : 3); // RF 3 of 5, minus self when it holds a copy
        }
    }

    @Test
    void everyBroadcastMethodReachesExactlyTheOtherReplicas() {
        String schema = schemaWhere(true);
        List<String> expected = namesOf(RingRouter.otherReplicas(DB, schema));
        HashMap<String, Object> body = new HashMap<>();
        body.put("n", 1);

        HashMap<String, Object> schemaBody = new HashMap<>();
        schemaBody.put(JsonKeys.SCHEMA_NAME, schema);
        schemaBody.put(JsonKeys.SCHEMA, new HashMap<String, Object>());

        reached.clear();
        service.broadcastSchema(DB, schemaBody);
        assertThat(reached).as("broadcastSchema").containsExactlyInAnyOrderElementsOf(expected);

        reached.clear();
        service.broadcastDocument(DB, schema, body);
        assertThat(reached).as("broadcastDocument").containsExactlyInAnyOrderElementsOf(expected);

        reached.clear();
        service.broadcastUpdate(DB, schema, "0", body);
        assertThat(reached).as("broadcastUpdate").containsExactlyInAnyOrderElementsOf(expected);

        reached.clear();
        service.broadcastDelete(DB, schema, "0");
        assertThat(reached).as("broadcastDelete").containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    void singleNodeFansOutToNobody() {
        Network.nodes = Collections.synchronizedList(new ArrayList<>());
        Network.rebuildRing();

        assertThat(service.replicaTargets(DB, "products")).isEmpty();
        assertThat(service.broadcastDocument(DB, "products", new HashMap<>())).isEmpty();
    }

    /** A schema (by scan) whose preference list does or does not include this node. */
    private static String schemaWhere(boolean selfIsReplica) {
        for (int i = 0; i < 1000; i++) {
            String schema = "s" + i;
            if (RingRouter.isReplica(DB, schema) == selfIsReplica) {
                return schema;
            }
        }
        throw new AssertionError("no schema found with selfIsReplica=" + selfIsReplica);
    }

    private Node peer(String name) {
        return new Node(name, "8080") {
            @Override
            public Response createSchema(String database, HashMap<String, Object> schema) {
                return record();
            }

            @Override
            public Response createDocument(String database, String schema, HashMap<String, Object> document) {
                return record();
            }

            @Override
            public Response updateDocument(String database, String schema, String id, HashMap<String, Object> document) {
                return record();
            }

            @Override
            public Response deleteDocument(String database, String schema, String id) {
                return record();
            }

            private Response record() {
                reached.add(getName());
                return new Response(ResponseType.SUCCESS, "ok");
            }
        };
    }

    private static List<String> namesOf(List<Node> nodes) {
        return nodes.stream().map(Node::getName).toList();
    }
}
