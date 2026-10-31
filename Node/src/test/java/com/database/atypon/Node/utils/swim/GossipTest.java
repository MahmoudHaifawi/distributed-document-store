package com.database.atypon.Node.utils.swim;

import com.database.atypon.Node.controllers.SwimController;
import com.database.atypon.Node.model.Network;
import com.database.atypon.Node.model.Node;
import com.database.atypon.Node.utils.ring.HashRing;
import com.database.atypon.Node.utils.swim.dto.AckMessage;
import com.database.atypon.Node.utils.swim.dto.MemberUpdate;
import com.database.atypon.Node.utils.swim.dto.PingMessage;
import com.database.atypon.Node.utils.swim.dto.PingReqMessage;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

/**
 * Infection-style gossip: the retransmit queue inside {@link MembershipTable}, the way inbound
 * gossip is merged, and the way probes carry it. The queue mechanics are plain unit tests; the wire
 * is exercised against real {@link HttpServer} stubs, one of which fronts a second, real
 * {@link MembershipTable} so that a whole probe round trip between two nodes is covered.
 */
class GossipTest {

    private static final String SELF = "Node0";
    private static final String BEARER = "Bearer gossip-test-token";
    private static final long PING_TIMEOUT_MS = 500L;
    private static final long T0 = 50_000L;
    private static final ObjectMapper JSON = new ObjectMapper();

    private List<Node> savedNodes;
    private Node savedSelf;
    private HashRing savedRing;
    private Function<String, String> savedEnv;

    private final List<Stub> stubs = new ArrayList<>();
    private final List<SwimDetector> detectors = new ArrayList<>();
    private AtomicLong clock;
    private MembershipTable table;

    // ---- fixtures -----------------------------------------------------------------------------

    private record Received(String path, String auth, String body) {
    }

    @FunctionalInterface
    private interface Responder {
        String reply(String path, String body) throws Exception;
    }

    /** A peer on a loopback address that records every request and answers with whatever its responder says. */
    private static final class Stub {
        final HttpServer server;
        final Node node;
        final List<Received> received = Collections.synchronizedList(new ArrayList<>());
        volatile Responder responder = (path, body) -> "{\"acked\":true}";

        Stub() throws IOException {
            server = HttpServer.create(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0), 0);
            node = new Node("127.0.0.1", String.valueOf(server.getAddress().getPort()));
            server.createContext("/", this::serve);
        }

        private void serve(HttpExchange exchange) throws IOException {
            String path = exchange.getRequestURI().getPath();
            String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            received.add(new Received(path, exchange.getRequestHeaders().getFirst("Authorization"), body));
            int status = 200;
            String reply;
            try {
                reply = responder.reply(path, body);
            } catch (Exception e) {
                status = 500;
                reply = "{\"error\":\"" + e + "\"}";
            }
            byte[] bytes = reply.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(status, bytes.length);
            exchange.getResponseBody().write(bytes);
            exchange.close();
        }

        Received only(String path) {
            synchronized (received) {
                List<Received> matching = received.stream().filter(r -> r.path().equals(path)).toList();
                assertThat(matching).as("requests to %s", path).hasSize(1);
                return matching.get(0);
            }
        }
    }

    @BeforeEach
    void formCluster() {
        savedNodes = Network.nodes;
        savedSelf = Network.self;
        savedRing = Network.ring;
        savedEnv = SwimConfig.env;

        Network.nodes = Collections.synchronizedList(new ArrayList<>());
        Network.self = n(SELF);
        Network.ring = null;

        clock = new AtomicLong(1_000L);
        table = new MembershipTable(clock::get);
        table.initSelf(n(SELF));
    }

    @AfterEach
    void restore() {
        stubs.forEach(s -> s.server.stop(0));
        detectors.forEach(SwimDetector::close);
        SwimConfig.env = savedEnv;
        Network.nodes = savedNodes;
        Network.self = savedSelf;
        Network.ring = savedRing;
    }

    private static Node n(String name) {
        return new Node(name, "8080");
    }

    private static Member m(String name, MemberState state, int incarnation) {
        return new Member(n(name), state, incarnation, 0L);
    }

    private static MemberUpdate u(String name, MemberState state, int incarnation) {
        return new MemberUpdate(name, "8080", state, incarnation);
    }

    /** A table of {@code live} live members (this node and {@code live - 1} peers) with an empty gossip queue. */
    private MembershipTable clusterOf(int live) {
        MembershipTable t = new MembershipTable(clock::get);
        t.initSelf(n(SELF));
        for (int i = 1; i < live; i++) {
            t.merge(m("Node" + i, MemberState.ALIVE, 0));
        }
        drainAll(t);
        return t;
    }

    private static void drainAll(MembershipTable t) {
        for (int i = 0; i < 1_000 && !t.drainGossip(1_000).isEmpty(); i++) {
            // each drain spends one retransmit of everything it returns, so this ends
        }
    }

    /** How many times the lone queued update is handed out before it disappears. */
    private static int sendsUntilGone(MembershipTable t) {
        for (int sends = 0; sends < 1_000; sends++) {
            if (t.drainGossip(1).isEmpty()) {
                return sends;
            }
        }
        throw new AssertionError("the update never expired");
    }

    private void env(Map<String, String> values) {
        SwimConfig.env = values::get;
    }

    private void gossipLambda(int lambda) {
        env(Map.of("SWIM_GOSSIP_LAMBDA", String.valueOf(lambda)));
    }

    private Stub stub() throws IOException {
        Stub stub = new Stub();
        stubs.add(stub);
        return stub;
    }

    private SwimClient clientFor(MembershipTable t) {
        return new SwimClient(t, PING_TIMEOUT_MS, () -> BEARER);
    }

    /** A node other than this one: where it listens, and the table it keeps. */
    private record RealNode(Stub stub, MembershipTable table) {
        Node node() {
            return stub.node;
        }
    }

    /**
     * A real node B: its own table behind a real {@link SwimController}, reached over HTTP through a stub.
     * B is named by the stub's loopback address.
     */
    private RealNode realNode() throws IOException {
        Stub stub = stub();
        MembershipTable tableB = new MembershipTable(clock::get);
        tableB.initSelf(stub.node);
        SwimController controllerB = controllerFor(tableB);
        stub.responder = (path, body) ->
                JSON.writeValueAsString(controllerB.ping(JSON.readValue(body, PingMessage.class)));
        stub.server.start();
        return new RealNode(stub, tableB);
    }

    private static List<String> described(List<Member> members) {
        return members.stream()
                .map(x -> x.getNode().getName() + ":" + x.getState() + "@" + x.getIncarnation())
                .toList();
    }

    /** The gossip list of a captured request body as {@code name:STATE@incarnation}. */
    private static List<String> gossipIn(String body) throws IOException {
        List<String> out = new ArrayList<>();
        for (JsonNode update : JSON.readTree(body).get("gossip")) {
            out.add(update.get("name").asText() + ":" + update.get("state").asText() + "@" + update.get("incarnation").asInt());
        }
        return out;
    }

    private static String ackJson(boolean acked, MemberUpdate... gossip) throws IOException {
        return JSON.writeValueAsString(new AckMessage(acked, Arrays.asList(gossip)));
    }

    // ---- retransmit queue ---------------------------------------------------------------------

    @Test
    void updateIsReGossipedBoundedTimes() {
        // With lambda = 3 and N live members an update is handed out ceil(3 * log2(N + 1)) times.
        int[][] liveToSends = {{1, 3}, {2, 5}, {3, 6}, {4, 7}, {5, 8}, {6, 9}, {7, 9}, {8, 10}};
        for (int[] row : liveToSends) {
            MembershipTable t = clusterOf(row[0]);
            Member update = m("Gone", MemberState.DEAD, 1);
            t.enqueue(update);

            assertThat(t.drainGossip(6)).as("first send with %d live members", row[0]).containsExactly(update);
            assertThat(sendsUntilGone(t) + 1).as("sends with %d live members", row[0]).isEqualTo(row[1]);
            assertThat(t.drainGossip(6)).as("an exhausted update never comes back").isEmpty();
        }
    }

    @Test
    void retransmitBudgetScalesWithLambda() {
        for (int[] row : new int[][]{{1, 2}, {2, 4}, {3, 6}, {5, 10}}) {
            gossipLambda(row[0]);
            MembershipTable t = clusterOf(3); // log2(3 + 1) = 2
            t.enqueue(m("Gone", MemberState.DEAD, 1));

            assertThat(sendsUntilGone(t)).as("lambda %d", row[0]).isEqualTo(row[1]);
        }
    }

    @Test
    void drainReturnsHighestCounterFirstCappedAtMax() {
        MembershipTable t = clusterOf(3); // log2(4) = 2, so the budget is 2 * lambda
        gossipLambda(1);
        t.enqueue(m("Low", MemberState.DEAD, 1));   // budget 2
        gossipLambda(3);
        t.enqueue(m("High", MemberState.DEAD, 1));  // budget 6
        gossipLambda(2);
        t.enqueue(m("Mid", MemberState.DEAD, 1));   // budget 4

        // the two highest counters, highest first; the lowest is left behind
        assertThat(described(t.drainGossip(2))).containsExactly("High:DEAD@1", "Mid:DEAD@1");
        // High is on 5, Mid on 3, Low on 2: the same two again
        assertThat(described(t.drainGossip(2))).containsExactly("High:DEAD@1", "Mid:DEAD@1");
        // High 4, Mid 2, Low 2: the tie goes to the older update
        List<String> third = described(t.drainGossip(2));
        assertThat(third).hasSize(2);
        assertThat(third.get(0)).isEqualTo("High:DEAD@1");

        // a cap never returns more, and a non-positive cap returns nothing and spends nothing
        assertThat(t.drainGossip(1)).hasSize(1);
        assertThat(t.drainGossip(0)).isEmpty();
        assertThat(t.drainGossip(-3)).isEmpty();
        assertThat(t.drainGossip(100)).hasSizeLessThanOrEqualTo(3);
    }

    @Test
    void aNewerUpdateForTheSameNodeReplacesTheQueuedOneWithAFreshBudget() {
        MembershipTable t = clusterOf(3);
        t.enqueue(m("X", MemberState.SUSPECT, 1));
        t.drainGossip(1);
        t.drainGossip(1);

        t.enqueue(m("X", MemberState.DEAD, 1));

        List<Member> next = t.drainGossip(10);
        assertThat(described(next)).containsExactly("X:DEAD@1");
        assertThat(sendsUntilGone(t) + 1).as("a fresh budget").isEqualTo(6);
    }

    @Test
    void aWeakerUpdateNeverDisplacesAStrongerQueuedOne() {
        MembershipTable t = clusterOf(3);
        t.enqueue(m("X", MemberState.DEAD, 1));
        t.enqueue(m("X", MemberState.SUSPECT, 1));  // same incarnation, weaker state
        t.enqueue(m("X", MemberState.ALIVE, 0));    // older incarnation
        t.enqueue(m("X", MemberState.DEAD, 1));     // an exact repeat must not top the budget up either

        assertThat(described(t.drainGossip(10))).containsExactly("X:DEAD@1");
        assertThat(sendsUntilGone(t) + 1).as("the budget of the first enqueue (3 live: 6), not topped up").isEqualTo(6);

        t.enqueue(m("X", MemberState.ALIVE, 2));    // a higher incarnation does win
        assertThat(described(t.drainGossip(10))).containsExactly("X:ALIVE@2");
    }

    @Test
    void concurrentUpdatesNeverDropAStateChangeFromTheQueue() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(3);
        try {
            for (int round = 0; round < 300; round++) {
                MembershipTable t = clusterOf(1);
                CountDownLatch go = new CountDownLatch(1);
                List<Future<?>> done = new ArrayList<>();
                for (MemberUpdate update : List.of(u("X", MemberState.ALIVE, 1), u("X", MemberState.SUSPECT, 1), u("X", MemberState.DEAD, 1))) {
                    done.add(pool.submit(() -> {
                        go.await();
                        t.mergeGossip(List.of(update));
                        return null;
                    }));
                }
                go.countDown();
                for (Future<?> f : done) {
                    f.get(10, TimeUnit.SECONDS);
                }

                // whatever the interleaving, the strongest claim wins the table AND is what gets re-gossiped
                assertThat(t.get("X").getState()).as("round %d", round).isEqualTo(MemberState.DEAD);
                assertThat(described(t.drainGossip(10))).as("round %d", round).containsExactly("X:DEAD@1");
            }
        } finally {
            pool.shutdownNow();
        }
    }

    // ---- the outgoing list --------------------------------------------------------------------

    @Test
    void outgoingGossipAlwaysCarriesOwnAliveAndRespectsTheCap() {
        MembershipTable t = clusterOf(3);
        for (int i = 0; i < 5; i++) {
            t.enqueue(m("Q" + i, MemberState.DEAD, 1));
        }

        List<MemberUpdate> out = t.outgoingGossip(3);

        // the cap is for the whole list, own ALIVE included
        assertThat(out).hasSize(3);
        assertThat(out).filteredOn(x -> x.getName().equals(SELF)).hasSize(1);
        assertThat(out).filteredOn(x -> x.getName().equals(SELF)).first()
                .extracting(MemberUpdate::getState, MemberUpdate::getIncarnation).containsExactly(MemberState.ALIVE, 0);
        assertThat(t.outgoingGossip(1)).extracting(MemberUpdate::getName).containsExactly(SELF);
        assertThat(t.outgoingGossip(0)).isEmpty();
    }

    @Test
    void outgoingGossipNamesOwnRefutationOnlyOnce() {
        MembershipTable t = clusterOf(2);
        t.merge(m(SELF, MemberState.SUSPECT, 4)); // refuted: ALIVE at 5, queued for gossip

        List<MemberUpdate> out = t.outgoingGossip(6);

        assertThat(out).filteredOn(x -> x.getName().equals(SELF))
                .extracting(MemberUpdate::getState, MemberUpdate::getIncarnation)
                .containsExactly(tuple(MemberState.ALIVE, 5));
    }

    @Test
    void aTableThatIsNotYetAMemberOfAnythingGossipsOnlyItsQueue() {
        MembershipTable bare = new MembershipTable(clock::get);
        bare.enqueue(m("Q", MemberState.DEAD, 1));

        assertThat(bare.outgoingGossip(6)).extracting(MemberUpdate::getName).containsExactly("Q");
    }

    // ---- inbound merge ------------------------------------------------------------------------

    private SwimController controllerFor(MembershipTable t) {
        return new SwimController(clientFor(t), t);
    }

    @Test
    void inboundGossipMergesByPrecedence() throws Exception {
        table.merge(m("X", MemberState.ALIVE, 0));
        SwimController controller = controllerFor(table);

        // the body exactly as a peer puts it on the wire
        PingMessage ping = JSON.readValue(
                "{\"from\":\"Node1\",\"gossip\":[{\"name\":\"X\",\"port\":\"8080\",\"state\":\"DEAD\",\"incarnation\":5}]}",
                PingMessage.class);
        controller.ping(ping);

        assertThat(table.get("X").getState()).isEqualTo(MemberState.DEAD);
        assertThat(table.get("X").getIncarnation()).isEqualTo(5);
        assertThat(Network.ring.physicalNodeCount()).as("a dead member leaves the ring").isEqualTo(1);

        // weaker or older claims change nothing
        controller.ping(new PingMessage("Node1", List.of(
                u("X", MemberState.ALIVE, 5), u("X", MemberState.SUSPECT, 5), u("X", MemberState.ALIVE, 4))));
        assertThat(described(List.of(table.get("X")))).containsExactly("X:DEAD@5");

        // a higher incarnation does, even with better news
        controller.ping(new PingMessage("Node1", List.of(u("X", MemberState.ALIVE, 6))));
        assertThat(described(List.of(table.get("X")))).containsExactly("X:ALIVE@6");
        assertThat(Network.ring.physicalNodeCount()).isEqualTo(2);
    }

    @Test
    void aMemberFirstHeardOfAsDeadIsStoredAsATombstone() {
        controllerFor(table).ping(new PingMessage("Node1", List.of(u("Y", MemberState.DEAD, 5))));

        assertThat(described(List.of(table.get("Y")))).containsExactly("Y:DEAD@5");
        assertThat(table.liveNodes()).extracting(Node::getName).containsExactly(SELF);
    }

    @Test
    void everyKindOfInboundBodyIsMerged() {
        SwimController controller = controllerFor(table);

        controller.ping(new PingMessage("Node1", List.of(u("A", MemberState.DEAD, 1))));
        // the target is unreachable, so the answer is "not acked" - but the gossip rode in regardless
        AckMessage answer = controller.pingReq(new PingReqMessage("Node1", new Node("127.0.0.1", "1"),
                List.of(u("B", MemberState.SUSPECT, 2))));

        assertThat(answer.isAcked()).isFalse();
        assertThat(described(List.of(table.get("A")))).containsExactly("A:DEAD@1");
        assertThat(described(List.of(table.get("B")))).containsExactly("B:SUSPECT@2");
    }

    @Test
    void changedUpdatesAreReEnqueued() {
        MembershipTable t = clusterOf(3);
        t.merge(m("X", MemberState.ALIVE, 0));
        drainAll(t);
        SwimController controller = controllerFor(t);

        AckMessage ack = controller.ping(new PingMessage("Node1", List.of(u("X", MemberState.SUSPECT, 0))));

        // the news is passed on at once, in the ack to the very ping that brought it
        assertThat(ack.getGossip()).extracting(MemberUpdate::getName, MemberUpdate::getState)
                .contains(tuple("X", MemberState.SUSPECT));
        // and keeps being passed on: a suspect is still live, so N is unchanged at 4 and the budget is
        // ceil(3 * log2(5)) = 7 sends, one of which the ack above already spent
        assertThat(described(t.drainGossip(10))).containsExactly("X:SUSPECT@0");
        assertThat(sendsUntilGone(t) + 2).isEqualTo(7);
        assertThat(sendsUntilGone(t)).isZero();
    }

    @Test
    void updatesThatChangedNothingAreNotReEnqueued() {
        MembershipTable t = clusterOf(3);
        t.merge(m("X", MemberState.DEAD, 3));
        drainAll(t);
        SwimController controller = controllerFor(t);

        // identical, weaker, older: all of it is stale news to this node
        controller.ping(new PingMessage("Node1", List.of(
                u("X", MemberState.DEAD, 3), u("X", MemberState.SUSPECT, 3), u("X", MemberState.ALIVE, 2),
                u("Node1", MemberState.ALIVE, 0))));

        assertThat(t.drainGossip(10)).as("nothing new to pass on, so gossip dies out instead of echoing forever").isEmpty();
    }

    @Test
    void malformedAndMissingGossipIsIgnoredWithoutLosingTheRest() throws Exception {
        SwimController controller = controllerFor(table);

        controller.ping(null);
        controller.ping(new PingMessage("Node1"));
        controller.ping(JSON.readValue("{\"from\":\"Node1\",\"gossip\":null}", PingMessage.class));
        controller.ping(new PingMessage("Node1", Arrays.asList(
                null,
                new MemberUpdate("", "8080", MemberState.ALIVE, 0),
                new MemberUpdate("NoState", "8080", null, 0),
                new MemberUpdate("NoPort", null, MemberState.ALIVE, 0),
                new MemberUpdate("Negative", "8080", MemberState.ALIVE, -1),
                u("Fine", MemberState.ALIVE, 0))));

        assertThat(table.snapshot()).extracting(x -> x.getNode().getName()).containsExactly(SELF, "Fine");
    }

    // ---- wire form ----------------------------------------------------------------------------

    @Test
    void aMemberUpdateIsTheWireFormOfAMemberWithoutItsLocalClock() throws Exception {
        Member member = new Member(new Node("Node3", "9090"), MemberState.SUSPECT, 4, 123_456L);

        MemberUpdate update = MemberUpdate.from(member);
        JsonNode wire = JSON.readTree(JSON.writeValueAsString(new AckMessage(true, List.of(update)))).get("gossip").get(0);

        List<String> fields = new ArrayList<>();
        wire.fieldNames().forEachRemaining(fields::add);
        assertThat(fields).containsExactlyInAnyOrder("name", "port", "state", "incarnation");
        assertThat(wire.get("name").asText()).isEqualTo("Node3");
        assertThat(wire.get("port").asText()).isEqualTo("9090");
        assertThat(wire.get("state").asText()).isEqualTo("SUSPECT");
        assertThat(wire.get("incarnation").asInt()).isEqualTo(4);

        // the receiver rebuilds a Member; lastChange is its own business, so it is not carried
        Member back = update.toMember();
        assertThat(back.getNode().getName()).isEqualTo("Node3");
        assertThat(back.getNode().getPort()).isEqualTo("9090");
        assertThat(back.getState()).isEqualTo(MemberState.SUSPECT);
        assertThat(back.getIncarnation()).isEqualTo(4);
        assertThat(back.getLastChange()).isZero();
    }

    @Test
    void theGossipFieldIsNeverNull() throws Exception {
        assertThat(new PingMessage("a").getGossip()).isEmpty();
        assertThat(new PingReqMessage("a", n("b")).getGossip()).isEmpty();
        assertThat(new AckMessage(true).getGossip()).isEmpty();
        assertThat(JSON.readValue("{\"acked\":true}", AckMessage.class).getGossip()).isEmpty();
        assertThat(JSON.readValue("{\"acked\":true,\"gossip\":null}", AckMessage.class).getGossip()).isEmpty();
    }

    // ---- over the wire ------------------------------------------------------------------------

    @Test
    void outgoingProbesCarryQueuedUpdatesAndTheSendersOwnAlive() throws Exception {
        Stub peer = stub();
        peer.server.start();
        table.merge(new Member(peer.node, MemberState.ALIVE, 0, 0));
        table.merge(m("Node7", MemberState.ALIVE, 2));
        SwimClient client = clientFor(table);

        assertThat(client.ping(peer.node)).isTrue();
        assertThat(client.pingReq(peer.node, n("Node7"))).isTrue();

        Received ping = peer.only("/swim/ping");
        assertThat(ping.auth()).isEqualTo(BEARER);
        assertThat(JSON.readTree(ping.body()).get("from").asText()).isEqualTo(SELF);
        assertThat(gossipIn(ping.body())).containsExactlyInAnyOrder(
                SELF + ":ALIVE@0", "127.0.0.1:ALIVE@0", "Node7:ALIVE@2");

        Received pingReq = peer.only("/swim/ping-req");
        assertThat(JSON.readTree(pingReq.body()).get("target").get("name").asText()).isEqualTo("Node7");
        assertThat(gossipIn(pingReq.body())).containsExactlyInAnyOrder(
                SELF + ":ALIVE@0", "127.0.0.1:ALIVE@0", "Node7:ALIVE@2");
    }

    @Test
    void aProbeCarriesAtMostGossipMaxUpdates() throws Exception {
        env(Map.of("SWIM_GOSSIP_MAX", "3"));
        Stub peer = stub();
        peer.server.start();
        for (int i = 0; i < 10; i++) {
            table.merge(m("Q" + i, MemberState.ALIVE, 0));
        }

        clientFor(table).ping(peer.node);

        List<String> gossip = gossipIn(peer.only("/swim/ping").body());
        assertThat(gossip).hasSize(3).contains(SELF + ":ALIVE@0");
    }

    @Test
    void gossipOnAnAckIsMergedAndPassedOn() throws Exception {
        Stub peer = stub();
        peer.responder = (path, body) -> path.equals("/swim/ping")
                ? ackJson(true, u("Node5", MemberState.DEAD, 2), u("Node6", MemberState.ALIVE, 0), u(SELF, MemberState.ALIVE, 0))
                : ackJson(false, u("Node8", MemberState.SUSPECT, 1));
        peer.server.start();
        table.merge(new Member(peer.node, MemberState.ALIVE, 0, 0));
        table.merge(m("Node5", MemberState.ALIVE, 2));
        drainAll(table);
        SwimClient client = clientFor(table);

        assertThat(client.ping(peer.node)).isTrue();
        assertThat(described(List.of(table.get("Node5")))).containsExactly("Node5:DEAD@2");
        assertThat(described(List.of(table.get("Node6")))).containsExactly("Node6:ALIVE@0");
        // what was new is queued for the next probe; the repeat about this node itself is not
        assertThat(described(table.drainGossip(10))).containsExactlyInAnyOrder("Node5:DEAD@2", "Node6:ALIVE@0");

        // a "no ack" answer still has valid gossip in it, and that is not thrown away
        assertThat(client.pingReq(peer.node, n("Node5"))).isFalse();
        assertThat(described(List.of(table.get("Node8")))).containsExactly("Node8:SUSPECT@1");
    }

    // ---- suspicion is cleared by the peer's own ALIVE ------------------------------------------

    @Test
    void ackCarryingHigherAliveClearsLocalSuspicion() throws Exception {
        Stub peer = stub();
        // B answers with its own ALIVE at a higher incarnation, as a node that refuted does
        peer.responder = (path, body) -> ackJson(true, new MemberUpdate("127.0.0.1", peer.node.getPort(), MemberState.ALIVE, 1));
        peer.server.start();
        table.merge(new Member(peer.node, MemberState.ALIVE, 0, 0));
        table.markSuspect("127.0.0.1", T0);
        assertThat(described(List.of(table.get("127.0.0.1")))).containsExactly("127.0.0.1:SUSPECT@0");
        SwimDetector detector = new SwimDetector(table, clientFor(table));
        detectors.add(detector);

        detector.tick(T0 + 1, n -> 0);

        Member b = table.get("127.0.0.1");
        assertThat(b.getState()).isEqualTo(MemberState.ALIVE);
        assertThat(b.getIncarnation()).isEqualTo(1);
        assertThat(b.getLastChange()).as("stamped by this node's clock, not the sender's").isEqualTo(clock.get());
        assertThat(Network.nodes).extracting(Node::getName).contains("127.0.0.1");
        assertThat(Network.ring.physicalNodeCount()).isEqualTo(2);
    }

    @Test
    void anAliveAtTheSameIncarnationDoesNotClearASuspicionButAHigherOneDoes() {
        table.merge(m("Node1", MemberState.ALIVE, 0));
        table.markSuspect("Node1", T0);

        table.mergeGossip(List.of(u("Node1", MemberState.ALIVE, 0)));
        assertThat(table.get("Node1").getState()).as("the merge rule: SUSPECT beats ALIVE at equal incarnation").isEqualTo(MemberState.SUSPECT);

        table.mergeGossip(List.of(u("Node1", MemberState.ALIVE, 1)));
        assertThat(described(List.of(table.get("Node1")))).containsExactly("Node1:ALIVE@1");
        assertThat(Network.nodes).extracting(Node::getName).containsExactly("Node1");
        assertThat(Network.ring.physicalNodeCount()).isEqualTo(2);
    }

    @Test
    void aPeerDeclaredDeadComesBackOntoTheRingWhenItAnswersAtAHigherIncarnation() {
        table.merge(m("Node1", MemberState.ALIVE, 0));
        table.markSuspect("Node1", T0);
        table.promoteExpiredSuspectsToDead(T0 + SwimConfig.suspectTimeoutMs());
        assertThat(Network.ring.physicalNodeCount()).as("dead, so off the ring").isEqualTo(1);

        table.mergeGossip(List.of(u("Node1", MemberState.ALIVE, 1)));

        assertThat(described(List.of(table.get("Node1")))).containsExactly("Node1:ALIVE@1");
        assertThat(Network.ring.physicalNodeCount()).as("back on the ring").isEqualTo(2);
        assertThat(Network.nodes).extracting(Node::getName).containsExactly("Node1");
    }

    @Test
    void localSuspicionAndDeathAreGossipedToo() {
        MembershipTable t = clusterOf(3);

        t.markSuspect("Node1", T0);
        assertThat(described(t.drainGossip(10))).containsExactly("Node1:SUSPECT@0");

        t.promoteExpiredSuspectsToDead(T0 + SwimConfig.suspectTimeoutMs());
        assertThat(described(t.drainGossip(10))).as("DEAD displaced the SUSPECT it replaced").containsExactly("Node1:DEAD@0");
    }

    @Test
    void aSuspectedPeerRefutesAndTheRefutationRidesBackOnTheAck() throws Exception {
        RealNode b = realNode();

        // A believes B is alive, then fails to reach it and suspects it
        table.merge(new Member(b.node(), MemberState.ALIVE, 0, 0));
        table.markSuspect(b.node().getName(), T0);
        SwimDetector detector = new SwimDetector(table, clientFor(table));
        detectors.add(detector);

        // B is reachable now: the next probe carries A's SUSPECT(B) to B, and B's refutation back to A
        detector.tick(T0 + 1, n -> 0);

        assertThat(described(List.of(b.table().get("127.0.0.1")))).as("B refuted at a higher incarnation")
                .containsExactly("127.0.0.1:ALIVE@1");
        assertThat(described(List.of(table.get("127.0.0.1")))).as("A cleared its suspicion")
                .containsExactly("127.0.0.1:ALIVE@1");
        assertThat(b.table().get(SELF)).as("the probe introduced A to B").isNotNull();

        // and it stays cleared: another probe changes nothing
        detector.tick(T0 + 2, n -> 0);
        assertThat(described(List.of(table.get("127.0.0.1")))).containsExactly("127.0.0.1:ALIVE@1");
    }

    @Test
    void aLaggingProberStillLearnsOfARefutationAfterItsGossipHasRunOut() throws Exception {
        // B was suspected and refuted long ago, and by now has stopped gossiping it: its queue is empty.
        RealNode b = realNode();
        b.table().merge(m("127.0.0.1", MemberState.SUSPECT, 0)); // about itself: refuted at incarnation 1
        drainAll(b.table());
        assertThat(b.table().drainGossip(10)).isEmpty();

        // A never heard, and still holds the suspicion
        table.merge(new Member(b.node(), MemberState.ALIVE, 0, 0));
        table.markSuspect(b.node().getName(), T0);
        SwimDetector detector = new SwimDetector(table, clientFor(table));
        detectors.add(detector);

        detector.tick(T0 + 1, n -> 0);

        // Nothing queued carried it. B's answer still names B as ALIVE at its current incarnation, which
        // is what lets A's merge rule clear the suspicion, whenever A happens to ask.
        assertThat(described(List.of(table.get("127.0.0.1")))).containsExactly("127.0.0.1:ALIVE@1");
        assertThat(b.table().get("127.0.0.1").getIncarnation()).as("A's stale SUSPECT@0 is no news to B").isEqualTo(1);
    }

    @Test
    void gossipOnAnAckCannotDowngradeWhatTheProberKnows() throws Exception {
        // gossip merged from an ack is subject to the same rule as any other: a lower incarnation is ignored
        Stub peer = stub();
        peer.responder = (path, body) -> ackJson(true, new MemberUpdate("127.0.0.1", peer.node.getPort(), MemberState.ALIVE, 0));
        peer.server.start();
        table.merge(new Member(peer.node, MemberState.ALIVE, 3, 0));

        assertThat(clientFor(table).ping(peer.node)).isTrue();

        assertThat(described(List.of(table.get("127.0.0.1")))).containsExactly("127.0.0.1:ALIVE@3");
    }
}
