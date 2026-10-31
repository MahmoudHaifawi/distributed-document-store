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
import org.springframework.web.server.ResponseStatusException;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Joining a running cluster through a seed, leaving it gracefully, the node-role scheduler that drives the
 * detector, and the one mechanism that lets a partition re-merge when it heals.
 *
 * <p>Peers are real {@link HttpServer} stubs on loopback; where a test needs a peer that behaves like a
 * node, the stub fronts a second, real {@link MembershipTable} behind a real {@link SwimController}.
 * Every table here projects onto the one set of {@link Network} statics, so each test saves and restores
 * them and pins {@link SwimConfig#env}, so an exported SWIM_* variable cannot change what is expected.
 */
class JoinLeaveTest {

    private static final String BEARER = "Bearer join-leave-test-token";
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
        volatile int status = 200;
        volatile Responder responder = (path, body) -> "{\"acked\":true}";

        Stub(String host) throws IOException {
            server = HttpServer.create(new InetSocketAddress(InetAddress.getByName(host), 0), 0);
            node = new Node(host, String.valueOf(server.getAddress().getPort()));
            server.createContext("/", this::serve);
        }

        private void serve(HttpExchange exchange) throws IOException {
            String path = exchange.getRequestURI().getPath();
            String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            received.add(new Received(path, exchange.getRequestHeaders().getFirst("Authorization"), body));
            int code = status;
            String reply;
            try {
                reply = responder.reply(path, body);
            } catch (Exception e) {
                code = 500;
                reply = "{\"error\":\"" + e + "\"}";
            }
            byte[] bytes = reply.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(code, bytes.length);
            exchange.getResponseBody().write(bytes);
            exchange.close();
        }

        String address() {
            return node.getName() + ":" + node.getPort();
        }

        List<Received> to(String path) {
            synchronized (received) {
                return received.stream().filter(r -> r.path().equals(path)).toList();
            }
        }

        Received only(String path) {
            List<Received> matching = to(path);
            assertThat(matching).as("requests to %s", path).hasSize(1);
            return matching.get(0);
        }
    }

    @BeforeEach
    void isolateStatics() {
        savedNodes = Network.nodes;
        savedSelf = Network.self;
        savedRing = Network.ring;
        savedEnv = SwimConfig.env;

        Network.nodes = Collections.synchronizedList(new ArrayList<>());
        Network.self = null;
        Network.ring = null;
        SwimConfig.env = name -> null;

        clock = new AtomicLong(1_000L);
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

    private void env(String... keyValues) {
        Map<String, String> values = new HashMap<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            values.put(keyValues[i], keyValues[i + 1]);
        }
        SwimConfig.env = values::get;
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

    private MembershipTable newTable() {
        return new MembershipTable(clock::get);
    }

    private Stub stub() throws IOException {
        return stub("127.0.0.1");
    }

    /**
     * A stub listening on its own loopback address. The membership table keys members by name, and a stub is
     * named after its address, so a test that needs several distinct peers needs distinct addresses
     * (the whole of 127.0.0.0/8 is loopback).
     */
    private Stub stub(String loopbackAddress) throws IOException {
        Stub stub = new Stub(loopbackAddress);
        stubs.add(stub);
        return stub;
    }

    private SwimClient clientFor(MembershipTable t) {
        return new SwimClient(t, PING_TIMEOUT_MS, () -> BEARER);
    }

    private SwimBootstrap bootstrapFor(MembershipTable t) {
        return new SwimBootstrap(t, clientFor(t), "8080");
    }

    private SwimController controllerFor(MembershipTable t) {
        return new SwimController(clientFor(t), t);
    }

    private static void drainAll(MembershipTable t) {
        for (int i = 0; i < 1_000 && !t.drainGossip(1_000).isEmpty(); i++) {
            // each drain spends one retransmit of everything it returns, so this ends
        }
    }

    private static List<String> described(List<Member> members) {
        return members.stream()
                .map(x -> x.getNode().getName() + ":" + x.getState() + "@" + x.getIncarnation())
                .toList();
    }

    /** The gossip list of an answer as {@code name:STATE@incarnation}. */
    private static List<String> wire(List<MemberUpdate> gossip) {
        return gossip.stream().map(x -> x.getName() + ":" + x.getState() + "@" + x.getIncarnation()).toList();
    }

    private static String snapshotJson(MemberUpdate... members) throws IOException {
        return JSON.writeValueAsString(Arrays.asList(members));
    }

    /** A peer that is a whole node: its own table behind its own controller, reached over HTTP. */
    private record RealNode(Stub stub, MembershipTable table) {
        Node node() {
            return stub.node;
        }
    }

    private RealNode realNode() throws IOException {
        Stub stub = stub();
        MembershipTable tableB = newTable();
        tableB.initSelf(stub.node);
        SwimController controllerB = controllerFor(tableB);
        stub.responder = (path, body) -> switch (path) {
            case "/swim/ping" -> JSON.writeValueAsString(controllerB.ping(JSON.readValue(body, PingMessage.class)));
            case "/swim/join" -> JSON.writeValueAsString(controllerB.join(JSON.readValue(body, MemberUpdate.class)));
            case "/swim/leave" -> JSON.writeValueAsString(controllerB.leave(JSON.readValue(body, MemberUpdate.class)));
            default -> throw new IllegalStateException("unexpected " + path);
        };
        stub.server.start();
        return new RealNode(stub, tableB);
    }

    // ---- join ---------------------------------------------------------------------------------

    @Test
    void joinAdoptsSeedMembershipAndRebuildsRing() throws Exception {
        Stub seed = stub();
        seed.responder = (path, body) -> snapshotJson(
                new MemberUpdate("127.0.0.1", seed.node.getPort(), MemberState.ALIVE, 0),
                u("Node2", MemberState.ALIVE, 3),
                u("Gone", MemberState.DEAD, 2));
        seed.server.start();
        env("SEED_NODES", seed.address(), "NODE_NAME", "NodeA");
        MembershipTable table = newTable();

        boolean joined = bootstrapFor(table).join();

        assertThat(joined).isTrue();
        // what went out: this node, as itself, alive at its own incarnation, with the service token
        Received request = seed.only("/swim/join");
        assertThat(request.auth()).isEqualTo(BEARER);
        JsonNode hello = JSON.readTree(request.body());
        assertThat(hello.get("name").asText()).isEqualTo("NodeA");
        assertThat(hello.get("port").asText()).isEqualTo("8080");
        assertThat(hello.get("state").asText()).isEqualTo("ALIVE");
        assertThat(hello.get("incarnation").asInt()).isZero();

        // what came back is adopted whole, tombstones included, and this node is in it too
        assertThat(described(table.snapshot())).containsExactlyInAnyOrder(
                "NodeA:ALIVE@0", "127.0.0.1:ALIVE@0", "Node2:ALIVE@3", "Gone:DEAD@2");
        assertThat(Network.self.getName()).isEqualTo("NodeA");
        assertThat(Network.nodes).extracting(Node::getName).containsExactlyInAnyOrder("127.0.0.1", "Node2");
        assertThat(Network.ring.physicalNodeCount()).as("self + the two live members; the dead one is off the ring").isEqualTo(3);
        assertThat(Network.ring.preferenceList("some-schema", 3)).extracting(Node::getName)
                .containsExactlyInAnyOrder("NodeA", "127.0.0.1", "Node2");
    }

    @Test
    void joinWithNoReachableSeedStaysSingleNode() throws Exception {
        Stub refused = stub();
        refused.server.start();
        String closed = refused.address();
        refused.server.stop(0); // now nothing listens there
        Stub broken = stub();
        broken.status = 500;
        broken.server.start();
        env("SEED_NODES", closed + "," + broken.address(), "NODE_NAME", "NodeA");
        MembershipTable table = newTable();

        boolean joined = bootstrapFor(table).join();

        assertThat(joined).isFalse();
        assertThat(broken.to("/swim/join")).as("the seed that answered with an error was tried").hasSize(1);
        assertThat(described(table.snapshot())).containsExactly("NodeA:ALIVE@0");
        assertThat(Network.nodes).isEmpty();
        assertThat(Network.ring.physicalNodeCount()).isEqualTo(1);
    }

    @Test
    void joinWithNoSeedsConfiguredStaysSingleNode() {
        env("NODE_NAME", "NodeA");
        MembershipTable table = newTable();

        assertThat(bootstrapFor(table).join()).isFalse();

        assertThat(described(table.snapshot())).containsExactly("NodeA:ALIVE@0");
        assertThat(Network.ring.physicalNodeCount()).isEqualTo(1);
    }

    @Test
    void joinTriesSeedsInOrderUntilOneAnswers() throws Exception {
        Stub down = stub();
        down.status = 503;
        down.server.start();
        Stub up = stub();
        up.responder = (path, body) -> snapshotJson(new MemberUpdate("127.0.0.1", up.node.getPort(), MemberState.ALIVE, 0));
        up.server.start();
        Stub spare = stub();
        spare.server.start();
        env("SEED_NODES", down.address() + "," + up.address() + "," + spare.address(), "NODE_NAME", "NodeA");
        MembershipTable table = newTable();

        assertThat(bootstrapFor(table).join()).isTrue();

        assertThat(down.to("/swim/join")).hasSize(1);
        assertThat(up.to("/swim/join")).hasSize(1);
        assertThat(spare.received).as("once one seed has answered, the rest are left alone").isEmpty();
        assertThat(table.get("127.0.0.1").getState()).isEqualTo(MemberState.ALIVE);
    }

    @Test
    void joinSkipsASeedThatIsThisNode() throws Exception {
        // docker-compose style: every node lists the whole cluster as seeds, itself included
        Stub other = stub();
        other.responder = (path, body) -> snapshotJson(new MemberUpdate("127.0.0.1", other.node.getPort(), MemberState.ALIVE, 0));
        other.server.start();
        env("SEED_NODES", "NodeA:8080," + other.address(), "NODE_NAME", "NodeA");
        MembershipTable table = newTable();

        assertThat(bootstrapFor(table).join()).isTrue();

        assertThat(other.to("/swim/join")).as("a node does not join itself").hasSize(1);
        assertThat(table.get("127.0.0.1")).isNotNull();
    }

    @Test
    void joinMovesOnFromASeedThatLeadsBackToThisNode() throws Exception {
        // a seed address that is really this node under another name: its "cluster" is just the joiner
        Stub alias = stub();
        alias.responder = (path, body) -> snapshotJson(u("NodeA", MemberState.ALIVE, 0));
        alias.server.start();
        Stub real = stub("127.0.0.2");
        real.responder = (path, body) -> snapshotJson(
                new MemberUpdate("127.0.0.2", real.node.getPort(), MemberState.ALIVE, 0), u("NodeA", MemberState.ALIVE, 0));
        real.server.start();
        env("SEED_NODES", alias.address() + "," + real.address(), "NODE_NAME", "NodeA");
        MembershipTable table = newTable();

        assertThat(bootstrapFor(table).join()).isTrue();

        assertThat(alias.to("/swim/join")).hasSize(1);
        assertThat(real.to("/swim/join")).hasSize(1);
        assertThat(table.liveNodes()).extracting(Node::getName).containsExactlyInAnyOrder("NodeA", "127.0.0.2");
    }

    @Test
    void aNodeThatLeftCanComeBackUnderTheSameName() throws Exception {
        RealNode b = realNode();
        Network.self = b.node();
        b.table().merge(m("NodeA", MemberState.LEFT, 0));
        env("SEED_NODES", b.stub().address(), "NODE_NAME", "NodeA");
        MembershipTable tableA = newTable();

        assertThat(bootstrapFor(tableA).join()).isTrue();

        // the join answer says LEFT@0 about the joiner; a restart is not bound by the old process's goodbye
        assertThat(described(List.of(tableA.get("NodeA")))).containsExactly("NodeA:ALIVE@1");
        assertThat(clientFor(tableA).ping(b.node())).isTrue(); // its next probe carries ALIVE@1
        assertThat(described(List.of(b.table().get("NodeA")))).containsExactly("NodeA:ALIVE@1");
        assertThat(b.table().liveNodes()).extracting(Node::getName).contains("NodeA");
    }

    @Test
    void gossipDisabledSkipsJoin() throws Exception {
        Stub seed = stub();
        seed.server.start();
        env("GOSSIP_ENABLED", "false", "SEED_NODES", seed.address(), "NODE_NAME", "NodeA");
        MembershipTable table = newTable();
        SwimBootstrap bootstrap = bootstrapFor(table);

        assertThat(bootstrap.join()).isFalse();
        bootstrap.leave();

        assertThat(seed.received).as("no join and no leave goes out with gossip off").isEmpty();
        assertThat(table.snapshot()).as("the membership table is not even started").isEmpty();
        assertThat(Network.self).as("the legacy wiring still owns this node's identity").isNull();
        assertThat(Network.ring).isNull();
    }

    @Test
    void joinNeverFailsStartup() {
        env("SEED_NODES", "not a seed at all:::", "NODE_NAME", "NodeA");
        MembershipTable table = newTable();
        // a seed whose address is nonsense, and a client that blows up outright: both are a log line, not a crash
        SwimClient exploding = new SwimClient(table, PING_TIMEOUT_MS, () -> BEARER) {
            @Override
            public Optional<List<MemberUpdate>> join(Node seed, MemberUpdate self) {
                throw new IllegalStateException("boom");
            }
        };

        assertThat(bootstrapFor(table).join()).isFalse();
        SwimBootstrap bootstrap = new SwimBootstrap(table, exploding, "8080");
        assertThat(bootstrap.join()).isFalse();
        bootstrap.onReady(); // the listener Spring calls
        assertThat(table.get("NodeA").getState()).as("it still runs, on its own").isEqualTo(MemberState.ALIVE);
    }

    @Test
    void nodeIdentityComesFromNodeNameAndThePortTheServerListensOn() {
        env("NODE_NAME", "Node7");
        MembershipTable table = newTable();

        new SwimBootstrap(table, clientFor(table), "9091").join();

        assertThat(Network.self.getName()).isEqualTo("Node7");
        assertThat(Network.self.getPort()).isEqualTo("9091");
        assertThat(table.get("Node7").getNode().getPort()).isEqualTo("9091");
    }

    @Test
    void anIdentityAlreadyAssignedIsKeptWhenNoNodeNameIsConfigured() {
        Network.self = n("Assigned");
        MembershipTable table = newTable();

        bootstrapFor(table).join();

        assertThat(Network.self.getName()).isEqualTo("Assigned");
        assertThat(table.get("Assigned").getState()).isEqualTo(MemberState.ALIVE);
    }

    @Test
    void aNodeWithoutAnyNameStillStartsInsteadOfFailing() {
        // no NODE_NAME and no assigned identity: the host name stands in, whatever it is
        MembershipTable table = newTable();

        bootstrapFor(table).join();

        assertThat(Network.self).isNotNull();
        assertThat(table.get(Network.self.getName()).getState()).isEqualTo(MemberState.ALIVE);
    }

    @Test
    void aRestartedNodeJoinsAndRefutesItsOwnDeathCertificate() throws Exception {
        RealNode b = realNode();
        Network.self = b.node();
        // while this node was down B declared it dead at incarnation 2; the new process starts at 0
        b.table().merge(m("NodeA", MemberState.DEAD, 2));
        env("SEED_NODES", b.stub().address(), "NODE_NAME", "NodeA");
        MembershipTable tableA = newTable();

        assertThat(bootstrapFor(tableA).join()).isTrue();

        // the join answer is the whole table, DEAD(A,2) with it, however long ago that stopped being gossiped
        assertThat(described(List.of(tableA.get("NodeA")))).as("refuted at 3").containsExactly("NodeA:ALIVE@3");
        assertThat(wire(tableA.outgoingGossip(6))).contains("NodeA:ALIVE@3");
    }

    // ---- leave --------------------------------------------------------------------------------

    @Test
    void leaveMarksLeftAndDropsFromRing() throws Exception {
        RealNode b = realNode();
        Network.self = b.node(); // the statics are B's: this is the ring B routes by
        env("SEED_NODES", b.stub().address(), "NODE_NAME", "NodeA");
        MembershipTable tableA = newTable();
        SwimBootstrap a = bootstrapFor(tableA);
        assertThat(a.join()).isTrue();
        assertThat(b.table().liveNodes()).extracting(Node::getName).contains("NodeA");
        assertThat(Network.ring.physicalNodeCount()).as("A is on B's ring once it has joined").isEqualTo(2);

        a.leave();

        assertThat(described(List.of(b.table().get("NodeA")))).containsExactly("NodeA:LEFT@0");
        assertThat(b.table().liveNodes()).extracting(Node::getName).containsExactly("127.0.0.1");
        assertThat(Network.nodes).as("B routes nothing to A any more").isEmpty();
        assertThat(Network.ring.physicalNodeCount()).isEqualTo(1);
        // the departure is news B passes on, so the rest of the cluster hears it from B
        assertThat(wire(b.table().outgoingGossip(6))).contains("NodeA:LEFT@0");
    }

    @Test
    void leaveSendsTheLeftRecordItselfAndNeverMergesItLocally() throws Exception {
        Stub peer = stub();
        peer.server.start();
        env("NODE_NAME", "NodeA");
        MembershipTable table = newTable();
        SwimBootstrap a = bootstrapFor(table);
        a.join();
        table.merge(new Member(peer.node, MemberState.ALIVE, 0, 0));

        a.leave();

        Received leave = peer.only("/swim/leave");
        assertThat(leave.auth()).isEqualTo(BEARER);
        JsonNode body = JSON.readTree(leave.body());
        List<String> fields = new ArrayList<>();
        body.fieldNames().forEachRemaining(fields::add);
        assertThat(fields).as("the LEFT record and nothing riding along: no gossip, so no ALIVE(self)")
                .containsExactlyInAnyOrder("name", "port", "state", "incarnation");
        assertThat(body.get("name").asText()).isEqualTo("NodeA");
        assertThat(body.get("state").asText()).isEqualTo("LEFT");
        assertThat(body.get("incarnation").asInt()).isZero();
        // and the leaver did not LEFT itself: the table would have refuted that and bumped its incarnation
        assertThat(described(List.of(table.get("NodeA")))).containsExactly("NodeA:ALIVE@0");
        assertThat(peer.to("/swim/ping")).as("leaving is not a probe").isEmpty();
    }

    @Test
    void leaveCarriesTheIncarnationTheNodeHasReached() throws Exception {
        Stub peer = stub();
        peer.server.start();
        env("NODE_NAME", "NodeA");
        MembershipTable table = newTable();
        SwimBootstrap a = bootstrapFor(table);
        a.join();
        table.merge(new Member(peer.node, MemberState.ALIVE, 0, 0));
        table.merge(m("NodeA", MemberState.SUSPECT, 4)); // wrongly suspected once: refuted at 5

        a.leave();

        // LEFT at 5 beats ALIVE at 5; LEFT at 0 would be a stale claim every peer ignores
        assertThat(JSON.readTree(peer.only("/swim/leave").body()).get("incarnation").asInt()).isEqualTo(5);
    }

    @Test
    void leaveTellsAFewLivePeersAndSkipsTheDeparted() throws Exception {
        env("NODE_NAME", "NodeA");
        MembershipTable table = newTable();
        SwimBootstrap a = bootstrapFor(table);
        a.join();
        List<Stub> alive = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            Stub peer = stub("127.0.0." + (10 + i));
            peer.server.start();
            alive.add(peer);
            table.merge(new Member(peer.node, MemberState.ALIVE, 0, 0));
        }
        Stub dead = stub("127.0.0.20");
        dead.server.start();
        table.merge(new Member(dead.node, MemberState.DEAD, 1, 0));

        a.leave();

        long told = alive.stream().filter(p -> !p.to("/swim/leave").isEmpty()).count();
        assertThat(told).as("best effort to a few peers, not a broadcast").isEqualTo(SwimBootstrap.LEAVE_FANOUT);
        assertThat(dead.received).isEmpty();
    }

    @Test
    void leaveIsBestEffortAndNeverThrows() throws Exception {
        Stub gone = stub();
        gone.server.start();
        Node goneNode = gone.node;
        gone.server.stop(0);
        env("NODE_NAME", "NodeA");
        MembershipTable table = newTable();
        SwimBootstrap a = bootstrapFor(table);
        a.join();
        table.merge(new Member(goneNode, MemberState.ALIVE, 0, 0));

        a.leave();
        a.leave(); // shutdown hooks can fire twice

        assertThat(table.get("NodeA").getState()).isEqualTo(MemberState.ALIVE);
    }

    @Test
    void leaveBeforeAnyJoinDoesNothing() throws Exception {
        Stub peer = stub();
        peer.server.start();
        MembershipTable table = newTable();
        table.merge(new Member(peer.node, MemberState.ALIVE, 0, 0));

        bootstrapFor(table).leave();

        assertThat(peer.received).isEmpty();
    }

    @Test
    void aLeaveOfAnotherNodeIsRecordedButOneNamingTheReceiverIsRefuted() {
        // LEFT about another node is plain news; LEFT about the receiver is a claim the receiver refutes
        MembershipTable table = newTable();
        table.initSelf(n("NodeB"));
        SwimController controller = controllerFor(table);

        controller.leave(u("NodeA", MemberState.ALIVE, 0)); // the state in the body does not matter: it is a leave
        controller.leave(u("NodeB", MemberState.LEFT, 0));

        assertThat(described(List.of(table.get("NodeA")))).containsExactly("NodeA:LEFT@0");
        assertThat(described(List.of(table.get("NodeB")))).containsExactly("NodeB:ALIVE@1");
    }

    @Test
    void joinAndLeaveRejectABodyThatNamesNobody() {
        MembershipTable table = newTable();
        SwimController controller = controllerFor(table);

        assertThatThrownBy(() -> controller.join(new MemberUpdate("", "8080", MemberState.ALIVE, 0)))
                .isInstanceOf(ResponseStatusException.class).hasMessageContaining("400");
        assertThatThrownBy(() -> controller.join(null)).isInstanceOf(ResponseStatusException.class);
        assertThatThrownBy(() -> controller.leave(new MemberUpdate("NodeA", null, MemberState.LEFT, 0)))
                .isInstanceOf(ResponseStatusException.class).hasMessageContaining("400");
        assertThatThrownBy(() -> controller.leave(null)).isInstanceOf(ResponseStatusException.class);
        assertThat(table.snapshot()).isEmpty();
    }

    @Test
    void joinMergesTheJoinerAndPassesItOn() {
        MembershipTable table = newTable();
        table.initSelf(n("Seed"));
        drainAll(table);
        SwimController controller = controllerFor(table);

        List<MemberUpdate> view = controller.join(u("Newcomer", MemberState.ALIVE, 0));

        assertThat(wire(view)).containsExactlyInAnyOrder("Seed:ALIVE@0", "Newcomer:ALIVE@0");
        assertThat(table.liveNodes()).extracting(Node::getName).containsExactly("Seed", "Newcomer");
        // its arrival spreads by gossip, not by a broadcast from the seed
        assertThat(wire(table.outgoingGossip(6))).contains("Newcomer:ALIVE@0");
    }

    @Test
    void joinAnswerIncludesTombstones() {
        MembershipTable table = newTable();
        table.initSelf(n("Seed"));
        table.merge(m("Old", MemberState.DEAD, 3));
        table.merge(m("Left", MemberState.LEFT, 1));
        drainAll(table);

        List<MemberUpdate> view = controllerFor(table).join(u("Newcomer", MemberState.ALIVE, 0));

        assertThat(wire(view)).contains("Old:DEAD@3", "Left:LEFT@1");
    }

    // ---- partition heal -----------------------------------------------------------------------

    @Test
    void partitionHealReconvergesViaRefutation() throws Exception {
        RealNode b = realNode();
        Node a = n("NodeA");
        Network.self = a; // A is the prober here, so the probes say "from NodeA"
        MembershipTable tableA = newTable();
        tableA.initSelf(a);
        tableA.merge(new Member(b.node(), MemberState.ALIVE, 0, 0));
        SwimClient clientA = clientFor(tableA);

        // The partition lasted longer than the suspicion timeout, so B declared A dead at incarnation 0,
        // and has long since stopped gossiping it: nothing will announce it again, and B does not probe the dead.
        b.table().merge(new Member(a, MemberState.ALIVE, 0, 0));
        b.table().markSuspect("NodeA", T0);
        b.table().promoteExpiredSuspectsToDead(T0 + SwimConfig.suspectTimeoutMs());
        drainAll(b.table());
        assertThat(described(List.of(b.table().get("NodeA")))).containsExactly("NodeA:DEAD@0");
        assertThat(b.table().drainGossip(10)).as("the DEAD update has run out of sends").isEmpty();

        // The partition heals and A, which never learnt it was written off, probes B.
        assertThat(clientA.ping(b.node())).isTrue();

        // B's answer carried what B believes of the sender, DEAD at 0, and A refuted it.
        assertThat(described(List.of(tableA.get("NodeA")))).containsExactly("NodeA:ALIVE@1");
        assertThat(b.table().get("NodeA").getState()).as("B has not heard the refutation yet").isEqualTo(MemberState.DEAD);

        // One more exchange: this probe carries A's ALIVE at 1, which beats DEAD at 0.
        Network.ring = null; // only B putting A back can publish a ring now
        assertThat(clientA.ping(b.node())).isTrue();

        assertThat(described(List.of(b.table().get("NodeA")))).containsExactly("NodeA:ALIVE@1");
        assertThat(b.table().liveNodes()).extracting(Node::getName).contains("NodeA");
        assertThat(Network.ring).as("B rebuilt its ring when A came back").isNotNull();
        assertThat(Network.ring.preferenceList("some-schema", 2)).extracting(Node::getName)
                .containsExactlyInAnyOrder("NodeA", "127.0.0.1");
    }

    @Test
    void aSuspectIsToldItIsSuspectedWhenItProbes() {
        MembershipTable table = newTable();
        table.initSelf(n("NodeB"));
        table.merge(m("NodeA", MemberState.ALIVE, 2));
        table.markSuspect("NodeA", T0);
        drainAll(table); // the suspicion has been gossiped out and its budget is spent

        AckMessage ack = controllerFor(table).ping(new PingMessage("NodeA", List.of(u("NodeA", MemberState.ALIVE, 2))));

        assertThat(wire(ack.getGossip())).contains("NodeA:SUSPECT@2", "NodeB:ALIVE@0");
    }

    @Test
    void theBeliefAboutTheSenderRidesOnAPingReqAnswerToo() {
        MembershipTable table = newTable();
        table.initSelf(n("NodeB"));
        table.merge(m("NodeA", MemberState.DEAD, 4));
        drainAll(table);

        // the target is unreachable, so the answer is "not acked", and it still tells the sender what B thinks
        AckMessage ack = controllerFor(table).pingReq(new PingReqMessage("NodeA", new Node("127.0.0.1", "1"), List.of()));

        assertThat(ack.isAcked()).isFalse();
        assertThat(wire(ack.getGossip())).contains("NodeA:DEAD@4");
    }

    @Test
    void theBeliefAboutTheJoinerRidesOnTheJoinAnswer() {
        MembershipTable table = newTable();
        table.initSelf(n("NodeB"));
        table.merge(m("NodeA", MemberState.DEAD, 4));
        drainAll(table);

        List<MemberUpdate> view = controllerFor(table).join(u("NodeA", MemberState.ALIVE, 0));

        assertThat(wire(view)).contains("NodeA:DEAD@4");
    }

    @Test
    void noBeliefIsSentAboutASenderThatIsAliveLeftOrUnknown() {
        MembershipTable table = newTable();
        table.initSelf(n("NodeB"));
        table.merge(m("Fine", MemberState.ALIVE, 1));
        table.merge(m("Gone", MemberState.LEFT, 3));
        drainAll(table);
        SwimController controller = controllerFor(table);

        // a LEFT sender must not be told so: it would refute it and undo its own departure
        for (String sender : Arrays.asList("Fine", "Gone", "Stranger", "", null)) {
            AckMessage ack = controller.ping(new PingMessage(sender, List.of()));
            assertThat(wire(ack.getGossip())).as("answer to %s", sender).containsExactly("NodeB:ALIVE@0");
        }
        assertThat(wire(controller.ping(null).getGossip())).containsExactly("NodeB:ALIVE@0");
    }

    @Test
    void aSenderThatProvesItselfAliveIsNotToldItIsDead() {
        MembershipTable table = newTable();
        table.initSelf(n("NodeB"));
        table.merge(m("NodeA", MemberState.DEAD, 4));
        drainAll(table);

        // the ping itself carries ALIVE at 5: that is merged first, so there is no stale belief left to send
        AckMessage ack = controllerFor(table).ping(new PingMessage("NodeA", List.of(u("NodeA", MemberState.ALIVE, 5))));

        assertThat(described(List.of(table.get("NodeA")))).containsExactly("NodeA:ALIVE@5");
        assertThat(wire(ack.getGossip())).doesNotContain("NodeA:DEAD@4");
    }

    @Test
    void theBeliefAboutTheSenderStillRespectsTheGossipCap() {
        env("SWIM_GOSSIP_MAX", "3");
        MembershipTable table = newTable();
        table.initSelf(n("NodeB"));
        table.merge(m("NodeA", MemberState.DEAD, 4));
        for (int i = 0; i < 8; i++) {
            table.merge(m("Q" + i, MemberState.ALIVE, 0));
        }

        AckMessage ack = controllerFor(table).ping(new PingMessage("NodeA", List.of()));

        assertThat(wire(ack.getGossip())).hasSizeLessThanOrEqualTo(3).contains("NodeA:DEAD@4", "NodeB:ALIVE@0");
    }

    // ---- the node-role scheduler --------------------------------------------------------------

    private SwimScheduler schedulerFor(MembershipTable t) {
        SwimClient client = clientFor(t);
        SwimDetector detector = new SwimDetector(t, client);
        detectors.add(detector);
        return new SwimScheduler(detector, t, client);
    }

    @Test
    void schedulerTickRunsTheDetector() throws Exception {
        Stub peer = stub();
        peer.server.start();
        Network.self = n("NodeA");
        MembershipTable table = newTable();
        table.initSelf(n("NodeA"));
        table.merge(new Member(peer.node, MemberState.ALIVE, 0, 0));

        schedulerFor(table).tick(T0, bound -> 0);

        assertThat(peer.to("/swim/ping")).as("one period, one probe").hasSize(1);
    }

    @Test
    void schedulerTickWithNoPeersDoesNothing() {
        Network.self = n("NodeA");
        MembershipTable table = newTable();
        table.initSelf(n("NodeA"));
        SwimScheduler scheduler = schedulerFor(table);

        for (int i = 0; i < 20; i++) {
            scheduler.tick(T0 + i, bound -> 0);
        }

        assertThat(described(table.snapshot())).containsExactly("NodeA:ALIVE@0");
    }

    @Test
    void schedulerDoesNothingWithGossipOff() throws Exception {
        env("GOSSIP_ENABLED", "false");
        Stub peer = stub();
        peer.server.start();
        Network.self = n("NodeA");
        MembershipTable table = newTable();
        table.initSelf(n("NodeA"));
        table.merge(new Member(peer.node, MemberState.ALIVE, 0, 0));

        SwimScheduler scheduler = schedulerFor(table);
        for (int i = 0; i < 10; i++) {
            scheduler.tick(T0 + i, bound -> 0);
        }

        assertThat(peer.received).isEmpty();
    }

    @Test
    void schedulerTickWithTheRealClockAndRandomnessIsSafe() {
        Network.self = n("NodeA");
        MembershipTable table = newTable();
        table.initSelf(n("NodeA"));

        schedulerFor(table).tick(); // the entry point Spring calls

        assertThat(table.snapshot()).hasSize(1);
    }

    @Test
    void schedulerPingsOneDeadMemberEveryFewPeriodsButNeverADepartedOne() throws Exception {
        Stub dead = stub();
        dead.server.start();
        Stub left = stub("127.0.0.2");
        left.server.start();
        Network.self = n("NodeA");
        MembershipTable table = newTable();
        table.initSelf(n("NodeA"));
        table.merge(new Member(dead.node, MemberState.DEAD, 1, 0));
        table.merge(new Member(left.node, MemberState.LEFT, 1, 0));
        SwimScheduler scheduler = schedulerFor(table);

        for (int i = 1; i < SwimScheduler.HEAL_EVERY; i++) {
            scheduler.tick(T0 + i, bound -> 0);
        }
        assertThat(dead.received).as("the detector alone never probes the dead").isEmpty();

        scheduler.tick(T0 + SwimScheduler.HEAL_EVERY, bound -> 0);
        assertThat(dead.to("/swim/ping")).hasSize(1);

        for (int i = 1; i <= SwimScheduler.HEAL_EVERY; i++) {
            scheduler.tick(T0 + 100 + i, bound -> 0);
        }
        assertThat(dead.to("/swim/ping")).hasSize(2);
        assertThat(left.received).as("a node that left on purpose is left alone").isEmpty();
    }

    @Test
    void aClusterThatWroteEachOtherOffHealsOnceTheDeadAreProbed() throws Exception {
        // Both sides of a long partition hold the other as DEAD@0, nobody gossips it any more and the
        // detector probes only the living: the situation that used to be permanent.
        RealNode b = realNode();
        Node a = n("NodeA");
        Network.self = a;
        MembershipTable tableA = newTable();
        tableA.initSelf(a);
        tableA.merge(new Member(b.node(), MemberState.DEAD, 0, 0));
        b.table().merge(new Member(a, MemberState.DEAD, 0, 0));
        drainAll(tableA);
        drainAll(b.table());
        SwimController controllerA = controllerFor(tableA);
        SwimScheduler schedulerA = schedulerFor(tableA);

        // A's scheduler gets to its heal probe: B tells A it is written off, and A refutes
        for (int i = 1; i <= SwimScheduler.HEAL_EVERY; i++) {
            schedulerA.tick(T0 + i, bound -> 0);
        }
        assertThat(described(List.of(tableA.get("NodeA")))).containsExactly("NodeA:ALIVE@1");

        // A's next heal probe carries that refutation: B takes A back
        for (int i = 1; i <= SwimScheduler.HEAL_EVERY; i++) {
            schedulerA.tick(T0 + 100 + i, bound -> 0);
        }
        assertThat(described(List.of(b.table().get("NodeA")))).containsExactly("NodeA:ALIVE@1");

        // B now has a live peer to probe. Its ping reaches A, A tells B it is held DEAD, and B refutes too
        // (done by hand: Network.self is one static, so a second node cannot probe from this JVM).
        AckMessage answer = controllerA.ping(new PingMessage(b.node().getName(), b.table().outgoingGossip(6)));
        b.table().mergeGossip(answer.getGossip());
        assertThat(described(List.of(b.table().get("127.0.0.1")))).containsExactly("127.0.0.1:ALIVE@1");

        // and the next heal probe from A hears B's ALIVE at 1 and writes B back in
        for (int i = 1; i <= SwimScheduler.HEAL_EVERY; i++) {
            schedulerA.tick(T0 + 200 + i, bound -> 0);
        }
        assertThat(described(List.of(tableA.get("127.0.0.1")))).containsExactly("127.0.0.1:ALIVE@1");
        assertThat(tableA.liveNodes()).extracting(Node::getName).containsExactlyInAnyOrder("NodeA", "127.0.0.1");
    }
}
