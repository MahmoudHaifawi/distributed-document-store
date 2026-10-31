package com.database.atypon.Node.utils.swim;

import com.database.atypon.Node.model.Network;
import com.database.atypon.Node.model.Node;
import com.database.atypon.Node.utils.ring.HashRing;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The SWIM failure detector driven by hand: every tick is a direct call with an injected clock and an
 * injected random selector, and the peers are real {@link HttpServer} stubs, each bound to its own
 * loopback address so that every peer has a distinct node name. No timer ever fires on its own.
 */
class SwimDetectorTest {

    private static final String SELF = "Node0";
    private static final String BEARER = "Bearer swim-test-token";
    private static final long PING_TIMEOUT_MS = 250L;

    /** Where the detector's clock starts; far from the table's own clock so the injected one is provably used. */
    private static final long T0 = 50_000L;

    private static final String ACK = "{\"acked\":true}";
    private static final String NO_ACK = "{\"acked\":false}";

    private List<Node> savedNodes;
    private Node savedSelf;
    private HashRing savedRing;
    private Function<String, String> savedEnv;

    private final List<Stub> stubs = new ArrayList<>();
    private MembershipTable table;
    private SwimDetector detector;

    private record Received(String method, String path, String auth, String body) {
    }

    private record Reply(int status, String body, long delayMs) {
        static final Reply ACKED = new Reply(200, ACK, 0);
        static final Reply NOT_ACKED = new Reply(200, NO_ACK, 0);
        static final Reply FAILS = new Reply(500, "{\"error\":\"boom\"}", 0);

        static Reply slow(long delayMs) {
            return new Reply(200, ACK, delayMs);
        }
    }

    /** A peer that records every request and answers /swim/ping and /swim/ping-req with whatever the test set. */
    private static final class Stub {
        final HttpServer server;
        final Node node;
        final List<Received> received = Collections.synchronizedList(new ArrayList<>());
        volatile Reply onPing = Reply.ACKED;
        volatile Reply onPingReq = Reply.NOT_ACKED;

        Stub(String address) throws IOException {
            server = HttpServer.create(new InetSocketAddress(InetAddress.getByName(address), 0), 0);
            server.createContext("/", this::serve);
            server.start();
            // the peer is named by its loopback address: distinct per stub, and the URL reaches the stub
            node = new Node(address, String.valueOf(server.getAddress().getPort()));
        }

        private void serve(HttpExchange exchange) throws IOException {
            String path = exchange.getRequestURI().getPath();
            received.add(new Received(
                    exchange.getRequestMethod(),
                    path,
                    exchange.getRequestHeaders().getFirst("Authorization"),
                    new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8)));
            Reply reply = path.equals("/swim/ping") ? onPing : onPingReq;
            try {
                if (reply.delayMs() > 0) {
                    Thread.sleep(reply.delayMs());
                }
                byte[] bytes = reply.body().getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().add("Content-Type", "application/json");
                exchange.sendResponseHeaders(reply.status(), bytes.length);
                exchange.getResponseBody().write(bytes);
            } catch (IOException | InterruptedException e) {
                // the caller gave up on a deliberately slow reply
            } finally {
                exchange.close();
            }
        }

        List<Received> requestsTo(String path) {
            synchronized (received) {
                return received.stream().filter(r -> r.path().equals(path)).toList();
            }
        }

        List<Received> pings() {
            return requestsTo("/swim/ping");
        }

        List<Received> pingReqs() {
            return requestsTo("/swim/ping-req");
        }
    }

    @BeforeEach
    void formCluster() {
        savedNodes = Network.nodes;
        savedSelf = Network.self;
        savedRing = Network.ring;
        savedEnv = SwimConfig.env;

        Network.nodes = Collections.synchronizedList(new ArrayList<>());
        Network.self = new Node(SELF, "8080");
        Network.ring = null;

        buildDetector();
    }

    private void buildDetector() {
        table = new MembershipTable(new AtomicLong(1_000L)::get);
        table.initSelf(new Node(SELF, "8080"));
        SwimClient client = new SwimClient(table, PING_TIMEOUT_MS, () -> BEARER);
        detector = new SwimDetector(table, client);
    }

    @AfterEach
    void stopStubsAndRestoreMembership() {
        stubs.forEach(s -> s.server.stop(0));
        detector.close();
        SwimConfig.env = savedEnv;
        Network.nodes = savedNodes;
        Network.self = savedSelf;
        Network.ring = savedRing;
    }

    /** Starts a stub peer on the given loopback address and makes it a known, ALIVE member. */
    private Stub peer(String address) throws IOException {
        Stub stub = new Stub(address);
        stubs.add(stub);
        table.merge(new Member(stub.node, MemberState.ALIVE, 0, 0));
        return stub;
    }

    private static String name(Stub stub) {
        return stub.node.getName();
    }

    private MemberState stateOf(Stub stub) {
        return table.get(name(stub)).getState();
    }

    // ---- direct probe ---------------------------------------------------------------------------

    @Test
    void directAckKeepsAlive() throws IOException {
        Stub target = peer("127.0.0.1");
        Stub bystander = peer("127.0.0.2");
        HashRing ring = Network.ring;

        detector.tick(T0, n -> 0);

        assertThat(stateOf(target)).isEqualTo(MemberState.ALIVE);
        assertThat(stateOf(bystander)).isEqualTo(MemberState.ALIVE);
        assertThat(Network.ring).as("nothing changed, so the ring is not rebuilt").isSameAs(ring);

        // exactly one direct ping, authenticated as an internal call, naming this node as the sender
        assertThat(target.pings()).hasSize(1);
        Received ping = target.pings().get(0);
        assertThat(ping.method()).isEqualTo("POST");
        assertThat(ping.auth()).isEqualTo(BEARER);
        assertThat(new JSONObject(ping.body()).getString("from")).isEqualTo(SELF);

        // an acked ping needs no indirect probe and touches nobody else
        assertThat(target.pingReqs()).isEmpty();
        assertThat(bystander.received).isEmpty();
    }

    // ---- indirect probe -------------------------------------------------------------------------

    @Test
    void indirectAckCancelsSuspicion() throws IOException {
        Stub target = peer("127.0.0.1");
        Stub helper = peer("127.0.0.2");
        target.onPing = Reply.FAILS;      // the direct ping fails ...
        helper.onPingReq = Reply.ACKED;   // ... but the helper reaches the target and says so
        HashRing ring = Network.ring;

        detector.tick(T0, n -> 0);

        assertThat(stateOf(target)).as("one indirect ack is enough").isEqualTo(MemberState.ALIVE);
        assertThat(Network.ring).as("no membership change, so the ring is untouched").isSameAs(ring);

        assertThat(target.pings()).hasSize(1);
        assertThat(helper.pingReqs()).hasSize(1);
        Received request = helper.pingReqs().get(0);
        assertThat(request.method()).isEqualTo("POST");
        assertThat(request.auth()).isEqualTo(BEARER);
        JSONObject body = new JSONObject(request.body());
        assertThat(body.getString("from")).isEqualTo(SELF);
        assertThat(body.getJSONObject("target").getString("name")).isEqualTo(name(target));
        assertThat(body.getJSONObject("target").getString("port")).isEqualTo(target.node.getPort());
        assertThat(helper.pings()).as("the helper is asked to relay, never pinged itself").isEmpty();
    }

    @Test
    void noAckMarksSuspect() throws IOException {
        Stub target = peer("127.0.0.1");
        Stub helper = peer("127.0.0.2");
        target.onPing = Reply.FAILS;
        helper.onPingReq = Reply.NOT_ACKED; // the helper could not reach it either
        HashRing ring = Network.ring;

        detector.tick(T0, n -> 0);

        Member suspect = table.get(name(target));
        assertThat(suspect.getState()).isEqualTo(MemberState.SUSPECT);
        assertThat(suspect.getLastChange()).as("suspicion starts at the injected clock").isEqualTo(T0);
        assertThat(stateOf(helper)).isEqualTo(MemberState.ALIVE);
        assertThat(Network.ring).as("a suspect stays on the ring").isSameAs(ring);
        assertThat(table.liveNodes()).extracting(Node::getName).contains(name(target));
        assertThat(helper.pingReqs()).hasSize(1);
    }

    @Test
    void aLoneUnreachablePeerIsSuspectedWithNoOneToAskIndirectly() throws IOException {
        Stub target = peer("127.0.0.1");
        target.onPing = Reply.FAILS;

        detector.tick(T0, n -> 0);

        assertThat(stateOf(target)).isEqualTo(MemberState.SUSPECT);
    }

    @Test
    void indirectProbeAsksAtMostKOtherPeersNeverTheTargetOrSelf() throws IOException {
        SwimConfig.env = name -> name.equals("SWIM_INDIRECT_K") ? "2" : null;
        Stub target = peer("127.0.0.1");
        Stub helperA = peer("127.0.0.2");
        Stub helperB = peer("127.0.0.3");
        Stub helperC = peer("127.0.0.4");
        Stub helperD = peer("127.0.0.5");
        target.onPing = Reply.FAILS;

        detector.tick(T0, n -> 0);

        assertThat(stateOf(target)).isEqualTo(MemberState.SUSPECT);
        assertThat(target.pingReqs()).as("the target is never asked to probe itself").isEmpty();
        assertThat(helperA.pingReqs()).hasSize(1);
        assertThat(helperB.pingReqs()).hasSize(1);
        assertThat(helperC.pingReqs()).isEmpty();
        assertThat(helperD.pingReqs()).isEmpty();
    }

    // ---- failure handling -----------------------------------------------------------------------

    @Test
    void anUnreachablePeerCountsAsNoAckAndNeverThrows() throws IOException {
        Stub target = peer("127.0.0.1");
        target.server.stop(0); // connection refused

        detector.tick(T0, n -> 0);

        assertThat(stateOf(target)).isEqualTo(MemberState.SUSPECT);
    }

    @Test
    void aSlowPeerTimesOutAsNoAck() throws IOException {
        Stub target = peer("127.0.0.1");
        target.onPing = Reply.slow(PING_TIMEOUT_MS * 4);

        detector.tick(T0, n -> 0);

        assertThat(stateOf(target)).isEqualTo(MemberState.SUSPECT);
    }

    @Test
    void aSlowRelayedProbeStillCancelsSuspicionWithinItsOwnTimeout() throws IOException {
        // the helper has to ping the target before it can answer, so its own reply is allowed to take longer
        Stub target = peer("127.0.0.1");
        Stub helper = peer("127.0.0.2");
        target.onPing = Reply.FAILS;
        helper.onPingReq = new Reply(200, ACK, PING_TIMEOUT_MS + PING_TIMEOUT_MS / 2);

        detector.tick(T0, n -> 0);

        assertThat(stateOf(target)).isEqualTo(MemberState.ALIVE);
    }

    // ---- suspicion window -----------------------------------------------------------------------

    @Test
    void suspectTimesOutToDead() throws IOException {
        Stub target = peer("127.0.0.1");
        Stub bystander = peer("127.0.0.2");
        target.onPing = Reply.FAILS;
        bystander.onPingReq = Reply.NOT_ACKED;
        long timeout = SwimConfig.suspectTimeoutMs();

        detector.tick(T0, n -> 0);
        assertThat(stateOf(target)).isEqualTo(MemberState.SUSPECT);
        HashRing ringWhileSuspect = Network.ring;

        detector.tick(T0 + timeout - 1, n -> 0);
        assertThat(stateOf(target)).as("still inside the window").isEqualTo(MemberState.SUSPECT);
        assertThat(Network.ring).isSameAs(ringWhileSuspect);

        detector.tick(T0 + timeout, n -> 0);
        assertThat(stateOf(target)).isEqualTo(MemberState.DEAD);
        assertThat(table.liveNodes()).extracting(Node::getName).doesNotContain(name(target));
        assertThat(Network.nodes).extracting(Node::getName).doesNotContain(name(target));
        assertThat(Network.ring).as("the ring is rebuilt without the dead member").isNotSameAs(ringWhileSuspect);
        assertThat(stateOf(bystander)).isEqualTo(MemberState.ALIVE);
    }

    @Test
    void aDeadPeerIsNotProbedAgain() throws IOException {
        Stub target = peer("127.0.0.1");
        target.onPing = Reply.FAILS;
        long timeout = SwimConfig.suspectTimeoutMs();
        detector.tick(T0, n -> 0);
        detector.tick(T0 + timeout, n -> 0);
        assertThat(stateOf(target)).isEqualTo(MemberState.DEAD);
        int pingsSoFar = target.pings().size();

        detector.tick(T0 + timeout + 1_000, n -> {
            throw new AssertionError("nobody left to probe");
        });

        assertThat(target.pings()).hasSize(pingsSoFar);
        assertThat(stateOf(target)).isEqualTo(MemberState.DEAD);
    }

    // ---- selection ------------------------------------------------------------------------------

    @Test
    void tickWithNoPeersDoesNothing() {
        HashRing ring = Network.ring;
        assertThat(ring).isNotNull(); // the lone node's own ring

        detector.tick(T0, n -> {
            throw new AssertionError("there is no one to pick");
        });

        assertThat(table.snapshot()).hasSize(1);
        assertThat(table.get(SELF).getState()).isEqualTo(MemberState.ALIVE);
        assertThat(Network.ring).isSameAs(ring);
    }

    @Test
    void theInjectedSelectorChoosesAmongThePeersInAStableOrderAndNeverSelf() throws IOException {
        Stub first = peer("127.0.0.1");
        Stub second = peer("127.0.0.2");
        AtomicInteger bound = new AtomicInteger();

        detector.tick(T0, n -> {
            bound.set(n);
            return 1;
        });
        assertThat(bound.get()).as("two peers to choose from; this node is not one of them").isEqualTo(2);
        assertThat(first.pings()).isEmpty();
        assertThat(second.pings()).hasSize(1);

        detector.tick(T0 + 1, n -> 0);
        assertThat(first.pings()).hasSize(1);
        assertThat(second.pings()).hasSize(1);
    }

    @Test
    void aTickThatFailsStillSweepsExpiredSuspects() throws IOException {
        Stub target = peer("127.0.0.1");
        Stub other = peer("127.0.0.2");
        table.markSuspect(name(target), T0);
        long timeout = SwimConfig.suspectTimeoutMs();

        detector.tick(T0 + timeout, n -> {
            throw new IllegalStateException("selector blew up");
        });

        assertThat(stateOf(target)).as("the sweep runs even when probing cannot").isEqualTo(MemberState.DEAD);
        assertThat(stateOf(other)).isEqualTo(MemberState.ALIVE);
    }
}
