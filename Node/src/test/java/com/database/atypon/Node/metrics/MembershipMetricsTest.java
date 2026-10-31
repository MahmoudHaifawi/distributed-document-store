package com.database.atypon.Node.metrics;

import com.database.atypon.Node.model.Network;
import com.database.atypon.Node.model.Node;
import com.database.atypon.Node.operations.write.WriteOperation;
import com.database.atypon.Node.services.index.IndexManager;
import com.database.atypon.Node.utils.ring.HashRing;
import com.database.atypon.Node.utils.swim.HandoffService;
import com.database.atypon.Node.utils.swim.Member;
import com.database.atypon.Node.utils.swim.MemberState;
import com.database.atypon.Node.utils.swim.MembershipTable;
import com.database.atypon.Node.utils.swim.SwimClient;
import com.database.atypon.Node.utils.swim.SwimConfig;
import com.database.atypon.Node.utils.swim.SwimDetector;
import com.database.atypon.Node.utils.swim.SwimScheduler;
import com.database.atypon.Node.utils.swim.dto.MemberUpdate;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.distribution.DistributionStatisticConfig;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.ToDoubleFunction;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * M12: the membership gauges and the SWIM / handoff counters. The real {@link MembershipTable},
 * {@link SwimDetector}, {@link SwimScheduler} and {@link HandoffService} run against a
 * {@link SimpleMeterRegistry}, and the meters are read back by their catalogue names.
 *
 * <p>Probing is scripted (a {@link SwimClient} whose calls answer what the test says) except in the one test that
 * drives a real client against a stub peer; the handoff pulls from real {@link HttpServer} stubs. Both of those
 * make authenticated node-to-node calls, and {@code ServiceTokens} is a static bridge to whichever Spring context was
 * created last, so this class shares the application context (and JWT secret) of the other integration tests
 * instead of creating a context of its own that would repoint the bridge. That context has no
 * {@code MeterRegistry} bean, which is the situation {@link DbMetrics} must boot in.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestPropertySource(properties = "app.jwt.secret=dev-secret-change-me-please-at-least-32-bytes-long-000")
class MembershipMetricsTest {

    private static final String SELF = "Node0";
    private static final long T0 = 50_000L;
    private static final String DB = "m12membership";
    private static final String SCHEMA = "users";
    private static final String KEY = DB + "." + SCHEMA;

    private final Path dbDir = Paths.get("./data", DB);
    private final Path records = dbDir.resolve("users-records");

    private List<Node> savedNodes;
    private Node savedSelf;
    private HashRing savedRing;

    private SimpleMeterRegistry registry;
    private DbMetrics metrics;
    private MembershipTable table;
    private SwimDetector detector;
    private final List<Peer> peers = new ArrayList<>();
    private final List<HandoffService> handoffs = new ArrayList<>();

    @BeforeEach
    void aLoneNodeWithAFreshRegistry() {
        savedNodes = Network.nodes;
        savedSelf = Network.self;
        savedRing = Network.ring;
        Network.nodes = Collections.synchronizedList(new ArrayList<>());
        Network.self = new Node(SELF, "8080");
        Network.ring = null;

        registry = new SimpleMeterRegistry();
        metrics = new DbMetrics(registry);
        table = new MembershipTable(metrics);
        table.initSelf(new Node(SELF, "8080"));
    }

    @AfterEach
    void restoreNetworkAndCleanUp() throws Exception {
        peers.forEach(p -> p.server.stop(0));
        handoffs.forEach(HandoffService::stop);
        if (detector != null) {
            detector.close();
        }
        deleteTree(dbDir);
        Network.nodes = savedNodes;
        Network.self = savedSelf;
        Network.ring = savedRing;
    }

    // ---- helpers -----------------------------------------------------------------------------------------

    private static Node node(String name) {
        return new Node(name, "8080");
    }

    private void alive(String... names) {
        for (String name : names) {
            table.merge(new Member(node(name), MemberState.ALIVE, 0, 0L));
        }
    }

    private double gauge(String name, String... tags) {
        return gauge(registry, name, tags);
    }

    private static double gauge(MeterRegistry from, String name, String... tags) {
        Gauge gauge = from.find(name).tags(tags).gauge();
        assertThat(gauge).as("gauge %s %s", name, List.of(tags)).isNotNull();
        return gauge.value();
    }

    private double members(String state) {
        return gauge("db_swim_members", "state", state);
    }

    /** The sum over every counter of that name carrying the given tags (all of them when none are given). */
    private double count(String name, String... tags) {
        return count(registry, name, tags);
    }

    private static double count(MeterRegistry from, String name, String... tags) {
        return from.find(name).tags(tags).counters().stream().mapToDouble(Counter::count).sum();
    }

    /** A failure detector whose client answers what the test scripted, and records who it was asked to reach. */
    private static final class ScriptedClient extends SwimClient {
        final List<String> calls = Collections.synchronizedList(new ArrayList<>());
        volatile boolean pingAcks = true;
        volatile boolean pingReqAcks = false;

        ScriptedClient(MembershipTable table) {
            super(table);
        }

        @Override
        public boolean ping(Node target) {
            calls.add("ping " + target.getName());
            return pingAcks;
        }

        @Override
        public boolean pingReq(Node via, Node target) {
            calls.add("ping-req " + via.getName() + " -> " + target.getName());
            return pingReqAcks;
        }
    }

    private ScriptedClient scriptedDetector(DbMetrics m, MembershipTable t) {
        ScriptedClient client = new ScriptedClient(t);
        detector = new SwimDetector(t, client, m);
        return client;
    }

    // ---- gauges: db_ring_size and db_swim_members{state} ---------------------------------------------------

    @Test
    void ringSizeAndAliveCountReportTheLiveMembers() {
        assertThat(gauge("db_ring_size")).as("only this node").isEqualTo(1);
        assertThat(members("alive")).isEqualTo(1);

        alive("Node1", "Node2");

        assertThat(gauge("db_ring_size")).isEqualTo(3);
        assertThat(members("alive")).isEqualTo(3);
        assertThat(members("suspect")).isZero();
        assertThat(members("dead")).isZero();
    }

    @Test
    void beforeAnyNodeHasJoinedTheGaugesReadZero() {
        Network.ring = null;
        SimpleMeterRegistry fresh = new SimpleMeterRegistry();
        MembershipTable empty = new MembershipTable(new DbMetrics(fresh));

        assertThat(empty.snapshot()).isEmpty();
        assertThat(gauge(fresh, "db_ring_size")).isZero();
        assertThat(gauge(fresh, "db_swim_members", "state", "alive")).isZero();
        assertThat(gauge(fresh, "db_swim_members", "state", "suspect")).isZero();
        assertThat(gauge(fresh, "db_swim_members", "state", "dead")).isZero();
    }

    @Test
    void theGaugesFollowAMemberThroughSuspectAndDead() {
        alive("Node1", "Node2");

        table.markSuspect("Node1", T0);
        assertThat(members("alive")).isEqualTo(2);
        assertThat(members("suspect")).isEqualTo(1);
        assertThat(members("dead")).isZero();
        assertThat(gauge("db_ring_size")).as("a suspect is still on the ring").isEqualTo(3);

        table.promoteExpiredSuspectsToDead(T0 + SwimConfig.suspectTimeoutMs());
        assertThat(members("alive")).isEqualTo(2);
        assertThat(members("suspect")).isZero();
        assertThat(members("dead")).isEqualTo(1);
        assertThat(gauge("db_ring_size")).as("a dead member leaves the ring").isEqualTo(2);
    }

    @Test
    void aMemberThatRefutesItsSuspicionCountsAsAliveAgain() {
        alive("Node1");
        table.markSuspect("Node1", T0);
        assertThat(members("suspect")).isEqualTo(1);

        table.merge(new Member(node("Node1"), MemberState.ALIVE, 1, 0L)); // the refutation: a higher incarnation

        assertThat(members("suspect")).isZero();
        assertThat(members("alive")).isEqualTo(2);
        assertThat(gauge("db_ring_size")).isEqualTo(2);
    }

    @Test
    void aMemberThatLeftIsOffTheRingAndInNoneOfTheStateGauges() {
        alive("Node1", "Node2");

        table.merge(new Member(node("Node2"), MemberState.LEFT, 0, 0L));

        assertThat(gauge("db_ring_size")).isEqualTo(2);
        assertThat(members("alive")).isEqualTo(2);
        assertThat(members("suspect")).isZero();
        assertThat(members("dead")).isZero();
    }

    @Test
    void theGaugesAreRegisteredOnceAtStartupAndReadLazilyOnScrapeNeverOnTheWritePath() {
        AtomicInteger snapshots = new AtomicInteger();
        registry = new SimpleMeterRegistry();
        MembershipTable counting = new MembershipTable(new DbMetrics(registry)) {
            @Override
            public List<Member> snapshot() {
                snapshots.incrementAndGet();
                return super.snapshot();
            }
        };
        assertThat(registry.find("db_ring_size").meters()).hasSize(1);
        assertThat(registry.find("db_swim_members").meters()).hasSize(3);

        counting.initSelf(node(SELF));
        counting.merge(new Member(node("Node1"), MemberState.ALIVE, 0, 0L));
        counting.markSuspect("Node1", T0);
        counting.promoteExpiredSuspectsToDead(T0 + SwimConfig.suspectTimeoutMs());
        counting.outgoingGossip(6);
        counting.mergeGossip(List.of(new MemberUpdate("Node2", "8080", MemberState.ALIVE, 0)));
        assertThat(snapshots.get()).as("no gauge is recomputed by a table update").isZero();
        assertThat(registry.find("db_ring_size").meters()).hasSize(1);
        assertThat(registry.find("db_swim_members").meters()).hasSize(3);

        assertThat(members("alive")).isEqualTo(2);
        assertThat(members("dead")).isEqualTo(1);
        assertThat(snapshots.get()).as("the scrape is what reads the table").isPositive();
    }

    @Test
    void aGaugeSourceNothingElseHoldsSurvivesGarbageCollection() {
        SimpleMeterRegistry fresh = new SimpleMeterRegistry();
        int[] seven = {7};
        // capturing lambdas that only the registry knows about: a registry holds a gauge's source weakly by default
        new DbMetrics(fresh).bindMembership(() -> seven[0], state -> seven[0] + state.ordinal());

        for (int i = 0; i < 3; i++) {
            System.gc();
        }

        assertThat(gauge(fresh, "db_ring_size")).as("a weakly held source would have gone to NaN").isEqualTo(7);
        assertThat(gauge(fresh, "db_swim_members", "state", "dead")).isEqualTo(9);
    }

    @Test
    void theMembershipGaugesStillReadAfterGarbageCollection() {
        alive("Node1");

        for (int i = 0; i < 3; i++) {
            System.gc();
        }

        assertThat(gauge("db_ring_size")).isEqualTo(2);
        assertThat(members("alive")).isEqualTo(2);
    }

    // ---- db_swim_probes_total{kind} ------------------------------------------------------------------------

    @Test
    void aDirectProbeCountsOneDirectProbe() {
        alive("Node1", "Node2");
        ScriptedClient client = scriptedDetector(metrics, table);
        client.pingAcks = true;

        detector.tick(T0, n -> 0);

        assertThat(client.calls).containsExactly("ping Node1");
        assertThat(count("db_swim_probes_total", "kind", "direct")).isEqualTo(1);
        assertThat(count("db_swim_probes_total", "kind", "indirect")).isZero();

        detector.tick(T0 + 1, n -> 0);
        assertThat(count("db_swim_probes_total", "kind", "direct")).as("one per period").isEqualTo(2);
    }

    @Test
    void anUnackedDirectProbeIsFollowedByOneIndirectProbePerHelper() {
        alive("Node1", "Node2", "Node3");
        ScriptedClient client = scriptedDetector(metrics, table);
        client.pingAcks = false;
        client.pingReqAcks = false;

        detector.tick(T0, n -> 0);

        assertThat(client.calls).containsExactlyInAnyOrder("ping Node1", "ping-req Node2 -> Node1", "ping-req Node3 -> Node1");
        assertThat(count("db_swim_probes_total", "kind", "direct")).isEqualTo(1);
        assertThat(count("db_swim_probes_total", "kind", "indirect")).isEqualTo(2);
        assertThat(members("suspect")).as("nobody reached it, so it is suspected").isEqualTo(1);
    }

    @Test
    void aTickWithNobodyToProbeCountsNothing() {
        scriptedDetector(metrics, table);

        detector.tick(T0, n -> {
            throw new AssertionError("there is no one to pick");
        });

        assertThat(count("db_swim_probes_total")).isZero();
    }

    @Test
    void theHealProbeOfADeadMemberIsADirectProbe() {
        alive("Node1");
        table.merge(new Member(node("Node1"), MemberState.DEAD, 0, 0L));
        ScriptedClient client = scriptedDetector(metrics, table);
        SwimScheduler scheduler = new SwimScheduler(detector, table, client, metrics);

        int periods = 0;
        while (client.calls.isEmpty() && periods++ < 50) {
            scheduler.tick(); // with nobody alive to probe, only the periodic heal period pings anyone: a dead member
        }

        assertThat(client.calls).containsExactly("ping Node1");
        assertThat(count("db_swim_probes_total", "kind", "direct")).isEqualTo(1);
        assertThat(count("db_swim_probes_total", "kind", "indirect")).isZero();
    }

    // ---- db_swim_gossip_total{direction} -------------------------------------------------------------------

    @Test
    void gossipHandedToAnOutboundMessageCountsOneSentPerMessage() {
        alive("Node1", "Node2");

        List<MemberUpdate> first = table.outgoingGossip(6);
        table.outgoingGossip(6);

        assertThat(first).as("this node's own ALIVE always goes out").isNotEmpty();
        assertThat(count("db_swim_gossip_total", "direction", "sent")).isEqualTo(2);
        assertThat(count("db_swim_gossip_total", "direction", "received")).isZero();
    }

    @Test
    void anAnswerThatCorrectsADeadSenderIsOneSentMessageNotTwo() {
        alive("Node1");
        table.merge(new Member(node("Node1"), MemberState.DEAD, 0, 0L));

        List<MemberUpdate> answer = table.replyGossip(6, "Node1");

        assertThat(answer).extracting(MemberUpdate::getName).contains("Node1");
        assertThat(count("db_swim_gossip_total", "direction", "sent")).isEqualTo(1);
    }

    @Test
    void anOutboundMessageThatCarriesNoGossipCountsNothing() {
        table.outgoingGossip(0);
        table.replyGossip(0, "Node1");

        assertThat(count("db_swim_gossip_total")).isZero();
    }

    @Test
    void inboundGossipThatIsMergedCountsOneReceivedPerMessage() {
        table.mergeGossip(List.of(
                new MemberUpdate("Node1", "8080", MemberState.ALIVE, 0),
                new MemberUpdate("Node2", "8080", MemberState.ALIVE, 0)));
        table.mergeGossip(List.of(new MemberUpdate("Node1", "8080", MemberState.SUSPECT, 0)));

        assertThat(count("db_swim_gossip_total", "direction", "received")).as("per message, not per entry").isEqualTo(2);
        assertThat(count("db_swim_gossip_total", "direction", "sent")).isZero();
        assertThat(gauge("db_ring_size")).isEqualTo(3);
    }

    @Test
    void nullAndEmptyInboundGossipCountNothing() {
        table.mergeGossip(null);
        table.mergeGossip(List.of());

        assertThat(count("db_swim_gossip_total")).isZero();
    }

    @Test
    void aRealProbeCarriesGossipOutAndMergesTheGossipOfItsAck() throws Exception {
        Peer stranger = peer("127.0.0.1");
        table.merge(new Member(stranger.node, MemberState.ALIVE, 0, 0L));
        stranger.pingReply = new JSONObject().put("acked", true).put("gossip", new JSONArray().put(
                new JSONObject().put("name", "Newcomer").put("port", "8080").put("state", "ALIVE").put("incarnation", 0))).toString();
        detector = new SwimDetector(table, new SwimClient(table), metrics);

        detector.tick(T0, n -> 0);

        assertThat(stranger.pings.get()).isEqualTo(1);
        assertThat(count("db_swim_probes_total", "kind", "direct")).isEqualTo(1);
        assertThat(count("db_swim_gossip_total", "direction", "sent")).isEqualTo(1);
        assertThat(count("db_swim_gossip_total", "direction", "received")).isEqualTo(1);
        assertThat(table.get("Newcomer")).as("the ack's gossip was merged").isNotNull();
        assertThat(gauge("db_ring_size")).isEqualTo(3);
    }

    // ---- db_handoff_pulls_total and db_handoff_docs_total --------------------------------------------------

    @Test
    void aHandoffPassCountsThePullAndEveryDocumentItApplied() throws Exception {
        Peer source = peer("127.0.0.1").offer(KEY, definition(2), docs(
                "0", doc("ann", 31, 2, "src", 2_000), "1", doc("bob", 40, 1, "src", 1_000)));
        clusterOf(source);

        handoff(table, metrics).onChange();

        assertThat(count("db_handoff_pulls_total")).isEqualTo(1);
        assertThat(count("db_handoff_docs_total")).isEqualTo(2);
        assertThat(source.pulls.get()).isEqualTo(1);
    }

    @Test
    void onlyTheDocumentsThatChangedHereAreCountedAsApplied() throws Exception {
        seedReceiver(1, docs("0", doc("zed", 99, 5, "src", 5_000))); // already newer than the source's copy
        Peer source = peer("127.0.0.1").offer(KEY, definition(2), docs(
                "0", doc("ann", 31, 2, "src", 2_000),   // older than the local one: not applied
                "1", doc("bob", 40, 1, "src", 1_000))); // a gap this node fills
        clusterOf(source);

        handoff(table, metrics).onChange();

        assertThat(count("db_handoff_pulls_total")).isEqualTo(1);
        assertThat(count("db_handoff_docs_total")).isEqualTo(1);
    }

    @Test
    void aPassThatFindsEverythingAlreadyHereMovesNothingAndCountsNothing() throws Exception {
        Peer source = peer("127.0.0.1").offer(KEY, definition(1), docs("0", doc("ann", 31, 1, "src", 1_000)));
        clusterOf(source);
        HandoffService service = handoff(table, metrics);
        service.onChange();
        assertThat(count("db_handoff_pulls_total")).isEqualTo(1);
        assertThat(count("db_handoff_docs_total")).isEqualTo(1);

        service.onChange(); // the schema already moved, so there is nothing left to pull

        assertThat(count("db_handoff_pulls_total")).isEqualTo(1);
        assertThat(count("db_handoff_docs_total")).isEqualTo(1);
        assertThat(source.pulls.get()).isEqualTo(1);

        handoff(table, metrics).onChange(); // a fresh service pulls again, and the causal apply changes nothing

        assertThat(count("db_handoff_pulls_total")).as("the pull happened").isEqualTo(2);
        assertThat(count("db_handoff_docs_total")).as("but it applied nothing").isEqualTo(1);
    }

    @Test
    void aPullThatFailsCountsNeitherAPullNorDocuments() throws Exception {
        Peer source = peer("127.0.0.1").offer(KEY, definition(1), docs("0", doc("ann", 31, 1, "src", 1_000)));
        source.pullStatus = 500;
        clusterOf(source);

        handoff(table, metrics).onChange();

        assertThat(count("db_handoff_pulls_total")).isZero();
        assertThat(count("db_handoff_docs_total")).isZero();
    }

    @Test
    void aSingleNodeHasNothingToPullAndCountsNothing() throws Exception {
        peer("127.0.0.1").offer(KEY, definition(1), docs("0", doc("ann", 31, 1, "src", 1_000)));
        clusterOf();

        handoff(table, metrics).onChange();

        assertThat(count("db_handoff_pulls_total")).isZero();
        assertThat(count("db_handoff_docs_total")).isZero();
    }

    // ---- a metrics failure never breaks SWIM or handoff ----------------------------------------------------

    @Test
    void aBrokenRegistryNeverBreaksSwimOrHandoff() throws Exception {
        for (MeterRegistry broken : List.of(new RegistryThatCannotRegister(), new RegistryWhoseMetersThrow())) {
            DbMetrics failing = new DbMetrics(broken);
            MembershipTable swim = new MembershipTable(failing);
            swim.initSelf(node(SELF));
            swim.merge(new Member(node("Node1"), MemberState.ALIVE, 0, 0L));
            swim.merge(new Member(node("Node2"), MemberState.ALIVE, 0, 0L));
            ScriptedClient client = scriptedDetector(failing, swim);
            client.pingAcks = false;

            detector.tick(T0, n -> 0); // direct and indirect probes, then the suspicion

            assertThat(swim.get("Node1").getState()).as("the probe still ran its course").isEqualTo(MemberState.SUSPECT);
            assertThat(client.calls).hasSize(2);
            swim.mergeGossip(List.of(new MemberUpdate("Node3", "8080", MemberState.ALIVE, 0)));
            assertThat(swim.get("Node3")).isNotNull();
            assertThat(swim.outgoingGossip(6)).isNotEmpty();
            assertThat(swim.replyGossip(6, "Node1")).isNotEmpty();

            Peer source = peer("127.0.0.1").offer(KEY, definition(1), docs("0", doc("ann", 31, 1, "src", 1_000)));
            clusterOf(source);
            handoff(swim, failing).onChange();
            assertThat(Files.exists(records.resolve("0.json"))).as("the handoff still applied the document").isTrue();

            peers.forEach(p -> p.server.stop(0));
            peers.clear();
            deleteTree(dbDir);
            detector.close();
            Network.self = new Node(SELF, "8080");
            Network.nodes = Collections.synchronizedList(new ArrayList<>());
            Network.ring = null;
        }
    }

    // ---- the catalogue -------------------------------------------------------------------------------------

    @Test
    void registersExactlyTheCataloguedNamesTagsAndTypesAndNeverTheReservedTags() {
        Set<String> names = registry.getMeters().stream().map(m -> m.getId().getName())
                .filter(name -> name.startsWith("db_ring_") || name.startsWith("db_swim_") || name.startsWith("db_handoff_"))
                .collect(Collectors.toSet());
        assertThat(names).containsExactlyInAnyOrder("db_ring_size", "db_swim_members", "db_swim_probes_total",
                "db_swim_gossip_total", "db_handoff_pulls_total", "db_handoff_docs_total");

        assertThat(tagValues("db_ring_size")).containsExactly("");
        assertThat(tagValues("db_swim_members")).containsExactlyInAnyOrder("state=alive", "state=suspect", "state=dead");
        assertThat(tagValues("db_swim_probes_total")).containsExactlyInAnyOrder("kind=direct", "kind=indirect");
        assertThat(tagValues("db_swim_gossip_total")).containsExactlyInAnyOrder("direction=sent", "direction=received");
        assertThat(tagValues("db_handoff_pulls_total")).containsExactly("");
        assertThat(tagValues("db_handoff_docs_total")).containsExactly("");

        assertThat(registry.find("db_ring_size").meters()).allSatisfy(m -> assertThat(m).isInstanceOf(Gauge.class));
        assertThat(registry.find("db_swim_members").meters()).allSatisfy(m -> assertThat(m).isInstanceOf(Gauge.class));
        for (String counter : List.of("db_swim_probes_total", "db_swim_gossip_total", "db_handoff_pulls_total",
                "db_handoff_docs_total")) {
            assertThat(registry.find(counter).meters()).allSatisfy(m -> assertThat(m).isInstanceOf(Counter.class));
            assertThat(count(counter)).as("%s is present from startup, at zero", counter).isZero();
        }
        for (Meter meter : registry.getMeters()) {
            assertThat(meter.getId().getTags()).extracting(Tag::getKey)
                    .as("%s must not use a reserved common tag", meter.getId().getName())
                    .doesNotContain("role", "node");
        }
    }

    private Set<String> tagValues(String name) {
        Set<String> combos = new HashSet<>();
        for (Meter meter : registry.find(name).meters()) {
            combos.add(meter.getId().getTags().stream().sorted()
                    .map(t -> t.getKey() + "=" + t.getValue()).collect(Collectors.joining("|")));
        }
        return combos;
    }

    // ---- fixtures ------------------------------------------------------------------------------------------

    private Peer peer(String host) throws IOException {
        Peer peer = new Peer(host);
        peers.add(peer);
        return peer;
    }

    /** The cluster as this node sees it: itself and the given peers, ring included. */
    private void clusterOf(Peer... members) {
        List<Node> live = new ArrayList<>();
        live.add(new Node(SELF, "8080"));
        for (Peer member : members) {
            live.add(member.node);
        }
        Network.setLiveMembers(live);
    }

    private HandoffService handoff(MembershipTable membership, DbMetrics m) {
        HandoffService service = new HandoffService(membership, new WriteOperation(mock(IndexManager.class)), m);
        handoffs.add(service);
        return service;
    }

    private static JSONObject definition(int nextId) {
        return new JSONObject()
                .put("info", new JSONObject().put("schemaName", SCHEMA).put("nextId", nextId))
                .put("schema", new JSONObject().put("Name", "String").put("Age", "Integer"));
    }

    /** A live document at {@code version}, whose clock is the single entry {@code node:version}. */
    private static JSONObject doc(String name, int age, int version, String node, long ts) {
        return new JSONObject().put("Name", name).put("Age", age).put("_version", version)
                .put("_vclock", new JSONObject().put(node, version)).put("_ts", ts);
    }

    private static Map<String, JSONObject> docs(Object... idsAndDocs) {
        Map<String, JSONObject> docs = new LinkedHashMap<>();
        for (int i = 0; i < idsAndDocs.length; i += 2) {
            docs.put((String) idsAndDocs[i], (JSONObject) idsAndDocs[i + 1]);
        }
        return docs;
    }

    /** This node already holds the schema, with {@code localDocs} (id to document) on disk. */
    private void seedReceiver(int nextId, Map<String, JSONObject> localDocs) throws Exception {
        Path schemaFile = dbDir.resolve("schemas").resolve("users.json");
        Files.createDirectories(schemaFile.getParent());
        Files.createDirectories(records);
        Files.writeString(schemaFile, definition(nextId).toString());
        for (Map.Entry<String, JSONObject> entry : localDocs.entrySet()) {
            Files.writeString(records.resolve(entry.getKey() + ".json"), entry.getValue().toString());
        }
    }

    private static void deleteTree(Path root) throws IOException {
        if (Files.exists(root)) {
            try (var paths = Files.walk(root)) {
                paths.sorted((a, b) -> b.compareTo(a)).forEach(p -> p.toFile().delete());
            }
        }
    }

    /**
     * A peer on loopback: acks {@code /swim/ping} with whatever it was told to, and serves {@code /swim/schemas}
     * and {@code /swim/pull} in the wire form of {@code HandoffController}.
     */
    private static final class Peer {
        final HttpServer server;
        final Node node;
        final AtomicInteger pings = new AtomicInteger();
        final AtomicInteger pulls = new AtomicInteger();
        final Map<String, String> pullBodies = new ConcurrentHashMap<>();
        volatile List<String> listing = new ArrayList<>();
        volatile int pullStatus = 200;
        volatile String pingReply = "{\"acked\":true}";

        Peer(String host) throws IOException {
            server = HttpServer.create(new InetSocketAddress(InetAddress.getByName(host), 0), 0);
            node = new Node(host, String.valueOf(server.getAddress().getPort()));
            server.createContext("/swim/ping", exchange -> {
                exchange.getRequestBody().readAllBytes();
                pings.incrementAndGet();
                reply(exchange, 200, pingReply);
            });
            server.createContext("/swim/schemas", exchange -> reply(exchange, 200, new JSONArray(listing).toString()));
            server.createContext("/swim/pull", exchange -> {
                pulls.incrementAndGet();
                String body = pullBodies.get(queryParam(exchange, "database") + "." + queryParam(exchange, "schema"));
                if (pullStatus != 200) {
                    reply(exchange, pullStatus, "{}");
                } else {
                    reply(exchange, body == null ? 404 : 200, body == null ? "{}" : body);
                }
            });
            server.start();
        }

        /** Makes this peer advertise {@code key} and serve {@code docs} (id to document) when asked for it. */
        Peer offer(String key, JSONObject definition, Map<String, JSONObject> docs) {
            String[] parts = key.split("\\.", 2);
            JSONObject documents = new JSONObject();
            docs.forEach(documents::put);
            pullBodies.put(key, new JSONObject()
                    .put("database", parts[0]).put("schema", parts[1])
                    .put("definition", definition)
                    .put("documents", documents).toString());
            List<String> advertised = new ArrayList<>(listing);
            if (!advertised.contains(key)) {
                advertised.add(key);
            }
            listing = advertised;
            return this;
        }

        private static String queryParam(HttpExchange exchange, String name) {
            String query = exchange.getRequestURI().getRawQuery();
            if (query != null) {
                for (String pair : query.split("&")) {
                    if (pair.startsWith(name + "=")) {
                        return pair.substring(name.length() + 1);
                    }
                }
            }
            return "";
        }

        private static void reply(HttpExchange exchange, int status, String body) throws IOException {
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(status, bytes.length);
            exchange.getResponseBody().write(bytes);
            exchange.close();
        }
    }

    /** A registry that fails the moment a meter is registered, as a broken exporter would. */
    private static final class RegistryThatCannotRegister extends SimpleMeterRegistry {
        @Override
        protected Counter newCounter(Meter.Id id) {
            throw new IllegalStateException("registry down");
        }

        @Override
        protected <T> Gauge newGauge(Meter.Id id, T obj, ToDoubleFunction<T> valueFunction) {
            throw new IllegalStateException("registry down");
        }

        @Override
        protected DistributionSummary newDistributionSummary(Meter.Id id, DistributionStatisticConfig config,
                                                             double scale) {
            throw new IllegalStateException("registry down");
        }
    }

    /** A registry whose counters register fine and then throw on every increment. */
    private static final class RegistryWhoseMetersThrow extends SimpleMeterRegistry {
        @Override
        protected Counter newCounter(Meter.Id id) {
            return new io.micrometer.core.instrument.cumulative.CumulativeCounter(id) {
                @Override
                public void increment(double amount) {
                    throw new IllegalStateException("counter down");
                }
            };
        }

        @Override
        protected <T> Gauge newGauge(Meter.Id id, T obj, ToDoubleFunction<T> valueFunction) {
            throw new IllegalStateException("gauge down");
        }

        @Override
        protected DistributionSummary newDistributionSummary(Meter.Id id, DistributionStatisticConfig config,
                                                             double scale) {
            throw new IllegalStateException("summary down");
        }
    }
}
