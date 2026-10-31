package com.database.atypon.Node.utils.swim;

import com.database.atypon.Node.model.Node;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SwimConfigTest {

    private Function<String, String> originalEnv;
    private final Map<String, String> fakeEnv = new HashMap<>();

    @BeforeEach
    void installFakeEnv() {
        originalEnv = SwimConfig.env;
        SwimConfig.env = fakeEnv::get;
    }

    @AfterEach
    void restoreEnv() {
        SwimConfig.env = originalEnv;
    }

    @Test
    void defaultsApplyWhenEnvironmentIsUnset() {
        assertThat(SwimConfig.gossipEnabled()).isTrue();
        assertThat(SwimConfig.periodMs()).isEqualTo(1000L);
        assertThat(SwimConfig.pingTimeoutMs()).isEqualTo(500L);
        assertThat(SwimConfig.indirectK()).isEqualTo(3);
        assertThat(SwimConfig.suspectTimeoutMs()).isEqualTo(5000L);
        assertThat(SwimConfig.gossipLambda()).isEqualTo(3);
        assertThat(SwimConfig.gossipMax()).isEqualTo(6);
    }

    @Test
    void blankValuesFallBackToDefaults() {
        fakeEnv.put("GOSSIP_ENABLED", "  ");
        fakeEnv.put("SWIM_PERIOD_MS", "");
        fakeEnv.put("SWIM_INDIRECT_K", "   ");

        assertThat(SwimConfig.gossipEnabled()).isTrue();
        assertThat(SwimConfig.periodMs()).isEqualTo(1000L);
        assertThat(SwimConfig.indirectK()).isEqualTo(3);
    }

    @Test
    void configuredValuesOverrideDefaults() {
        fakeEnv.put("GOSSIP_ENABLED", " false ");
        fakeEnv.put("SWIM_PERIOD_MS", "250");
        fakeEnv.put("SWIM_PING_TIMEOUT_MS", "100");
        fakeEnv.put("SWIM_INDIRECT_K", "5");
        fakeEnv.put("SWIM_SUSPECT_TIMEOUT_MS", "9000");
        fakeEnv.put("SWIM_GOSSIP_LAMBDA", "4");
        fakeEnv.put("SWIM_GOSSIP_MAX", "10");

        assertThat(SwimConfig.gossipEnabled()).isFalse();
        assertThat(SwimConfig.periodMs()).isEqualTo(250L);
        assertThat(SwimConfig.pingTimeoutMs()).isEqualTo(100L);
        assertThat(SwimConfig.indirectK()).isEqualTo(5);
        assertThat(SwimConfig.suspectTimeoutMs()).isEqualTo(9000L);
        assertThat(SwimConfig.gossipLambda()).isEqualTo(4);
        assertThat(SwimConfig.gossipMax()).isEqualTo(10);
    }

    @Test
    void invalidIntegerThrowsIllegalStateException() {
        fakeEnv.put("SWIM_INDIRECT_K", "three");

        assertThatThrownBy(SwimConfig::indirectK)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("SWIM_INDIRECT_K");
    }

    @Test
    void invalidLongThrowsIllegalStateException() {
        fakeEnv.put("SWIM_PERIOD_MS", "1s");

        assertThatThrownBy(SwimConfig::periodMs)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("SWIM_PERIOD_MS");
    }

    @Test
    void seedsParsesCommaSeparatedHostPortList() {
        fakeEnv.put("SEED_NODES", "a:8080,b:8080");

        List<Node> seeds = SwimConfig.seeds();

        assertThat(seeds).extracting(Node::getName).containsExactly("a", "b");
        assertThat(seeds).extracting(Node::getPort).containsExactly("8080", "8080");
    }

    @Test
    void seedsToleratesWhitespaceAndEmptyEntries() {
        fakeEnv.put("SEED_NODES", " a:8081 , ,b:8082,");

        List<Node> seeds = SwimConfig.seeds();

        assertThat(seeds).extracting(Node::getName).containsExactly("a", "b");
        assertThat(seeds).extracting(Node::getPort).containsExactly("8081", "8082");
    }

    @Test
    void seedsFallsBackToClusterNodesWhenSeedNodesBlank() {
        fakeEnv.put("SEED_NODES", "  ");
        fakeEnv.put("CLUSTER_NODES", "Node0:8080,Node1:9090");

        List<Node> seeds = SwimConfig.seeds();

        assertThat(seeds).extracting(Node::getName).containsExactly("Node0", "Node1");
        assertThat(seeds).extracting(Node::getPort).containsExactly("8080", "9090");
    }

    @Test
    void seedsPrefersSeedNodesOverClusterNodes() {
        fakeEnv.put("SEED_NODES", "seed:8080");
        fakeEnv.put("CLUSTER_NODES", "Node0:8080,Node1:8080");

        assertThat(SwimConfig.seeds()).extracting(Node::getName).containsExactly("seed");
    }

    @Test
    void seedsIsEmptyWhenNeitherIsConfigured() {
        assertThat(SwimConfig.seeds()).isEmpty();

        fakeEnv.put("SEED_NODES", "");
        fakeEnv.put("CLUSTER_NODES", "   ");
        assertThat(SwimConfig.seeds()).isEmpty();
    }

    @Test
    void memberStatePrecedenceOrdersAliveBelowSuspectBelowDeadAndLeft() {
        assertThat(MemberState.ALIVE.precedence()).isEqualTo(0);
        assertThat(MemberState.SUSPECT.precedence()).isEqualTo(1);
        assertThat(MemberState.DEAD.precedence()).isEqualTo(2);
        assertThat(MemberState.LEFT.precedence()).isEqualTo(2);
    }

    @Test
    void memberStateIsLiveOnlyForAliveAndSuspect() {
        assertThat(MemberState.ALIVE.isLive()).isTrue();
        assertThat(MemberState.SUSPECT.isLive()).isTrue();
        assertThat(MemberState.DEAD.isLive()).isFalse();
        assertThat(MemberState.LEFT.isLive()).isFalse();
    }

    @Test
    void memberCopiesDoNotMutateTheOriginal() {
        Node node = new Node("a", "8080");
        Member alive = new Member(node, MemberState.ALIVE, 2, 1000L);

        Member suspect = alive.withState(MemberState.SUSPECT);
        Member bumped = alive.withIncarnation(3);

        assertThat(alive.getState()).isEqualTo(MemberState.ALIVE);
        assertThat(alive.getIncarnation()).isEqualTo(2);
        assertThat(suspect.getState()).isEqualTo(MemberState.SUSPECT);
        assertThat(suspect.getIncarnation()).isEqualTo(2);
        assertThat(suspect.getNode()).isSameAs(node);
        assertThat(suspect.getLastChange()).isEqualTo(1000L);
        assertThat(bumped.getIncarnation()).isEqualTo(3);
        assertThat(bumped.getState()).isEqualTo(MemberState.ALIVE);
    }
}
