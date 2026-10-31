package com.database.atypon.BootstrappingNode;

import com.database.atypon.BootstrappingNode.model.MemberState;
import com.database.atypon.BootstrappingNode.model.MemberUpdate;
import com.database.atypon.BootstrappingNode.model.Node;
import com.database.atypon.BootstrappingNode.security.JwtService;
import com.database.atypon.BootstrappingNode.utils.ClusterNodes;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.web.client.RestTemplate;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.content;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.header;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * The coordinator as a SWIM seed: a joining data node POSTs itself to {@code /swim/join} and gets the
 * member list back in exactly the JSON shape the data nodes speak ({@code name}, {@code port},
 * {@code state}, {@code incarnation}); and the legacy one-shot mesh wiring runs only when gossip is off.
 *
 * <p>Same boot configuration (and dev secret) as {@code BootstrapAuthorizationTest}, so the Spring context
 * is shared rather than started twice.
 */
@SpringBootTest
@AutoConfigureMockMvc
@TestPropertySource(properties = "app.jwt.secret=test-secret-test-secret-test-secret-123456")
class SwimSeedControllerTest {

    @Autowired
    MockMvc mvc;
    @Autowired
    JwtService jwt;
    @Autowired
    ObjectMapper json;

    private String bearer(String role) {
        return "Bearer " + jwt.generateToken("u", role, Duration.ofMinutes(5));
    }

    private static String body(String name, String port, String state, int incarnation) {
        return "{\"name\":\"" + name + "\",\"port\":\"" + port + "\",\"state\":\"" + state
                + "\",\"incarnation\":" + incarnation + "}";
    }

    /** What a joining data node sends: itself, ALIVE. */
    private String join(String name, String port, int incarnation) throws Exception {
        return mvc.perform(post("/swim/join").contentType(MediaType.APPLICATION_JSON)
                        .header("Authorization", bearer("INTERNAL"))
                        .content(body(name, port, "ALIVE", incarnation)))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();
    }

    private List<MemberUpdate> view(String responseBody) throws Exception {
        return Arrays.asList(json.readValue(responseBody, MemberUpdate[].class));
    }

    private static MemberUpdate named(List<MemberUpdate> view, String name) {
        List<MemberUpdate> hits = new ArrayList<>();
        for (MemberUpdate member : view) {
            if (name.equals(member.getName())) {
                hits.add(member);
            }
        }
        assertThat(hits).as("entries named %s in %s", name, view).hasSize(1);
        return hits.get(0);
    }

    // ---- POST /swim/join ----------------------------------------------------------------------

    @Test
    void joinReturnsSeedViewAndRegistersJoiner() throws Exception {
        List<MemberUpdate> first = view(join("SeedTestA", "8081", 2));

        // The seed's own picture: every configured data node, alive at incarnation zero ...
        for (Node configured : ClusterNodes.all()) {
            MemberUpdate member = named(first, configured.getName());
            assertThat(member.getPort()).isEqualTo(configured.getPort());
            assertThat(member.getState()).isEqualTo(MemberState.ALIVE);
            assertThat(member.getIncarnation()).isZero();
        }
        // ... and the joiner itself, as it announced itself.
        MemberUpdate a = named(first, "SeedTestA");
        assertThat(a.getPort()).isEqualTo("8081");
        assertThat(a.getState()).isEqualTo(MemberState.ALIVE);
        assertThat(a.getIncarnation()).isEqualTo(2);

        // The joiner was registered: the next one to join is handed it too.
        List<MemberUpdate> second = view(join("SeedTestB", "8082", 0));
        assertThat(named(second, "SeedTestA").getIncarnation()).isEqualTo(2);
        assertThat(named(second, "SeedTestB").getPort()).isEqualTo("8082");
        for (Node configured : ClusterNodes.all()) {
            named(second, configured.getName());
        }
    }

    @Test
    void rejoiningUnderTheSameNameReplacesTheOldEntryInsteadOfAddingOne() throws Exception {
        join("SeedTestRestarted", "8083", 4);
        List<MemberUpdate> after = view(join("SeedTestRestarted", "8084", 0));

        MemberUpdate restarted = named(after, "SeedTestRestarted"); // exactly one entry
        assertThat(restarted.getPort()).isEqualTo("8084");
        assertThat(restarted.getIncarnation()).isZero();
    }

    @Test
    void joinAnswersInTheWireFormOfTheDataNodes() throws Exception {
        JsonNode array = json.readTree(join("SeedTestShape", "8086", 0));

        assertThat(array.isArray()).isTrue();
        assertThat(array.size()).isGreaterThanOrEqualTo(2);
        for (JsonNode entry : array) {
            Set<String> fields = new HashSet<>();
            entry.fieldNames().forEachRemaining(fields::add);
            // Exactly the Node module's MemberUpdate: no lastChange, nothing the joiner cannot bind.
            assertThat(fields).containsExactlyInAnyOrder("name", "port", "state", "incarnation");
            assertThat(entry.get("name").isTextual()).isTrue();
            assertThat(entry.get("port").isTextual()).isTrue();
            assertThat(entry.get("state").asText()).isIn("ALIVE", "SUSPECT", "DEAD", "LEFT");
            assertThat(entry.get("incarnation").isInt()).isTrue();
        }
    }

    @Test
    void joinNeedsAWellFormedJoiner() throws Exception {
        String[] bad = {
                body("", "8080", "ALIVE", 0),
                "{\"port\":\"8080\",\"state\":\"ALIVE\",\"incarnation\":0}",
                "{\"name\":\"SeedTestNoPort\",\"state\":\"ALIVE\",\"incarnation\":0}",
                "{\"name\":\"SeedTestNoState\",\"port\":\"8080\",\"incarnation\":0}",
                body("SeedTestNegative", "8080", "ALIVE", -1),
                // A join is a node saying it is alive; the seed runs no failure detector, so it takes no
                // other claim (a departing node uses /swim/leave on the data nodes, not this).
                body("SeedTestSuspect", "8080", "SUSPECT", 0),
                body("SeedTestDead", "8080", "DEAD", 1),
                body("SeedTestLeft", "8080", "LEFT", 1),
        };
        for (String payload : bad) {
            mvc.perform(post("/swim/join").contentType(MediaType.APPLICATION_JSON)
                            .header("Authorization", bearer("INTERNAL")).content(payload))
                    .andExpect(status().isBadRequest());
        }
        // No body at all.
        mvc.perform(post("/swim/join").contentType(MediaType.APPLICATION_JSON)
                        .header("Authorization", bearer("INTERNAL")))
                .andExpect(status().isBadRequest());
    }

    // ---- /swim/** is internal -----------------------------------------------------------------

    @Test
    void swimEndpointsAreInternalOnly() throws Exception {
        String payload = body("SeedTestNobody", "8080", "ALIVE", 0);
        mvc.perform(post("/swim/join").contentType(MediaType.APPLICATION_JSON).content(payload))
                .andExpect(status().is4xxClientError());
        mvc.perform(post("/swim/join").contentType(MediaType.APPLICATION_JSON).content(payload)
                        .header("Authorization", bearer("USER")))
                .andExpect(status().isForbidden());
        mvc.perform(post("/swim/join").contentType(MediaType.APPLICATION_JSON).content(payload)
                        .header("Authorization", bearer("ADMIN")))
                .andExpect(status().isForbidden());
        // The refused callers did not register anything: a later joiner is not told about them.
        List<MemberUpdate> view = view(join("SeedTestWitness", "8087", 0));
        assertThat(view).extracting(MemberUpdate::getName).doesNotContain("SeedTestNobody");
    }

    // ---- MeshWiringRunner behind GOSSIP_ENABLED -----------------------------------------------

    @Test
    void meshWiringRunsOnlyWhenGossipDisabled() {
        List<Node> nodes = List.of(new Node("n0", "8080"), new Node("n1", "8081"));

        // GOSSIP_ENABLED=true (the default): inert. A mock server with no expectations fails on any request.
        RestTemplate quietRest = new RestTemplate();
        MockRestServiceServer quiet = MockRestServiceServer.createServer(quietRest);
        new MeshWiringRunner(() -> true, quietRest, () -> nodes, () -> "Bearer t").run();
        quiet.verify();

        // GOSSIP_ENABLED=false: the legacy one-shot wiring, as before -- every node is given its peers and
        // told who it is, in that order, with the service token.
        RestTemplate wiringRest = new RestTemplate();
        MockRestServiceServer wiring = MockRestServiceServer.createServer(wiringRest);
        for (Node node : nodes) {
            Node peer = nodes.get(0) == node ? nodes.get(1) : nodes.get(0);
            wiring.expect(requestTo(node.getURL() + "/network/add/nodes"))
                    .andExpect(method(HttpMethod.POST))
                    .andExpect(header("Authorization", "Bearer t"))
                    .andExpect(content().json("[{\"name\":\"" + peer.getName() + "\",\"port\":\"" + peer.getPort() + "\"}]"))
                    .andRespond(withSuccess("ok", MediaType.TEXT_PLAIN));
            wiring.expect(requestTo(node.getURL() + "/network/assign/self"))
                    .andExpect(method(HttpMethod.POST))
                    .andExpect(header("Authorization", "Bearer t"))
                    .andExpect(content().json("{\"name\":\"" + node.getName() + "\",\"port\":\"" + node.getPort() + "\"}"))
                    .andRespond(withSuccess("ok", MediaType.TEXT_PLAIN));
        }
        new MeshWiringRunner(() -> false, wiringRest, () -> nodes, () -> "Bearer t").run();
        wiring.verify();
    }

    @Test
    void gossipIsEnabledUnlessTheEnvironmentSaysOtherwise() {
        // Same reading as the data nodes' SwimConfig: unset or blank is the default (on); anything else is
        // Boolean.parseBoolean of the trimmed value.
        assertThat(MeshWiringRunner.gossipEnabled(null)).isTrue();
        assertThat(MeshWiringRunner.gossipEnabled("")).isTrue();
        assertThat(MeshWiringRunner.gossipEnabled("   ")).isTrue();
        assertThat(MeshWiringRunner.gossipEnabled("true")).isTrue();
        assertThat(MeshWiringRunner.gossipEnabled(" TRUE ")).isTrue();
        assertThat(MeshWiringRunner.gossipEnabled("false")).isFalse();
        assertThat(MeshWiringRunner.gossipEnabled(" False ")).isFalse();
        assertThat(MeshWiringRunner.gossipEnabled("nonsense")).isFalse();
    }
}
