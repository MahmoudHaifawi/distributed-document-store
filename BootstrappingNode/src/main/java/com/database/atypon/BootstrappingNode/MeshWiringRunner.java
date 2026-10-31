package com.database.atypon.BootstrappingNode;

import com.database.atypon.BootstrappingNode.model.Node;
import com.database.atypon.BootstrappingNode.security.ServiceTokens;
import com.database.atypon.BootstrappingNode.utils.ClusterNodes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.List;
import java.util.Vector;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

/**
 * On start-up (coordinator role only), wires every data node's peer list and self-assignment.
 * A node that is not reachable yet is skipped; it is wired when it comes up and re-registers.
 *
 * <p>This is the <em>legacy</em> membership: the coordinator decides the cluster once, at start-up, and
 * tells each node who its peers are. With gossip on (the default, {@code GOSSIP_ENABLED}) the data nodes
 * find each other themselves by SWIM, joining through a seed (the coordinator is one, see
 * {@code SwimSeedController}), and this runner does <b>nothing</b>: wiring on top of gossip would hand a node
 * a second identity and a peer list the failure detector has not vouched for. It wires, exactly as it
 * always did, only when {@code GOSSIP_ENABLED=false}.
 */
@Component
@Profile("coordinator")
public class MeshWiringRunner implements CommandLineRunner {

    private static final String GOSSIP_ENABLED_ENV = "GOSSIP_ENABLED";
    private static final boolean DEFAULT_GOSSIP_ENABLED = true;

    private final BooleanSupplier gossipEnabled;
    private final RestTemplate restTemplate;
    private final Supplier<List<Node>> nodes;
    private final Supplier<String> bearer;

    @Autowired
    public MeshWiringRunner() {
        this(MeshWiringRunner::gossipEnabledFromEnvironment, new RestTemplate(), ClusterNodes::all, ServiceTokens::bearer);
    }

    /** For tests: the flag, the HTTP client, the nodes to wire and the service token are all supplied. */
    MeshWiringRunner(BooleanSupplier gossipEnabled, RestTemplate restTemplate,
                     Supplier<List<Node>> nodes, Supplier<String> bearer) {
        this.gossipEnabled = gossipEnabled;
        this.restTemplate = restTemplate;
        this.nodes = nodes;
        this.bearer = bearer;
    }

    @Override
    public void run(String... args) {
        if (gossipEnabled.getAsBoolean()) {
            return; // gossip is in charge of membership; see the class comment
        }
        List<Node> nodes = this.nodes.get();

        HttpHeaders headers = new HttpHeaders();
        headers.set("Authorization", bearer.get());

        for (int i = 0; i < nodes.size(); i++) {
            List<Node> peers = new Vector<>();
            for (int j = 0; j < nodes.size(); j++) {
                if (i != j) {
                    peers.add(nodes.get(j));
                }
            }
            String base = nodes.get(i).getURL();
            try {
                restTemplate.postForObject(base + "/network/add/nodes", new HttpEntity<>(peers, headers), String.class);
                restTemplate.postForObject(base + "/network/assign/self", new HttpEntity<>(nodes.get(i), headers), String.class);
            } catch (Exception e) {
                // Node not reachable yet; wired when it comes up and re-registers.
            }
        }
    }

    private static boolean gossipEnabledFromEnvironment() {
        return gossipEnabled(System.getenv(GOSSIP_ENABLED_ENV));
    }

    /**
     * Reads the {@code GOSSIP_ENABLED} flag the way the data nodes do (their {@code SwimConfig}; this module
     * cannot share it): unset or blank is the default, on; anything else is {@link Boolean#parseBoolean}
     * of the trimmed value. Package-private for testing.
     */
    static boolean gossipEnabled(String raw) {
        if (raw == null || raw.isBlank()) {
            return DEFAULT_GOSSIP_ENABLED;
        }
        return Boolean.parseBoolean(raw.trim());
    }
}
