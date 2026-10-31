package com.database.atypon.BootstrappingNode.controller;

import com.database.atypon.BootstrappingNode.model.MemberState;
import com.database.atypon.BootstrappingNode.model.MemberUpdate;
import com.database.atypon.BootstrappingNode.model.Node;
import com.database.atypon.BootstrappingNode.utils.ClusterNodes;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The coordinator as a SWIM seed: the well-known address a starting data node asks to be admitted by
 * ({@code POST /swim/join}). Node-to-node only: {@code /swim/**} requires the INTERNAL role (see
 * {@code SecurityConfig}).
 *
 * <p>The answer is a roster, in the JSON the data nodes' own {@code /swim/join} speaks (a list of
 * {@link MemberUpdate}), so a joiner needs to know nothing about who it is talking to. The roster is the
 * nodes this coordinator is configured with ({@link ClusterNodes}), each {@code ALIVE} at incarnation zero,
 * plus every node that has joined through here. That is a set of <em>contacts</em>, not a verdict on who is
 * up: the coordinator runs no failure detector, so it cannot say that a node has died. It does not need to.
 * The joiner gets people to probe, and the data nodes find out among themselves who is really alive, which
 * is the whole point of SWIM. A configured node that is down is listed {@code ALIVE} here and written off
 * by the first node that probes it.
 *
 * <p>A joiner is recorded by name, so a node that restarts under its old name replaces its earlier entry
 * (the latest claim wins; a node is the authority on itself) rather than appearing twice. A joiner is
 * always recorded {@code ALIVE}: joining is a node saying that it is, and a request claiming otherwise is
 * refused as malformed.
 */
@RestController
public class SwimSeedController {

    /** Nodes learned by joining, by name, in the order they first joined. Guarded by {@code this}. */
    private final Map<String, MemberUpdate> joined = new LinkedHashMap<>();

    /**
     * A node asks to be admitted: {@code joiner} is the new node as itself. It is remembered, and the answer
     * is the roster as it now stands, the joiner included.
     */
    @PostMapping("/swim/join")
    public List<MemberUpdate> join(@RequestBody(required = false) MemberUpdate joiner) {
        if (joiner == null || !joiner.wellFormed() || joiner.getState() != MemberState.ALIVE) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                    "join needs a node that is ALIVE, with a name, a port and a non-negative incarnation");
        }
        synchronized (this) {
            joined.put(joiner.getName(), new MemberUpdate(
                    joiner.getName(), joiner.getPort(), MemberState.ALIVE, joiner.getIncarnation()));
            return roster();
        }
    }

    /** Configured nodes first (in declaration order), then joiners; a joiner replaces a configured node of the same name. */
    private List<MemberUpdate> roster() {
        Map<String, MemberUpdate> byName = new LinkedHashMap<>();
        for (Node configured : ClusterNodes.all()) {
            byName.put(configured.getName(),
                    new MemberUpdate(configured.getName(), configured.getPort(), MemberState.ALIVE, 0));
        }
        for (MemberUpdate member : joined.values()) {
            byName.put(member.getName(), copy(member));
        }
        return new ArrayList<>(byName.values());
    }

    private static MemberUpdate copy(MemberUpdate member) {
        return new MemberUpdate(member.getName(), member.getPort(), member.getState(), member.getIncarnation());
    }
}
