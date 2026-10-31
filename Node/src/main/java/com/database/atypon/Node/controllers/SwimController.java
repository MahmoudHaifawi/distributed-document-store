package com.database.atypon.Node.controllers;

import com.database.atypon.Node.model.Node;
import com.database.atypon.Node.utils.swim.Member;
import com.database.atypon.Node.utils.swim.MemberState;
import com.database.atypon.Node.utils.swim.MembershipTable;
import com.database.atypon.Node.utils.swim.SwimClient;
import com.database.atypon.Node.utils.swim.SwimConfig;
import com.database.atypon.Node.utils.swim.dto.AckMessage;
import com.database.atypon.Node.utils.swim.dto.MemberUpdate;
import com.database.atypon.Node.utils.swim.dto.PingMessage;
import com.database.atypon.Node.utils.swim.dto.PingReqMessage;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.util.ArrayList;
import java.util.List;

/**
 * The receiving end of SWIM failure detection and of joining and leaving the cluster. Node-to-node only:
 * {@code /swim/**} requires the INTERNAL role (see {@code SecurityConfig}).
 *
 * <p>Every probe that arrives has its gossip merged into the {@link MembershipTable} first, and every
 * answer leaves carrying this node's outgoing gossip, which always includes its own {@code ALIVE}.
 * That last part is what lets a prober that suspected this node clear the suspicion when it answers.
 * An answer also carries what this node believes about the prober if it holds the prober as {@code DEAD}
 * or {@code SUSPECT} ({@link MembershipTable#replyGossip}): a node that was written off while cut off
 * hears about it the first time it gets through, refutes, and so rejoins the ring when a partition heals.
 */
@RestController
public class SwimController {

    private final SwimClient client;
    private final MembershipTable membership;

    public SwimController(SwimClient client, MembershipTable membership) {
        this.client = client;
        this.membership = membership;
    }

    /** A direct probe. Being able to answer at all is the ack; there is no separate ack endpoint. */
    @PostMapping("/swim/ping")
    public AckMessage ping(@RequestBody(required = false) PingMessage ping) {
        if (ping != null) {
            membership.mergeGossip(ping.getGossip());
        }
        return answer(true, ping == null ? null : ping.getFrom());
    }

    /**
     * An indirect probe: pings the named target on the caller's behalf and reports whether it acked.
     * The caller could not reach the target itself; this node may be able to.
     */
    @PostMapping("/swim/ping-req")
    public AckMessage pingReq(@RequestBody PingReqMessage request) {
        Node target = request.getTarget();
        if (target == null || target.getName() == null || target.getPort() == null) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "ping-req needs a target with a name and a port");
        }
        // Merged before the relay, so the ping to the target already carries what the caller just told us.
        membership.mergeGossip(request.getGossip());
        return answer(client.ping(target), request.getFrom());
    }

    /**
     * A node asks to be admitted: {@code joiner} is the new node as itself. It is merged like any other claim
     * (and so queued for gossip, which is how the rest of the cluster hears of it) and the answer is this
     * node's whole member list, tombstones included, for the joiner to adopt.
     *
     * <p>The list is the table as it stands, not gossip drawn from the retransmit queue, so it names the
     * joiner too if this node holds it as {@code DEAD} or {@code SUSPECT}: a node that restarts under its old
     * name is told how far its previous life got, and refutes it.
     */
    @PostMapping("/swim/join")
    public List<MemberUpdate> join(@RequestBody(required = false) MemberUpdate joiner) {
        requireNamed(joiner, "join");
        membership.merge(joiner.toMember());
        List<MemberUpdate> view = new ArrayList<>();
        for (Member member : membership.snapshot()) {
            view.add(MemberUpdate.from(member));
        }
        return view;
    }

    /**
     * A node announces it is leaving: the body names it. Whatever state the body claims, this is a
     * departure, so what is merged is {@code LEFT} at the incarnation given. A {@code LEFT} about another node
     * is ordinary news (queued for gossip, and the node drops off the ring); it is only a claim about
     * <em>this</em> node that the table refutes. The answer carries no gossip: the leaver is going and would
     * not merge it, and handing it out would only spend retransmits that belong to the cluster.
     */
    @PostMapping("/swim/leave")
    public AckMessage leave(@RequestBody(required = false) MemberUpdate leaver) {
        MemberUpdate departure = leaver == null ? null
                : new MemberUpdate(leaver.getName(), leaver.getPort(), MemberState.LEFT, leaver.getIncarnation());
        requireNamed(departure, "leave");
        membership.merge(departure.toMember());
        return new AckMessage(true);
    }

    private AckMessage answer(boolean acked, String sender) {
        return new AckMessage(acked, membership.replyGossip(SwimConfig.gossipMax(), sender));
    }

    private static void requireNamed(MemberUpdate update, String what) {
        if (update == null || !update.wellFormed()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                    what + " needs a member with a name, a port, a state and a non-negative incarnation");
        }
    }
}
