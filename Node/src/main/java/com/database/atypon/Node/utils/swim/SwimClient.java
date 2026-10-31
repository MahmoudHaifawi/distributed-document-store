package com.database.atypon.Node.utils.swim;

import com.database.atypon.Node.model.Network;
import com.database.atypon.Node.model.Node;
import com.database.atypon.Node.security.ServiceTokens;
import com.database.atypon.Node.utils.swim.dto.AckMessage;
import com.database.atypon.Node.utils.swim.dto.MemberUpdate;
import com.database.atypon.Node.utils.swim.dto.PingMessage;
import com.database.atypon.Node.utils.swim.dto.PingReqMessage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * The calls one node makes to another for failure detection. Both answer a plain boolean, "did the
 * peer ack", and a call that fails in any way (refused, timed out, an error status, a garbled body)
 * is simply {@code false}: for a failure detector, not answering is the signal, never an exception.
 *
 * <p>Calls are bounded by {@code SWIM_PING_TIMEOUT_MS}. A ping gets that long to connect and that long
 * to answer. A ping-req gets twice as long to answer, because the relay must itself ping the target
 * (up to the ping timeout) before it can reply.
 *
 * <p>{@link #join} and {@link #leave} are the membership calls rather than probes: a node joins by asking a
 * seed for its member list, and leaves by handing a few peers its {@code LEFT} record.
 *
 * <p>Every call carries membership gossip out ({@link MembershipTable#outgoingGossip}, this node's own
 * {@code ALIVE} among it) and merges the gossip that comes back in the answer, whether or not the answer
 * is an ack. That is the only way membership news travels: there is no broadcast. The gossip is spent
 * when it is handed to the call, not when the call succeeds, as in SWIM generally; a lost message costs
 * one of the several sends each update gets.
 */
@Component
public class SwimClient {

    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(SwimClient.class);

    private final MembershipTable membership;
    private final RestTemplate pingRest;
    private final RestTemplate pingReqRest;
    private final Supplier<String> bearer;

    @Autowired
    public SwimClient(MembershipTable membership) {
        this(membership, SwimConfig.pingTimeoutMs(), ServiceTokens::bearer);
    }

    /** Test seam: {@code bearer} supplies the Authorization header, so no JWT service is needed. */
    SwimClient(MembershipTable membership, long pingTimeoutMs, Supplier<String> bearer) {
        this.membership = membership;
        this.pingRest = restTemplate(pingTimeoutMs, pingTimeoutMs);
        this.pingReqRest = restTemplate(pingTimeoutMs, 2 * pingTimeoutMs);
        this.bearer = bearer;
    }

    /** Pings {@code target} directly; true if it answered with an ack in time. */
    public boolean ping(Node target) {
        return post(pingRest, target, "/swim/ping", new PingMessage(selfName(), outgoingGossip()));
    }

    /** Asks {@code via} to ping {@code target} for us; true if it reports that the target acked. */
    public boolean pingReq(Node via, Node target) {
        return post(pingReqRest, via, "/swim/ping-req", new PingReqMessage(selfName(), target, outgoingGossip()));
    }

    /**
     * Asks {@code seed} to admit this node, introducing it as {@code self}. The seed answers with its whole
     * member list, tombstones included, which the caller adopts. Empty if the seed could not be reached or
     * did not answer sensibly, so the caller can go on to the next seed; never throws.
     *
     * <p>This is a plain request, with no piggybacked gossip: the answer is a snapshot, not a probe, and the
     * caller merges it itself. It gets the same time to answer as a ping-req.
     */
    public Optional<List<MemberUpdate>> join(Node seed, MemberUpdate self) {
        try {
            MemberUpdate[] view = pingReqRest.postForObject(seed.getURL() + "/swim/join", entity(self), MemberUpdate[].class);
            return view == null ? Optional.empty() : Optional.of(Arrays.asList(view));
        } catch (RuntimeException e) {
            log.debug("SWIM join via {} got no answer: {}", seed.getName(), e.toString());
            return Optional.empty();
        }
    }

    /**
     * Tells {@code peer} that the node described by {@code leaving} (its {@code LEFT} record) has gone.
     * Deliberately not the gossip of a probe ({@code outgoingGossip}): that attaches this node's own {@code ALIVE}, which would
     * contradict, and on the receiving side outrank, the {@code LEFT} it is carrying. True if the peer
     * acknowledged; never throws.
     */
    public boolean leave(Node peer, MemberUpdate leaving) {
        try {
            AckMessage ack = pingRest.postForObject(peer.getURL() + "/swim/leave", entity(leaving), AckMessage.class);
            return ack != null && ack.isAcked();
        } catch (RuntimeException e) {
            log.debug("SWIM leave to {} got no ack: {}", peer.getName(), e.toString());
            return false;
        }
    }

    private HttpEntity<Object> entity(Object body) {
        HttpHeaders headers = new HttpHeaders();
        headers.set("Authorization", bearer.get());
        return new HttpEntity<>(body, headers);
    }

    private boolean post(RestTemplate rest, Node peer, String path, Object body) {
        try {
            HttpHeaders headers = new HttpHeaders();
            headers.set("Authorization", bearer.get());
            AckMessage ack = rest.postForObject(peer.getURL() + path, new HttpEntity<>(body, headers), AckMessage.class);
            if (ack == null) {
                return false;
            }
            membership.mergeGossip(ack.getGossip());
            return ack.isAcked();
        } catch (RuntimeException e) {
            log.debug("SWIM {} to {} got no ack: {}", path, peer.getName(), e.toString());
            return false;
        }
    }

    private List<MemberUpdate> outgoingGossip() {
        return membership.outgoingGossip(SwimConfig.gossipMax());
    }

    private static String selfName() {
        Node self = Network.self;
        return self == null ? null : self.getName();
    }

    private static RestTemplate restTemplate(long connectTimeoutMs, long readTimeoutMs) {
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(millis(connectTimeoutMs));
        factory.setReadTimeout(millis(readTimeoutMs));
        return new RestTemplate(factory);
    }

    /** A timeout of zero would mean "wait forever" to the HTTP client, which is never what a probe wants. */
    private static int millis(long timeoutMs) {
        return (int) Math.max(1L, Math.min(timeoutMs, Integer.MAX_VALUE));
    }
}
