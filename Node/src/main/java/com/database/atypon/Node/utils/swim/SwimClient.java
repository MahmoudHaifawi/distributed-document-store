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

import java.util.List;
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
