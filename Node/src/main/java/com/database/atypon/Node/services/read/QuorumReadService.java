package com.database.atypon.Node.services.read;

import com.database.atypon.Node.model.Network;
import com.database.atypon.Node.model.Node;
import com.database.atypon.Node.security.ServiceTokens;
import com.database.atypon.Node.services.write.WriteService;
import com.database.atypon.Node.utils.concurrent.Broadcaster;
import com.database.atypon.Node.utils.quorum.ConsistencyConfig;
import com.database.atypon.Node.utils.quorum.Quorum;
import com.database.atypon.Node.utils.response.Response;
import com.database.atypon.Node.utils.response.ResponseType;
import com.database.atypon.Node.utils.ring.RingRouter;
import com.database.atypon.Node.utils.vclock.Reconcile;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Reads a document by id from every replica of its schema, reconciles the copies by vector clock, answers
 * with the winner and writes that winner back to every replica whose copy differs from it (read-repair).
 *
 * <p>Asking all replicas, rather than only the {@code R} that the read consistency requires, means the
 * answers always overlap the {@code W} replicas that acknowledged the latest write. So read-your-writes
 * ({@code W + R > N}) holds for document creates, updates and deletes: the latest acknowledged write is
 * among the copies reconciled. {@code R} then only says how many replicas should have answered: fewer is a
 * degraded read, which is still served from whatever was found.
 *
 * <p>A delete is a tombstone, a copy like any other, so it takes part in the reconcile by its clock. When the
 * winner is a tombstone the client is told "Document not found", and the tombstone is repaired onto the
 * replicas still holding the live document, so a replica that missed a delete stops resurrecting it. A
 * replica with no copy at all is healed from the winner: since a delete leaves a tombstone, not an absent
 * file, an absent copy only ever missed a write. Each replica applies the winner through the same causal
 * apply as replication, which re-checks under its own write lock, so a newer write or a delete that lands
 * after the gather is not undone. This applies to read-by-id only.
 */
@Service
public class QuorumReadService {

    private static final org.slf4j.Logger log =
            org.slf4j.LoggerFactory.getLogger(QuorumReadService.class);

    private static final int CONNECT_TIMEOUT_MILLIS = 2_000;
    private static final int READ_TIMEOUT_MILLIS = 5_000;
    private static final RestTemplate REST = restTemplate();

    /** One replica's answer: the node, and its stored document (live or tombstone), or null when it has none. */
    record Copy(String node, JSONObject doc) {
    }

    private final ReadService readService;
    private final WriteService writeService;

    public QuorumReadService(ReadService readService, WriteService writeService) {
        this.readService = readService;
        this.writeService = writeService;
    }

    /**
     * The winner of the copies held by the reachable replicas, reconciled by vector clock: the document
     * itself, or an ERROR "Document not found" when no replica holds it or the winner is a tombstone.
     * Replicas whose copy differs from the winner are repaired before this returns.
     */
    public Response readById(String db, String schema, String id) {
        int replicas = RingRouter.replicasFor(db, schema).size();
        int required = Quorum.required(ConsistencyConfig.readConsistency(), replicas);

        Map<String, Node> peers = new LinkedHashMap<>();
        for (Node peer : RingRouter.otherReplicas(db, schema)) {
            peers.put(peer.getName(), peer);
        }

        Copy local = localCopy(db, schema, id);
        List<Copy> gathered = new ArrayList<>();
        if (local != null) {
            gathered.add(local);
        }
        gathered.addAll(peerCopies(peers.values(), db, schema, id));

        if (gathered.size() < required) {
            log.warn("degraded read of {}.{}/{}: {} of {} replicas answered, {} required",
                    db, schema, id, gathered.size(), replicas, required);
        }

        JSONObject winner = winnerOf(gathered);
        if (winner == null) {
            return new Response(ResponseType.ERROR, "Document not found");
        }

        repair(winner, local, gathered, peers, db, schema, id);
        if (Reconcile.isTombstone(winner)) {
            return new Response(ResponseType.ERROR, "Document not found");
        }
        return new Response(ResponseType.SUCCESS, "Document fetched successfully", winner.toString());
    }

    /**
     * Folds {@link Reconcile#winner} across the copies, or null when none holds a document (an absent copy,
     * a null doc, never wins against a present one).
     *
     * <p>{@code Reconcile.winner} is not commutative over three or more mutually concurrent copies: it hands
     * back the winner with the merged clock, which can then cover a later copy and shield it from losing on
     * timestamp. So the result of a fold depends on its order. The copies are therefore folded in order of
     * their source node's name, each copy coming from a distinct node, which makes the winner depend only on
     * the copies held and not on how they were gathered, and so identical on every replica that repairs.
     */
    static JSONObject winnerOf(List<Copy> copies) {
        List<Copy> byNode = new ArrayList<>(copies);
        byNode.sort(Comparator.comparing(Copy::node, Comparator.nullsFirst(Comparator.naturalOrder())));
        JSONObject winner = null;
        for (Copy copy : byNode) {
            winner = Reconcile.winner(winner, copy.doc());
        }
        return winner;
    }

    /**
     * This node's own copy: the raw stored document, a tombstone included. A document that is not there is
     * an absent copy; one that is there but cannot be parsed is no usable answer, so it is left out (null).
     */
    private Copy localCopy(String db, String schema, String id) {
        String self = Network.self == null ? "self" : Network.self.getName();
        Response local = readService.fetchById(id, db, schema);
        if (local.getResponseType() == ResponseType.SUCCESS && local.getContent() instanceof String content) {
            JSONObject doc = parse(content);
            return doc == null ? null : new Copy(self, doc);
        }
        return new Copy(self, null);
    }

    /** What each reachable peer answered; a peer that cannot be reached is left out. */
    private List<Copy> peerCopies(Iterable<Node> peers, String db, String schema, String id) {
        List<Node> asked = new ArrayList<>();
        List<Supplier<Response>> tasks = new ArrayList<>();
        for (Node peer : peers) {
            asked.add(peer);
            tasks.add(() -> peerRead(peer, db, schema, id));
        }
        List<Response> answers = Broadcaster.broadcast(tasks);

        List<Copy> copies = new ArrayList<>();
        for (int i = 0; i < asked.size(); i++) {
            Response answer = answers.get(i);
            if (answer == null) {
                continue; // no usable answer
            }
            Node peer = asked.get(i);
            if (answer.getResponseType() == ResponseType.SUCCESS && answer.getContent() instanceof String content) {
                JSONObject doc = parse(content);
                if (doc != null) {
                    copies.add(new Copy(peer.getName(), doc));
                }
            } else {
                copies.add(new Copy(peer.getName(), null));
            }
        }
        return copies;
    }

    /**
     * One peer's own answer to a read of the document, asked as an internal call so the peer serves it from
     * its local store. Returns null when the peer cannot be reached or does not answer in the expected shape.
     */
    private Response peerRead(Node peer, String db, String schema, String id) {
        try {
            HttpHeaders headers = new HttpHeaders();
            headers.set("Authorization", ServiceTokens.bearer());
            Map<?, ?> body = REST.exchange(
                    peer.getURL() + "/user/read/document?id={id}&databaseName={db}&schemaName={schema}",
                    HttpMethod.GET, new HttpEntity<>(headers), Map.class, id, db, schema).getBody();
            if (body == null || body.get("responseType") == null) {
                return null;
            }
            return new Response(ResponseType.valueOf((String) body.get("responseType")),
                    (String) body.get("message"), body.get("content"));
        } catch (Exception e) {
            log.debug("read of {}.{}/{} from {} failed: {}", db, schema, id, peer.getName(), e.getMessage());
            return null;
        }
    }

    /**
     * Offers the winner to every replica whose copy differs from it by content, a replica with no copy
     * included. Best effort, and safe to over-send: a replica applies it through the causal apply, which
     * stores it only when that changes the replica's copy.
     */
    private void repair(JSONObject winner, Copy local, List<Copy> gathered, Map<String, Node> peers,
                        String db, String schema, String id) {
        List<Supplier<Response>> repairs = new ArrayList<>();
        for (Copy copy : gathered) {
            if (copy.doc() != null && copy.doc().similar(winner)) {
                continue; // already the winner
            }
            if (copy == local) {
                repairs.add(() -> repairLocal(winner, db, schema, id));
            } else {
                Node peer = peers.get(copy.node());
                repairs.add(() -> repairPeer(peer, winner, db, schema, id));
            }
        }
        Broadcaster.broadcast(repairs);
    }

    private Response repairLocal(JSONObject winner, String db, String schema, String id) {
        try {
            HashMap<String, Object> document = new HashMap<>(winner.toMap());
            Response result = writeService.repairDocument(db, schema, id, document);
            if (result.getResponseType() != ResponseType.SUCCESS) {
                log.warn("read-repair of {}.{}/{} here failed: {}", db, schema, id, result.getMessage());
            }
            return result;
        } catch (Exception e) {
            log.warn("read-repair of {}.{}/{} here failed", db, schema, id, e);
            return new Response(ResponseType.ERROR, String.valueOf(e.getMessage()));
        }
    }

    private Response repairPeer(Node peer, JSONObject winner, String db, String schema, String id) {
        try {
            HttpHeaders headers = new HttpHeaders();
            headers.set("Authorization", ServiceTokens.bearer());
            headers.setContentType(MediaType.APPLICATION_JSON);
            REST.exchange(
                    peer.getURL() + "/write/document/repair?database={db}&schema={schema}&id={id}",
                    HttpMethod.POST, new HttpEntity<>(winner.toString(), headers), String.class, db, schema, id);
            return new Response(ResponseType.SUCCESS, "Replica repaired");
        } catch (Exception e) {
            log.warn("read-repair of {}.{}/{} on {} failed: {}", db, schema, id, peer.getName(), e.getMessage());
            return new Response(ResponseType.ERROR, String.valueOf(e.getMessage()));
        }
    }

    /** The document in {@code content}, or null when it is not a JSON object. */
    private static JSONObject parse(String content) {
        try {
            return new JSONObject(content);
        } catch (JSONException e) {
            return null;
        }
    }

    private static RestTemplate restTemplate() {
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(CONNECT_TIMEOUT_MILLIS);
        factory.setReadTimeout(READ_TIMEOUT_MILLIS);
        return new RestTemplate(factory);
    }
}
