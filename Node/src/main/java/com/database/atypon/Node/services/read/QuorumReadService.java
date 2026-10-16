package com.database.atypon.Node.services.read;

import com.database.atypon.Node.model.Network;
import com.database.atypon.Node.model.Node;
import com.database.atypon.Node.security.ServiceTokens;
import com.database.atypon.Node.services.write.WriteService;
import com.database.atypon.Node.utils.JsonKeys;
import com.database.atypon.Node.utils.concurrent.Broadcaster;
import com.database.atypon.Node.utils.quorum.ConsistencyConfig;
import com.database.atypon.Node.utils.quorum.Quorum;
import com.database.atypon.Node.utils.quorum.ReplicaDoc;
import com.database.atypon.Node.utils.response.Response;
import com.database.atypon.Node.utils.response.ResponseType;
import com.database.atypon.Node.utils.ring.RingRouter;
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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Reads a document by id from every replica of its schema, answers with the freshest copy and writes that
 * copy back to every replica that holds an older one (last-write-wins read-repair).
 *
 * <p>Asking all replicas, rather than only the {@code R} that the read consistency requires, means the
 * answers always overlap the {@code W} replicas that acknowledged the latest write. So read-your-writes
 * ({@code W + R > N}) holds for document creates and updates: the freshest acknowledged version is seen.
 * {@code R} then only says how many replicas should have answered: fewer is a degraded read, which is
 * still served from whatever was found.
 *
 * <p>Deletes are not reconciled. There are no tombstones yet, so a replica that missed a delete can still
 * return the document. Read-repair only upgrades an older copy that is still present: a replica that
 * answers "not found" is not written to, and the repair itself only upgrades a document that is still
 * present (and older) on the replica when it is applied, rather than upserting it, so it never re-creates
 * a missing document. This applies to read-by-id only.
 */
@Service
public class QuorumReadService {

    private static final org.slf4j.Logger log =
            org.slf4j.LoggerFactory.getLogger(QuorumReadService.class);

    private static final int CONNECT_TIMEOUT_MILLIS = 2_000;
    private static final int READ_TIMEOUT_MILLIS = 5_000;
    private static final RestTemplate REST = restTemplate();

    private final ReadService readService;
    private final WriteService writeService;

    public QuorumReadService(ReadService readService, WriteService writeService) {
        this.readService = readService;
        this.writeService = writeService;
    }

    /**
     * The freshest copy of the document held by any reachable replica, or an ERROR "Document not found"
     * when none holds it. Replicas holding an older copy are repaired before this returns.
     */
    public Response readById(String db, String schema, String id) {
        int replicas = RingRouter.replicasFor(db, schema).size();
        int required = Quorum.required(ConsistencyConfig.readConsistency(), replicas);

        Map<String, Node> peers = new LinkedHashMap<>();
        for (Node peer : RingRouter.otherReplicas(db, schema)) {
            peers.put(peer.getName(), peer);
        }

        ReplicaDoc local = localCopy(db, schema, id);
        List<ReplicaDoc> gathered = new ArrayList<>();
        gathered.add(local);
        gathered.addAll(peerCopies(peers.values(), db, schema, id));

        if (gathered.size() < required) {
            log.warn("degraded read of {}.{}/{}: {} of {} replicas answered, {} required",
                    db, schema, id, gathered.size(), replicas, required);
        }

        ReplicaDoc winner = Quorum.pickFreshest(gathered);
        if (winner == null) {
            return new Response(ResponseType.ERROR, "Document not found");
        }

        repair(winner, local, gathered, peers, db, schema, id);
        return new Response(ResponseType.SUCCESS, "Document fetched successfully", winner.content);
    }

    private ReplicaDoc localCopy(String db, String schema, String id) {
        String self = Network.self == null ? "self" : Network.self.getName();
        Response local = readService.fetchById(id, db, schema);
        if (local.getResponseType() == ResponseType.SUCCESS && local.getContent() instanceof String content) {
            return new ReplicaDoc(self, true, versionOf(content), content);
        }
        return new ReplicaDoc(self, false, 0, null);
    }

    /** What each reachable peer answered; a peer that cannot be reached is left out. */
    private List<ReplicaDoc> peerCopies(Iterable<Node> peers, String db, String schema, String id) {
        List<Node> asked = new ArrayList<>();
        List<Supplier<Response>> tasks = new ArrayList<>();
        for (Node peer : peers) {
            asked.add(peer);
            tasks.add(() -> peerRead(peer, db, schema, id));
        }
        List<Response> answers = Broadcaster.broadcast(tasks);

        List<ReplicaDoc> copies = new ArrayList<>();
        for (int i = 0; i < asked.size(); i++) {
            Response answer = answers.get(i);
            if (answer == null) {
                continue; // no usable answer
            }
            Node peer = asked.get(i);
            if (answer.getResponseType() == ResponseType.SUCCESS && answer.getContent() instanceof String content) {
                copies.add(new ReplicaDoc(peer.getName(), true, versionOf(content), content));
            } else {
                copies.add(new ReplicaDoc(peer.getName(), false, 0, null));
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
     * Offers the winning document to every replica that held an older copy of it. Best effort. Each replica
     * applies it through the conditional repair, which re-checks under its own write lock that the document
     * is still there and still older, so a delete or a newer write that lands after the gather is not undone.
     */
    private void repair(ReplicaDoc winner, ReplicaDoc local, List<ReplicaDoc> gathered, Map<String, Node> peers,
                        String db, String schema, String id) {
        List<Supplier<Response>> repairs = new ArrayList<>();
        for (ReplicaDoc copy : Quorum.staleTargets(gathered, winner)) {
            if (copy == local) {
                repairs.add(() -> repairLocal(winner, db, schema, id));
            } else {
                Node peer = peers.get(copy.node);
                repairs.add(() -> repairPeer(peer, winner, db, schema, id));
            }
        }
        Broadcaster.broadcast(repairs);
    }

    private Response repairLocal(ReplicaDoc winner, String db, String schema, String id) {
        try {
            HashMap<String, Object> document = new HashMap<>(new JSONObject(winner.content).toMap());
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

    private Response repairPeer(Node peer, ReplicaDoc winner, String db, String schema, String id) {
        try {
            HttpHeaders headers = new HttpHeaders();
            headers.set("Authorization", ServiceTokens.bearer());
            headers.setContentType(MediaType.APPLICATION_JSON);
            REST.exchange(
                    peer.getURL() + "/write/document/repair?database={db}&schema={schema}&id={id}",
                    HttpMethod.POST, new HttpEntity<>(winner.content, headers), String.class, db, schema, id);
            return new Response(ResponseType.SUCCESS, "Replica repaired");
        } catch (Exception e) {
            log.warn("read-repair of {}.{}/{} on {} failed: {}", db, schema, id, peer.getName(), e.getMessage());
            return new Response(ResponseType.ERROR, String.valueOf(e.getMessage()));
        }
    }

    /** The {@code _version} stamped on a stored document; a document that carries none is at version 1. */
    private static int versionOf(String content) {
        try {
            return new JSONObject(content).optInt(JsonKeys.VERSION, 1);
        } catch (JSONException e) {
            return 1;
        }
    }

    private static RestTemplate restTemplate() {
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(CONNECT_TIMEOUT_MILLIS);
        factory.setReadTimeout(READ_TIMEOUT_MILLIS);
        return new RestTemplate(factory);
    }
}
