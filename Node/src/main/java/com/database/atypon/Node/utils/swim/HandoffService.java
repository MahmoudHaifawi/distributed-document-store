package com.database.atypon.Node.utils.swim;

import com.database.atypon.Node.controllers.HandoffController;
import com.database.atypon.Node.model.Network;
import com.database.atypon.Node.model.Node;
import com.database.atypon.Node.operations.write.WriteOperation;
import com.database.atypon.Node.security.ServiceTokens;
import com.database.atypon.Node.utils.PathBuilder;
import com.database.atypon.Node.utils.response.Response;
import com.database.atypon.Node.utils.response.ResponseType;
import com.database.atypon.Node.utils.ring.RingRouter;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.File;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.regex.Pattern;

/**
 * State handoff (node role only): when a membership change makes this node a replica of a schema it does not
 * hold, it pulls that schema from a node that does and merges it, so that the node which just joined serves
 * its share of the data at once instead of waiting for reads to heal it document by document.
 *
 * <p><b>One pass per change.</b> The service listens to {@link MembershipTable#addChangeListener}. A change
 * only queues a pass on a thread of its own, never running it on the thread that changed the table (the
 * failure detector or a gossip handler, which must not sit through an HTTP transfer), and changes that arrive
 * before the queued pass starts share it. {@link #onChange()} is that pass: it asks every live peer which
 * schemas it holds ({@code GET /swim/schemas}), keeps those the ring now makes this node a replica of, and for
 * each of them that has not already been moved here pulls the whole schema ({@code GET /swim/pull}) from one
 * node that holds it. Nothing is repeated for a schema that was handed over by an earlier pass and is still
 * this node's, so the work of a pass is bounded by the schemas that actually moved, not by what the node
 * holds; a schema that fails to transfer is simply tried again at the next change. A node that has lost all
 * its peers forgets what it handed over, and so catches up again when it is back in a cluster.
 *
 * <p><b>Source.</b> A replica of the schema on the ring, other than this node, if one of those holds it; else
 * any peer that does (with a replication factor of one, say, the old owner is no longer on the ring but
 * still has the data). If a pull fails the next candidate is tried.
 *
 * <p><b>Merge.</b> Every document, tombstones included, goes through {@link WriteOperation#applyReplicated},
 * the causal apply that replication and read-repair use: it stores the winner of the pulled and the local
 * copy only if that differs from the local one, so a newer local write is never overwritten, a delete is
 * carried and cannot be undone by an older live copy, and applying the same copy again changes nothing.
 * The pull is read raw from the source's disk for the same reason ({@link HandoffController}). Before the
 * documents, {@link WriteOperation#adoptSchema} gives the schema a home here (data directory, definition, id
 * counter).
 *
 * <p>A single node has no peers and so nothing to pull from: the pass does nothing.
 */
@Component
@Profile("node")
public class HandoffService {

    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(HandoffService.class);

    private static final int CONNECT_TIMEOUT_MS = 2_000;
    private static final int TRANSFER_TIMEOUT_MS = 60_000;
    private static final Pattern DOCUMENT_ID = Pattern.compile("\\d{1,9}");

    private final MembershipTable membership;
    private final WriteOperation writeOperation;
    private final Supplier<String> bearer;
    private final RestTemplate rest;
    private final Executor executor;

    /** Set while a pass is queued and has not started; a change in that time is already covered by it. */
    private final AtomicBoolean passQueued = new AtomicBoolean();
    private final Runnable listener = this::requestPass;

    /** The {@code database.schema} keys moved to this node by an earlier pass and still owned by it. */
    private final Set<String> handedOff = ConcurrentHashMap.newKeySet();

    @Autowired
    public HandoffService(MembershipTable membership, WriteOperation writeOperation) {
        this(membership, writeOperation, ServiceTokens::bearer, TRANSFER_TIMEOUT_MS, Executors.newSingleThreadExecutor(runnable -> {
            Thread thread = new Thread(runnable, "state-handoff");
            thread.setDaemon(true);
            return thread;
        }));
    }

    /** Test seam: {@code bearer} supplies the Authorization header, and {@code executor} runs the queued passes. */
    HandoffService(MembershipTable membership, WriteOperation writeOperation, Supplier<String> bearer,
                   long transferTimeoutMs, Executor executor) {
        this.membership = membership;
        this.writeOperation = writeOperation;
        this.bearer = bearer;
        this.executor = executor;
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(CONNECT_TIMEOUT_MS);
        factory.setReadTimeout((int) Math.max(1L, Math.min(transferTimeoutMs, Integer.MAX_VALUE)));
        this.rest = new RestTemplate(factory);
    }

    /** Starts listening to membership changes. */
    @PostConstruct
    public void start() {
        membership.addChangeListener(listener);
    }

    /** Stops listening, and stops the thread that runs the passes. */
    @PreDestroy
    public void stop() {
        membership.removeChangeListener(listener);
        if (executor instanceof ExecutorService service) {
            service.shutdownNow();
        }
    }

    /** The membership listener: asks for a pass, unless one is already queued and has yet to start. Never blocks. */
    void requestPass() {
        if (!passQueued.compareAndSet(false, true)) {
            return;
        }
        try {
            executor.execute(() -> {
                passQueued.set(false); // from here a new change queues the next pass
                try {
                    onChange();
                } catch (RuntimeException e) {
                    log.warn("state handoff pass failed: {}", e.toString());
                }
            });
        } catch (RejectedExecutionException e) {
            passQueued.set(false); // shutting down
        }
    }

    /**
     * One handoff pass, for the membership as it is now. Safe to call at any time and as often as you like:
     * passes do not overlap, and a pass that finds everything already here does nothing.
     */
    public synchronized void onChange() {
        List<Node> peers = livePeers();
        if (peers.isEmpty()) {
            handedOff.clear();
            return; // a node on its own owns everything and has nobody to ask
        }
        Map<String, List<Node>> holders = whoHoldsWhat(peers);
        handedOff.removeIf(key -> !stillOwned(key));

        int moved = 0;
        for (Map.Entry<String, List<Node>> entry : holders.entrySet()) {
            String key = entry.getKey();
            String[] name = split(key);
            if (name == null) {
                continue;
            }
            try {
                if (!RingRouter.isReplica(name[0], name[1])) {
                    continue;
                }
                if (handedOff.contains(key) && holdsLocally(name[0], name[1])) {
                    continue;
                }
                if (handOff(name[0], name[1], sourcesFor(name[0], name[1], entry.getValue()))) {
                    handedOff.add(key);
                    moved++;
                }
            } catch (RuntimeException e) {
                log.warn("state handoff of {} failed: {}", key, e.toString());
            }
        }
        if (moved > 0) {
            log.info("state handoff: {} schema(s) moved to this node", moved);
        }
    }

    // ---- who has what -------------------------------------------------------------------------

    /** The live members other than this node, as of now. */
    private static List<Node> livePeers() {
        List<Node> members = Network.nodes;
        Node me = Network.self;
        if (members == null) {
            return List.of();
        }
        List<Node> peers = new ArrayList<>();
        synchronized (members) {
            for (Node member : members) {
                if (me == null || !me.getName().equals(member.getName())) {
                    peers.add(member);
                }
            }
        }
        return peers;
    }

    /** Every schema key any peer says it holds, with the peers that hold it. A peer that cannot be asked is skipped. */
    private Map<String, List<Node>> whoHoldsWhat(List<Node> peers) {
        Map<String, List<Node>> holders = new LinkedHashMap<>();
        for (Node peer : peers) {
            try {
                URI uri = UriComponentsBuilder.fromHttpUrl(peer.getURL()).path("/swim/schemas").build().toUri();
                JSONArray keys = new JSONArray(get(uri));
                for (int i = 0; i < keys.length(); i++) {
                    holders.computeIfAbsent(keys.getString(i), key -> new ArrayList<>()).add(peer);
                }
            } catch (RuntimeException e) {
                log.debug("state handoff: {} did not say which schemas it holds: {}", peer.getName(), e.toString());
            }
        }
        return holders;
    }

    /**
     * The nodes to pull {@code database.schema} from, best first: the schema's other replicas on the ring that
     * hold it, in preference order, then any other peer that holds it.
     */
    private static List<Node> sourcesFor(String database, String schema, List<Node> holders) {
        List<Node> sources = new ArrayList<>();
        for (Node replica : RingRouter.otherReplicas(database, schema)) {
            for (Node holder : holders) {
                if (holder.getName().equals(replica.getName()) && !contains(sources, holder)) {
                    sources.add(holder);
                }
            }
        }
        for (Node holder : holders) {
            if (!contains(sources, holder)) {
                sources.add(holder);
            }
        }
        return sources;
    }

    private static boolean contains(List<Node> nodes, Node node) {
        return nodes.stream().anyMatch(candidate -> candidate.getName().equals(node.getName()));
    }

    private static boolean stillOwned(String key) {
        String[] name = split(key);
        if (name == null) {
            return false;
        }
        try {
            return RingRouter.isReplica(name[0], name[1]);
        } catch (RuntimeException e) {
            return true; // cannot tell: keep it, the worst case is one skipped pull
        }
    }

    private static boolean holdsLocally(String database, String schema) {
        return new File(PathBuilder.getPathToSchema(database, schema)).isFile()
                && new File(PathBuilder.getPathToAllDocuments(database, schema)).isDirectory();
    }

    /** {@code [database, schema]} of a {@code database.schema} key, or null if it is not one or names something unsafe. */
    private static String[] split(String key) {
        int dot = key.indexOf('.');
        if (dot <= 0 || dot == key.length() - 1) {
            return null;
        }
        String database = key.substring(0, dot);
        String schema = key.substring(dot + 1);
        return HandoffController.isSafeName(database) && HandoffController.isSafeName(schema)
                ? new String[]{database, schema} : null;
    }

    // ---- the transfer -------------------------------------------------------------------------

    /** Pulls the schema from the first source that answers and applies it. True if it all went in. */
    private boolean handOff(String database, String schema, List<Node> sources) {
        for (Node source : sources) {
            JSONObject pulled = pull(source, database, schema);
            if (pulled != null) {
                return apply(source, database, schema, pulled);
            }
        }
        return false;
    }

    private JSONObject pull(Node source, String database, String schema) {
        try {
            URI uri = UriComponentsBuilder.fromHttpUrl(source.getURL()).path("/swim/pull")
                    .queryParam("database", "{database}").queryParam("schema", "{schema}")
                    .encode().buildAndExpand(database, schema).toUri();
            JSONObject pulled = new JSONObject(get(uri));
            if (pulled.optJSONObject(HandoffController.DOCUMENTS) == null
                    || pulled.optJSONObject(HandoffController.DEFINITION) == null) {
                // Documents without the schema's definition are not a snapshot to adopt: this node would hold the
                // data and still be unable to serve or replicate-create in the schema. Ask the next source instead.
                log.warn("state handoff: the pull of {}.{} from {} is incomplete (no definition or no documents), ignoring it",
                        database, schema, source.getName());
                return null;
            }
            return pulled;
        } catch (RuntimeException e) {
            log.warn("state handoff: pulling {}.{} from {} failed: {}", database, schema, source.getName(), e.toString());
            return null;
        }
    }

    /**
     * Gives the schema a home, then applies every document of it through {@link WriteOperation#applyReplicated}.
     * A document that cannot be applied is logged and left for the next pass; the rest still go in.
     */
    private boolean apply(Node source, String database, String schema, JSONObject pulled) {
        JSONObject documents = pulled.getJSONObject(HandoffController.DOCUMENTS);
        List<String> ids = new ArrayList<>();
        int nextIdFloor = 0;
        for (String id : documents.keySet()) {
            if (DOCUMENT_ID.matcher(id).matches() && documents.optJSONObject(id) != null) {
                ids.add(id);
                nextIdFloor = Math.max(nextIdFloor, Integer.parseInt(id) + 1);
            }
        }
        Response adopted = writeOperation.adoptSchema(database, schema,
                pulled.optJSONObject(HandoffController.DEFINITION), nextIdFloor);
        if (adopted.getResponseType() == ResponseType.ERROR) {
            log.warn("state handoff: cannot give {}.{} a home here: {}", database, schema, adopted.getMessage());
            return false;
        }
        if (!new File(PathBuilder.getPathToSchema(database, schema)).isFile()) {
            // Never store documents of a schema this node cannot serve: it would look handed over and not be.
            log.warn("state handoff: {}.{} still has no definition here, not applying its documents", database, schema);
            return false;
        }

        boolean complete = ids.size() == documents.length();
        int applied = 0;
        for (String id : ids) {
            Response result = writeOperation.applyReplicated(database, schema, id, documents.getJSONObject(id));
            if (result.getResponseType() == ResponseType.ERROR) {
                log.warn("state handoff: {}.{}/{} from {} was not applied: {}", database, schema, id, source.getName(),
                        result.getMessage());
                complete = false;
            } else if (Boolean.TRUE.equals(result.getContent())) {
                applied++;
            }
        }
        log.info("state handoff: {}.{} from {}: {} document(s) pulled, {} changed here", database, schema,
                source.getName(), ids.size(), applied);
        return complete;
    }

    private String get(URI uri) {
        HttpHeaders headers = new HttpHeaders();
        headers.set("Authorization", bearer.get());
        byte[] body = rest.exchange(uri, HttpMethod.GET, new HttpEntity<>(headers), byte[].class).getBody();
        return body == null ? "" : new String(body, StandardCharsets.UTF_8);
    }
}
