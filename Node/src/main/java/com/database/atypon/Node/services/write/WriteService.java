package com.database.atypon.Node.services.write;

import com.database.atypon.Node.model.Node;
import com.database.atypon.Node.operations.write.WriteOperation;
import com.database.atypon.Node.utils.JsonKeys;
import com.database.atypon.Node.utils.concurrent.Broadcaster;
import com.database.atypon.Node.utils.response.Response;
import com.database.atypon.Node.utils.response.ResponseType;
import com.database.atypon.Node.utils.ring.RingRouter;
import org.json.JSONObject;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.function.Supplier;

@Service
public class WriteService {

    private static final org.slf4j.Logger log =
            org.slf4j.LoggerFactory.getLogger(WriteService.class);

    private final WriteOperation writeOperation;

    public WriteService(WriteOperation writeOperation) {
        this.writeOperation = writeOperation;
    }

    public Response createSchema(String database, HashMap<String, Object> schema) {
            JSONObject schemaJSON = new JSONObject(schema);
            String schemaName = schemaJSON.get(JsonKeys.SCHEMA_NAME).toString();

            JSONObject schemaDetails = schemaJSON.getJSONObject(JsonKeys.SCHEMA);
            return writeOperation.createSchema(database, schemaName, schemaDetails);
    }

    /**
     * Origin create. On success the stamp this node gave the document (version, vector clock, timestamp) is
     * copied back onto {@code document}, so the caller can replicate that same map and every replica stores the
     * origin's stamp instead of making its own.
     */
    public Response createDocument(String database, String schema, HashMap<String, Object> document) {
        try{
            JSONObject documentJSON = new JSONObject(document);
            Response response = writeOperation.createDocument(database, schema, documentJSON);
            if (response.getResponseType() == ResponseType.SUCCESS)
                copyOriginStamp(documentJSON, document);
            return response;
        } catch (Exception e){
            return new Response(ResponseType.ERROR, e.getMessage());
        }
    }

    /** Replica side of a create: keeps the stamp the origin put on {@code document}. */
    public Response createReplicatedDocument(String database, String schema, HashMap<String, Object> document) {
        try {
            return writeOperation.createReplicatedDocument(database, schema, new JSONObject(document));
        } catch (Exception e) {
            return new Response(ResponseType.ERROR, e.getMessage());
        }
    }

    /**
     * Origin update. As with {@link #createDocument}, the new version, clock and timestamp are copied back onto
     * {@code document} on success so the caller replicates the stamped document.
     */
    public Response updateDocument(String database, String schema, String id,
                                   HashMap<String, Object> document, int expectedVersion) {
        try {
            JSONObject documentJSON = new JSONObject(document);
            Response response = writeOperation.updateDocument(database, schema, id, documentJSON, expectedVersion);
            if (response.getResponseType() == ResponseType.SUCCESS)
                copyOriginStamp(documentJSON, document);
            return response;
        } catch (Exception e) {
            return new Response(ResponseType.ERROR, e.getMessage());
        }
    }

    /** Copies the server-controlled keys of a stamped document onto the map that is replicated to peers. */
    private static void copyOriginStamp(JSONObject stamped, HashMap<String, Object> into) {
        for (String key : List.of(JsonKeys.VERSION, JsonKeys.VCLOCK, JsonKeys.TS)) {
            if (!stamped.has(key))
                continue;
            Object value = stamped.get(key);
            into.put(key, value instanceof JSONObject ? ((JSONObject) value).toMap() : value);
        }
        into.remove(JsonKeys.DELETED);
    }

    public Response applyUpdate(String database, String schema, String id, HashMap<String, Object> document) {
        try {
            return writeOperation.applyUpdate(database, schema, id, new JSONObject(document));
        } catch (Exception e) {
            return new Response(ResponseType.ERROR, e.getMessage());
        }
    }

    public Response repairDocument(String database, String schema, String id, HashMap<String, Object> winner) {
        try {
            return writeOperation.repairDocument(database, schema, id, new JSONObject(winner));
        } catch (Exception e) {
            return new Response(ResponseType.ERROR, e.getMessage());
        }
    }

    public Response deleteDocument(String database, String schema, String id, int expectedVersion) {
        try {
            return writeOperation.deleteDocument(database, schema, id, expectedVersion);
        } catch (Exception e) {
            return new Response(ResponseType.ERROR, e.getMessage());
        }
    }

    public Response applyDelete(String database, String schema, String id) {
        try {
            return writeOperation.applyDelete(database, schema, id);
        } catch (Exception e) {
            return new Response(ResponseType.ERROR, e.getMessage());
        }
    }

    public List<Response> broadcastSchema(String database, HashMap<String, Object> schema) {
        List<Supplier<Response>> tasks = new ArrayList<>();
        String schemaName = new JSONObject(schema).get(JsonKeys.SCHEMA_NAME).toString();
        for (Node node : replicaTargets(database, schemaName)) {
            tasks.add(() -> node.createSchema(database, schema));
        }
        return Broadcaster.broadcast(tasks);
    }

    public List<Response> broadcastDocument(String database, String schema, HashMap<String, Object> document) {
        List<Supplier<Response>> tasks = new ArrayList<>();
        for (Node node : replicaTargets(database, schema)) {
            tasks.add(() -> node.createDocument(database, schema, document));
        }
        return Broadcaster.broadcast(tasks);
    }

    public List<Response> broadcastUpdate(String database, String schema, String id,
                                          HashMap<String, Object> document) {
        List<Supplier<Response>> tasks = new ArrayList<>();
        for (Node node : replicaTargets(database, schema)) {
            tasks.add(() -> node.updateDocument(database, schema, id, document));
        }
        return Broadcaster.broadcast(tasks);
    }

    public List<Response> broadcastDelete(String database, String schema, String id) {
        List<Supplier<Response>> tasks = new ArrayList<>();
        for (Node node : replicaTargets(database, schema)) {
            tasks.add(() -> node.deleteDocument(database, schema, id));
        }
        return Broadcaster.broadcast(tasks);
    }

    /**
     * The peers a write to this schema must be copied to: the schema's other replicas on the ring, not
     * every node in the cluster. Empty on a single node.
     */
    List<Node> replicaTargets(String database, String schema) {
        return RingRouter.otherReplicas(database, schema);
    }
}
