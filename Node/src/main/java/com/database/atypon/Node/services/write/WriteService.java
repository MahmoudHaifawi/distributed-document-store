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

    public Response createDocument(String database, String schema, HashMap<String, Object> document) {
        try{
            JSONObject documentJSON = new JSONObject(document);
            return writeOperation.createDocument(database, schema, documentJSON);
        } catch (Exception e){
            return new Response(ResponseType.ERROR, e.getMessage());
        }
    }

    public Response updateDocument(String database, String schema, String id,
                                   HashMap<String, Object> document, int expectedVersion) {
        try {
            return writeOperation.updateDocument(database, schema, id, new JSONObject(document), expectedVersion);
        } catch (Exception e) {
            return new Response(ResponseType.ERROR, e.getMessage());
        }
    }

    public Response applyUpdate(String database, String schema, String id, HashMap<String, Object> document) {
        try {
            return writeOperation.applyUpdate(database, schema, id, new JSONObject(document));
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
