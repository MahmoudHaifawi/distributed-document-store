package com.database.atypon.Node.controllers.write;

import com.database.atypon.Node.model.Node;
import com.database.atypon.Node.security.SecurityUtils;
import com.database.atypon.Node.services.write.WriteService;
import com.database.atypon.Node.utils.AffinityLoadBalancer;
import com.database.atypon.Node.utils.Forwarder;
import com.database.atypon.Node.utils.JsonKeys;
import com.database.atypon.Node.utils.quorum.ConsistencyConfig;
import com.database.atypon.Node.utils.quorum.Quorum;
import com.database.atypon.Node.utils.response.Response;
import com.database.atypon.Node.utils.response.ResponseType;
import com.database.atypon.Node.utils.ring.RingRouter;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Vector;

@RestController
@RequestMapping("/write")
public class WriteController {

    private final WriteService writeService;

    public WriteController(WriteService writeService) {
        this.writeService = writeService;
    }

    private static boolean isInternal() {
        return "INTERNAL".equals(SecurityUtils.currentRole());
    }

    @PostMapping(value = "/schema/new", produces = "application/json")
    public Vector<Response> createSchema(@RequestParam String database,
                                       @RequestBody HashMap<String, Object> schema) {

        if(schema == null || schema.isEmpty())
            return new Vector<>(List.of(new Response(ResponseType.ERROR, "Schema is empty")));
        if(database == null || database.isEmpty())
            return new Vector<>(List.of(new Response(ResponseType.ERROR, "Database name is empty")));

        if (!isInternal()) {
            // a client write goes to the schema's primary, which applies it and copies it to the replicas
            Object schemaName = schema.get(JsonKeys.SCHEMA_NAME);
            if (schemaName == null)
                return new Vector<>(List.of(new Response(ResponseType.ERROR, "Schema name is empty")));
            if (!RingRouter.isPrimary(database, schemaName.toString()))
                return Forwarder.forwardForResponses(RingRouter.primaryFor(database, schemaName.toString()),
                        "/write/schema/new?database=" + database, schema);
        }

        Response localResult = writeService.createSchema(database, schema);

        if(isInternal())
            return new Vector<>(List.of(localResult));

        String schemaName = schema.get(JsonKeys.SCHEMA_NAME).toString();
        return withWriteQuorum(database, schemaName, localResult, writeService.broadcastSchema(database, schema));
    }

    @PostMapping(value = "/document/new", produces = "application/json")
    public Vector<Response> createDocument(@RequestParam String database,
                                         @RequestParam String schema,
                                         @RequestBody HashMap<String, Object> document) {
        if(document == null || document.isEmpty())
            return new Vector<>(List.of(new Response(ResponseType.ERROR, "Document is empty")));
        if(database == null || database.isEmpty())
            return new Vector<>(List.of(new Response(ResponseType.ERROR, "Database name is empty")));
        if(schema == null || schema.isEmpty())
            return new Vector<>(List.of(new Response(ResponseType.ERROR, "Schema name is empty")));

        if(isInternal())
            return new Vector<>(List.of(writeService.createDocument(database, schema, document)));

        //forward the request to another node if the node affinity is not the current node
        Response affinityResponse = AffinityLoadBalancer.checkAffinity(database, schema);
        if(affinityResponse.getResponseType() == ResponseType.ERROR){
            Node node = (Node) affinityResponse.getContent();
            return Forwarder.forwardForResponses(node,
                    "/write/document/new?database=" + database + "&schema=" + schema, document);
        }
        Response localResult = writeService.createDocument(database, schema, document);

        List<Response> replicaResponses = localResult.getResponseType() == ResponseType.SUCCESS
                ? writeService.broadcastDocument(database, schema, document)
                : List.of();
        return withWriteQuorum(database, schema, localResult, replicaResponses);
    }

    @PostMapping(value = "/document/update", produces = "application/json")
    public Vector<Response> updateDocument(@RequestParam String database,
                                           @RequestParam String schema,
                                           @RequestParam String id,
                                           @RequestBody HashMap<String, Object> document) {
        if (isInternal())
            return new Vector<>(List.of(writeService.applyUpdate(database, schema, id, document)));

        if (!RingRouter.isPrimary(database, schema))
            return Forwarder.forwardForResponses(RingRouter.primaryFor(database, schema),
                    "/write/document/update?database=" + database + "&schema=" + schema + "&id=" + id, document);

        int expectedVersion = 1;
        Object v = document.get("_version");
        if (v instanceof Number)
            expectedVersion = ((Number) v).intValue();

        Response result = writeService.updateDocument(database, schema, id, document, expectedVersion);

        List<Response> replicaResponses = List.of();
        if (result.getResponseType() == ResponseType.SUCCESS) {
            // stamp the new version onto the document and replicate it verbatim to peers
            document.put("_version", ((Number) result.getContent()).intValue());
            replicaResponses = writeService.broadcastUpdate(database, schema, id, document);
        }
        return withWriteQuorum(database, schema, result, replicaResponses);
    }

    /**
     * Read-repair, called by a peer that read a fresher copy of the document than the one here. Reachable
     * by internal callers only (see SecurityConfig). It upgrades a present, older document and does nothing
     * otherwise, so unlike {@code /document/update} it can never recreate a deleted document.
     */
    @PostMapping(value = "/document/repair", produces = "application/json")
    public Vector<Response> repairDocument(@RequestParam String database,
                                           @RequestParam String schema,
                                           @RequestParam String id,
                                           @RequestBody HashMap<String, Object> winner) {
        return new Vector<>(List.of(writeService.repairDocument(database, schema, id, winner)));
    }

    @PostMapping(value = "/document/delete", produces = "application/json")
    public Vector<Response> deleteDocument(@RequestParam String database,
                                           @RequestParam String schema,
                                           @RequestParam String id,
                                           @RequestParam(required = false, defaultValue = "0") int version) {
        if (isInternal())
            return new Vector<>(List.of(writeService.applyDelete(database, schema, id)));

        if (!RingRouter.isPrimary(database, schema))
            return Forwarder.forwardForResponses(RingRouter.primaryFor(database, schema),
                    "/write/document/delete?database=" + database + "&schema=" + schema + "&id=" + id
                            + "&version=" + version, null);

        Response result = writeService.deleteDocument(database, schema, id, version);

        List<Response> replicaResponses = result.getResponseType() == ResponseType.SUCCESS
                ? writeService.broadcastDelete(database, schema, id)
                : List.of();
        return withWriteQuorum(database, schema, result, replicaResponses);
    }

    /**
     * The answer to a client write this node applied and copied to the schema's other replicas: this
     * node's own result first, then each replica's. When the local apply succeeded but fewer than W of the
     * schema's N replicas (this node counted as one) persisted the write, the first response is replaced
     * by an ERROR so the client is not told "done" for a write that has not reached a quorum. A failed
     * local apply is left as it is. With a single replica, W is 1 and the local apply is the one ack.
     */
    private Vector<Response> withWriteQuorum(String database, String schema, Response localResult,
                                             List<Response> replicaResponses) {
        Vector<Response> responses = new Vector<>();
        responses.add(localResult);
        responses.addAll(replicaResponses);

        if (localResult.getResponseType() == ResponseType.SUCCESS) {
            int replicas = RingRouter.replicasFor(database, schema).size();
            int w = Quorum.required(ConsistencyConfig.writeConsistency(), replicas);
            responses.set(0, Quorum.writeClientResult(true, localResult, replicaResponses, w));
        }
        return responses;
    }
}
