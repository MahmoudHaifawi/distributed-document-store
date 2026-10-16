package com.database.atypon.Node.controllers.write;

import com.database.atypon.Node.model.Node;
import com.database.atypon.Node.security.SecurityUtils;
import com.database.atypon.Node.services.write.WriteService;
import com.database.atypon.Node.utils.AffinityLoadBalancer;
import com.database.atypon.Node.utils.Forwarder;
import com.database.atypon.Node.utils.JsonKeys;
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

        Vector<Response> responses = new Vector<>();
        responses.add(writeService.createSchema(database, schema));

        if(isInternal())
            return responses;

        responses.addAll(writeService.broadcastSchema(database, schema));

        return responses;
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
        Vector<Response> responses = new Vector<>();
        if(affinityResponse.getResponseType() == ResponseType.ERROR){
            Node node = (Node) affinityResponse.getContent();
            return Forwarder.forwardForResponses(node,
                    "/write/document/new?database=" + database + "&schema=" + schema, document);
        }
        responses.add(writeService.createDocument(database, schema, document));

        if(responses.get(0).getResponseType() == ResponseType.SUCCESS)
            responses.addAll(writeService.broadcastDocument(database, schema, document));

        return responses;
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

        Vector<Response> responses = new Vector<>();
        Response result = writeService.updateDocument(database, schema, id, document, expectedVersion);
        responses.add(result);

        if (result.getResponseType() == ResponseType.SUCCESS) {
            // stamp the new version onto the document and replicate it verbatim to peers
            document.put("_version", ((Number) result.getContent()).intValue());
            responses.addAll(writeService.broadcastUpdate(database, schema, id, document));
        }
        return responses;
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

        Vector<Response> responses = new Vector<>();
        Response result = writeService.deleteDocument(database, schema, id, version);
        responses.add(result);
        if (result.getResponseType() == ResponseType.SUCCESS) {
            responses.addAll(writeService.broadcastDelete(database, schema, id));
        }
        return responses;
    }
}
