package com.database.atypon.Node.utils;

import com.database.atypon.Node.model.Network;
import com.database.atypon.Node.utils.response.Response;
import com.database.atypon.Node.utils.response.ResponseType;
import com.database.atypon.Node.utils.ring.RingRouter;

public class AffinityLoadBalancer {

    public static Response checkAffinity(String database, String schema){
        // Single-node cluster (no peers): this node owns every schema, so there is nothing to route
        // to. Short-circuit before consulting the affinity file or self name, which otherwise assume
        // the Docker-style "Node0".. identities and would wrongly forward writes to an unreachable peer.
        if (Network.nodes == null || Network.nodes.isEmpty())
            return new Response(ResponseType.SUCCESS, "Single node owns all schemas");

        // The schema's primary is wherever the consistent-hash ring places it; no per-schema file to consult.
        if (RingRouter.isPrimary(database, schema))
            return new Response(ResponseType.SUCCESS, "Affinity is correct");
        return new Response(ResponseType.ERROR, "Affinity is incorrect", RingRouter.primaryFor(database, schema));
    }
}
