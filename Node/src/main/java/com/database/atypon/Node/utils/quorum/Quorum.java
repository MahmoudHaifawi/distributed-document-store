package com.database.atypon.Node.utils.quorum;

import com.database.atypon.Node.utils.response.Response;
import com.database.atypon.Node.utils.response.ResponseType;

import java.util.List;

/** Pure quorum arithmetic shared by the write and read paths. */
public final class Quorum {

    /** The start of the message of the ERROR a write that missed its quorum is answered with. */
    private static final String QUORUM_NOT_MET = "write quorum not met";

    private Quorum() {
    }

    /**
     * Number of replica acknowledgements that satisfy {@code level} when the schema has {@code n}
     * replicas. A non-positive {@code n} is treated as 1, and the result is always within
     * {@code [1, max(1, n)]}.
     */
    public static int required(ConsistencyLevel level, int n) {
        int replicas = Math.max(1, n);
        int needed = switch (level) {
            case ONE -> 1;
            case QUORUM -> replicas / 2 + 1;
            case ALL -> replicas;
        };
        return Math.min(Math.max(needed, 1), replicas);
    }

    /**
     * Total acknowledgements for a write: one for the local apply if it succeeded, plus one for
     * every replica response of type {@link ResponseType#SUCCESS}.
     */
    public static int ackCount(boolean localSuccess, List<Response> replicaResponses) {
        int acks = localSuccess ? 1 : 0;
        for (Response response : replicaResponses) {
            if (response != null && response.getResponseType() == ResponseType.SUCCESS) {
                acks++;
            }
        }
        return acks;
    }

    /**
     * What the client is told for a write that this node applied and copied to its replicas. A failed
     * local apply is controlling and is returned as is. Otherwise the write is acknowledged only when at
     * least {@code w} replicas (this node included) persisted it; if not, the client gets an ERROR saying
     * the write quorum was not met, even though the local apply itself succeeded.
     */
    public static Response writeClientResult(boolean localSuccess, Response localResult,
                                             List<Response> replicaResponses, int w) {
        if (!localSuccess) {
            return localResult;
        }
        int acks = ackCount(true, replicaResponses);
        if (acks >= w) {
            return localResult;
        }
        return new Response(ResponseType.ERROR, QUORUM_NOT_MET + " (" + acks + "/" + w + ")");
    }

    /**
     * Whether {@code response} is the ERROR {@link #writeClientResult} gives a write that did not reach its
     * quorum, as opposed to any other failure. It is how a node that handed a write to the primary can tell,
     * from the answer it got back, that the write was applied there but not acknowledged by enough replicas.
     */
    public static boolean isWriteQuorumFailure(Response response) {
        return response != null
                && response.getResponseType() == ResponseType.ERROR
                && response.getMessage() != null
                && response.getMessage().startsWith(QUORUM_NOT_MET);
    }
}
