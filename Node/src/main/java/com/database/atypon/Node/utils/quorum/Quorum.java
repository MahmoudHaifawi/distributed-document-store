package com.database.atypon.Node.utils.quorum;

import com.database.atypon.Node.utils.response.Response;
import com.database.atypon.Node.utils.response.ResponseType;

import java.util.List;

/** Pure quorum arithmetic shared by the write and read paths. */
public final class Quorum {

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
}
