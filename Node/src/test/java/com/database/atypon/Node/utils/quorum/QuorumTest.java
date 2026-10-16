package com.database.atypon.Node.utils.quorum;

import com.database.atypon.Node.utils.response.Response;
import com.database.atypon.Node.utils.response.ResponseType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class QuorumTest {

    @Test
    void requiredCountsPerLevel() {
        assertThat(Quorum.required(ConsistencyLevel.ONE, 3)).isEqualTo(1);
        assertThat(Quorum.required(ConsistencyLevel.QUORUM, 3)).isEqualTo(2);   // majority of 3
        assertThat(Quorum.required(ConsistencyLevel.QUORUM, 5)).isEqualTo(3);
        assertThat(Quorum.required(ConsistencyLevel.ALL, 3)).isEqualTo(3);
    }

    @Test
    void everyLevelIsOneForASingleReplica() {
        for (ConsistencyLevel l : ConsistencyLevel.values()) {
            assertThat(Quorum.required(l, 1)).isEqualTo(1);
        }
    }

    @Test
    void ackCountCountsLocalPlusSuccessfulReplicas() {
        List<Response> r = List.of(new Response(ResponseType.SUCCESS, "ok"),
                                   new Response(ResponseType.ERROR, "down"));
        assertThat(Quorum.ackCount(true, r)).isEqualTo(2);     // local + 1 success
        assertThat(Quorum.ackCount(false, r)).isEqualTo(1);
    }

    @Test
    void configDefaultsToQuorumWhenEnvUnset() {
        assertThat(ConsistencyConfig.writeConsistency()).isEqualTo(ConsistencyLevel.QUORUM);
        assertThat(ConsistencyConfig.readConsistency()).isEqualTo(ConsistencyLevel.QUORUM);
    }
}
