package com.database.atypon.Node.controllers.write;

import com.database.atypon.Node.utils.quorum.Quorum;
import com.database.atypon.Node.utils.response.Response;
import com.database.atypon.Node.utils.response.ResponseType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** The pure verdict on whether a write may be acknowledged to the client: did W replicas persist it. */
class WriteQuorumTest {

    @Test
    void successWhenAcksMeetW() {
        Response local = new Response(ResponseType.SUCCESS, "ok", 2);
        List<Response> replicas = List.of(
                new Response(ResponseType.SUCCESS, "ok"), new Response(ResponseType.ERROR, "down"));
        // acks = 2 (local + one replica), w = 2
        assertThat(Quorum.writeClientResult(true, local, replicas, 2).getResponseType())
                .isEqualTo(ResponseType.SUCCESS);
    }

    @Test
    void errorWhenAcksBelowW() {
        Response local = new Response(ResponseType.SUCCESS, "ok", 2);
        List<Response> replicas = List.of(
                new Response(ResponseType.ERROR, "down"), new Response(ResponseType.ERROR, "down"));
        // acks = 1 (local only), w = 2
        Response r = Quorum.writeClientResult(true, local, replicas, 2);
        assertThat(r.getResponseType()).isEqualTo(ResponseType.ERROR);
        assertThat(r.getMessage()).contains("quorum not met");
    }

    @Test
    void localFailureIsControlling() {
        Response local = new Response(ResponseType.ERROR, "Version conflict");
        assertThat(Quorum.writeClientResult(false, local, List.of(), 2)).isSameAs(local);
    }

    @Test
    void successResultIsTheLocalResultItselfSoItsContentSurvives() {
        Response local = new Response(ResponseType.SUCCESS, "ok", 2);
        assertThat(Quorum.writeClientResult(true, local, List.of(new Response(ResponseType.SUCCESS, "ok")), 2))
                .isSameAs(local);
    }

    @Test
    void aLoneNodeNeedsOnlyItsOwnAck() {
        Response local = new Response(ResponseType.SUCCESS, "ok");
        assertThat(Quorum.writeClientResult(true, local, List.of(), 1)).isSameAs(local);
    }

    @Test
    void quorumNotMetMessageReportsAcksAgainstW() {
        Response local = new Response(ResponseType.SUCCESS, "ok");
        Response r = Quorum.writeClientResult(true, local, List.of(new Response(ResponseType.ERROR, "down")), 3);
        assertThat(r.getMessage()).isEqualTo("write quorum not met (1/3)");
    }
}
