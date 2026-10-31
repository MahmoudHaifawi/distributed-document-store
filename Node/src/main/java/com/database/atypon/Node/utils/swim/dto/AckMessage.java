package com.database.atypon.Node.utils.swim.dto;

import java.util.ArrayList;
import java.util.List;

/**
 * The answer to a SWIM probe. For {@code POST /swim/ping} the response itself is the ack, so it is
 * always {@code acked = true}; for {@code POST /swim/ping-req} it reports whether the relayed ping
 * was acked by the target. Either way it carries the responder's membership gossip, which always
 * includes the responder's own {@code ALIVE}, so a prober that had suspected it can clear the suspicion.
 */
public class AckMessage {

    private boolean acked;
    private List<MemberUpdate> gossip = new ArrayList<>();

    public AckMessage() {
    }

    public AckMessage(boolean acked) {
        this.acked = acked;
    }

    public AckMessage(boolean acked, List<MemberUpdate> gossip) {
        this.acked = acked;
        setGossip(gossip);
    }

    public boolean isAcked() {
        return acked;
    }

    public void setAcked(boolean acked) {
        this.acked = acked;
    }

    /** Recent membership updates piggybacked on the answer; never null. */
    public List<MemberUpdate> getGossip() {
        return gossip;
    }

    public void setGossip(List<MemberUpdate> gossip) {
        this.gossip = gossip == null ? new ArrayList<>() : gossip;
    }
}
