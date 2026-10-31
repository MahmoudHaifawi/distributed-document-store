package com.database.atypon.Node.utils.swim.dto;

import java.util.ArrayList;
import java.util.List;

/**
 * Body of {@code POST /swim/ping}: a direct liveness probe. The receiver's answer is the ack, so
 * nothing needs to be sent beyond who is asking, plus the membership gossip riding along.
 */
public class PingMessage {

    private String from;
    private List<MemberUpdate> gossip = new ArrayList<>();

    public PingMessage() {
    }

    public PingMessage(String from) {
        this.from = from;
    }

    public PingMessage(String from, List<MemberUpdate> gossip) {
        this.from = from;
        setGossip(gossip);
    }

    /** Name of the node that sent the probe; null when the sender does not know its own name yet. */
    public String getFrom() {
        return from;
    }

    public void setFrom(String from) {
        this.from = from;
    }

    /** Recent membership updates piggybacked on the probe; never null. */
    public List<MemberUpdate> getGossip() {
        return gossip;
    }

    public void setGossip(List<MemberUpdate> gossip) {
        this.gossip = gossip == null ? new ArrayList<>() : gossip;
    }
}
