package com.database.atypon.Node.utils.swim.dto;

import com.database.atypon.Node.model.Node;

import java.util.ArrayList;
import java.util.List;

/**
 * Body of {@code POST /swim/ping-req}: asks the receiver to ping {@link #getTarget() target} on the
 * sender's behalf and report whether it answered. This is the indirect probe, which tells a dead
 * target apart from a broken link between the sender and the target. It carries the sender's
 * membership gossip like any other probe.
 */
public class PingReqMessage {

    private String from;
    private Node target;
    private List<MemberUpdate> gossip = new ArrayList<>();

    public PingReqMessage() {
    }

    public PingReqMessage(String from, Node target) {
        this.from = from;
        this.target = target;
    }

    public PingReqMessage(String from, Node target, List<MemberUpdate> gossip) {
        this.from = from;
        this.target = target;
        setGossip(gossip);
    }

    /** Name of the node that asked for the probe; null when it does not know its own name yet. */
    public String getFrom() {
        return from;
    }

    public void setFrom(String from) {
        this.from = from;
    }

    /** The node the receiver is asked to ping. */
    public Node getTarget() {
        return target;
    }

    public void setTarget(Node target) {
        this.target = target;
    }

    /** Recent membership updates piggybacked on the request; never null. */
    public List<MemberUpdate> getGossip() {
        return gossip;
    }

    public void setGossip(List<MemberUpdate> gossip) {
        this.gossip = gossip == null ? new ArrayList<>() : gossip;
    }
}
