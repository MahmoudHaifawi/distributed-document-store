package com.database.atypon.Node.utils.swim.dto;

/**
 * Body of {@code POST /swim/ping}: a direct liveness probe. The receiver's answer is the ack, so
 * nothing needs to be sent beyond who is asking.
 */
public class PingMessage {

    private String from;

    public PingMessage() {
    }

    public PingMessage(String from) {
        this.from = from;
    }

    /** Name of the node that sent the probe; null when the sender does not know its own name yet. */
    public String getFrom() {
        return from;
    }

    public void setFrom(String from) {
        this.from = from;
    }
}
