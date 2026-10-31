package com.database.atypon.Node.utils.swim.dto;

/**
 * The answer to a SWIM probe. For {@code POST /swim/ping} the response itself is the ack, so it is
 * always {@code acked = true}; for {@code POST /swim/ping-req} it reports whether the relayed ping
 * was acked by the target.
 */
public class AckMessage {

    private boolean acked;

    public AckMessage() {
    }

    public AckMessage(boolean acked) {
        this.acked = acked;
    }

    public boolean isAcked() {
        return acked;
    }

    public void setAcked(boolean acked) {
        this.acked = acked;
    }
}
