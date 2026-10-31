package com.database.atypon.Node.utils.swim;

import com.database.atypon.Node.model.Node;

/**
 * Immutable snapshot of one cluster member: where it lives, what state we believe it is in,
 * the incarnation number that orders competing claims about it, and when that belief last
 * changed (epoch millis).
 */
public final class Member {

    private final Node node;
    private final MemberState state;
    private final int incarnation;
    private final long lastChange;

    public Member(Node node, MemberState state, int incarnation, long lastChange) {
        this.node = node;
        this.state = state;
        this.incarnation = incarnation;
        this.lastChange = lastChange;
    }

    public Node getNode() {
        return node;
    }

    public MemberState getState() {
        return state;
    }

    public int getIncarnation() {
        return incarnation;
    }

    public long getLastChange() {
        return lastChange;
    }

    public Member withState(MemberState newState) {
        return new Member(node, newState, incarnation, lastChange);
    }

    public Member withIncarnation(int newIncarnation) {
        return new Member(node, state, newIncarnation, lastChange);
    }
}
