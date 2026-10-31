package com.database.atypon.Node.utils.swim;

/**
 * Lifecycle state of a cluster member as seen by the SWIM failure detector.
 *
 * <p>{@link #precedence()} breaks ties between updates carrying the same incarnation: the
 * "worse" news wins, so {@code ALIVE < SUSPECT < DEAD = LEFT}.
 */
public enum MemberState {
    ALIVE(0),
    SUSPECT(1),
    DEAD(2),
    LEFT(2);

    private final int precedence;

    MemberState(int precedence) {
        this.precedence = precedence;
    }

    /** True for members still considered part of the ring: {@code ALIVE} or {@code SUSPECT}. */
    public boolean isLive() {
        return this == ALIVE || this == SUSPECT;
    }

    /** Ordering used to resolve updates at an equal incarnation; higher wins. */
    public int precedence() {
        return precedence;
    }
}
