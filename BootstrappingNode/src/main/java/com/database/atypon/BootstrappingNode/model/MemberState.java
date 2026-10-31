package com.database.atypon.BootstrappingNode.model;

/**
 * Lifecycle state of a cluster member, as the data nodes' SWIM failure detector names it. The constants
 * are the wire form: they must stay spelled exactly as in the Node module's {@code MemberState}, because
 * the coordinator and the data nodes are separate modules that share no code, only JSON.
 */
public enum MemberState {
    ALIVE,
    SUSPECT,
    DEAD,
    LEFT
}
