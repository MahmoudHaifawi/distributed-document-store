package com.database.atypon.Node.utils.quorum;

/**
 * How many replicas of a schema must acknowledge an operation before it is reported as done.
 * The concrete count depends on the number of replicas; see {@link Quorum#required}.
 */
public enum ConsistencyLevel {
    /** A single replica is enough. */
    ONE,
    /** A majority of the replicas. */
    QUORUM,
    /** Every replica. */
    ALL
}
