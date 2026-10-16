package com.database.atypon.Node.utils.quorum;

/**
 * What one replica answered when asked for a document: whether it holds it, at which {@code _version}, and
 * the raw document JSON. A replica that does not hold the document has {@code present == false}, version 0
 * and no content.
 */
public final class ReplicaDoc {

    public final String node;
    public final boolean present;
    public final int version;
    public final String content;

    public ReplicaDoc(String node, boolean present, int version, String content) {
        this.node = node;
        this.present = present;
        this.version = version;
        this.content = content;
    }

    @Override
    public String toString() {
        return "ReplicaDoc{node='" + node + "', present=" + present + ", version=" + version + '}';
    }
}
