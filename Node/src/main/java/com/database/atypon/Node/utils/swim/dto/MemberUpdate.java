package com.database.atypon.Node.utils.swim.dto;

import com.database.atypon.Node.model.Node;
import com.database.atypon.Node.utils.swim.Member;
import com.database.atypon.Node.utils.swim.MemberState;

/**
 * One piece of gossip on the wire: what a node believes about one member. It is a
 * {@link Member} without {@code lastChange}, because that field is a stamp from the local clock of
 * the node that holds the record and means nothing anywhere else. The receiver stamps an adopted
 * update with its own clock when it merges it.
 *
 * <p>Plain bean (default constructor, getters and setters) so it binds the same way on both ends of a
 * probe. Use {@link #wellFormed()} before {@link #toMember()}: gossip comes from other nodes, and one
 * bad entry should cost only itself.
 */
public class MemberUpdate {

    private String name;
    private String port;
    private MemberState state;
    private int incarnation;

    public MemberUpdate() {
    }

    public MemberUpdate(String name, String port, MemberState state, int incarnation) {
        this.name = name;
        this.port = port;
        this.state = state;
        this.incarnation = incarnation;
    }

    /** The wire form of {@code member}. */
    public static MemberUpdate from(Member member) {
        Node node = member.getNode();
        return new MemberUpdate(node.getName(), node.getPort(), member.getState(), member.getIncarnation());
    }

    /** The member this update describes, with {@code lastChange} left at zero for the receiving table to stamp. */
    public Member toMember() {
        return new Member(new Node(name, port), state, incarnation, 0L);
    }

    /**
     * True if this update names a member (a name, a port, a state, a non-negative incarnation) and so
     * can be turned into one. Not a bean property, so it is not serialised.
     */
    public boolean wellFormed() {
        return name != null && !name.isBlank()
                && port != null && !port.isBlank()
                && state != null
                && incarnation >= 0;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public MemberState getState() {
        return state;
    }

    public void setState(MemberState state) {
        this.state = state;
    }

    public int getIncarnation() {
        return incarnation;
    }

    public void setIncarnation(int incarnation) {
        this.incarnation = incarnation;
    }
}
