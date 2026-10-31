package com.database.atypon.BootstrappingNode.model;

/**
 * What the coordinator and a data node say to each other about one cluster member: the coordinator's copy
 * of the Node module's {@code MemberUpdate}. The two modules share no code, so this class exists only to
 * produce and read the same JSON: exactly the four fields {@code name}, {@code port}, {@code state} and
 * {@code incarnation}. Keep them in step with the Node module's class; a joining data node fails to bind
 * a roster that differs.
 *
 * <p>Plain bean (default constructor, getters and setters) so it binds the same way on both ends.
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

    /**
     * True if this update names a member (a name, a port, a state, a non-negative incarnation). Not a bean
     * property, so it is not serialised.
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
