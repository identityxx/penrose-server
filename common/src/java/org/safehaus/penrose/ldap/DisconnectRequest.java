package org.safehaus.penrose.ldap;

/**
 * @author Endi Sukma Dewata
 */
public class DisconnectRequest {

    public Object connectionId;

    public void setConnectionId(Object connectionId) throws Exception {
        this.connectionId = connectionId;
    }

    public Object getConnectionId() throws Exception {
        return connectionId;
    }
}