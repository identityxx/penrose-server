package org.safehaus.penrose.ldapbackend;

/**
 * @author Endi Sukma Dewata
 */
public interface DisconnectRequest {

    public void setConnectionId(Object id) throws Exception;
    public Object getConnectionId() throws Exception;
}
