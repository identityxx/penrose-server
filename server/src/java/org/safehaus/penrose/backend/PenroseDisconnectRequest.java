package org.safehaus.penrose.backend;

import com.identyx.javabackend.DisconnectRequest;

/**
 * @author Endi Sukma Dewata
 */
public class PenroseDisconnectRequest implements DisconnectRequest {

    public org.safehaus.penrose.ldap.DisconnectRequest request = new org.safehaus.penrose.ldap.DisconnectRequest();

    public void setConnectionId(Object id) throws Exception {
        request.setConnectionId(id);
    }

    public Object getConnectionId() throws Exception {
        return request.getConnectionId();
    }

    public org.safehaus.penrose.ldap.DisconnectRequest getRequest() {
        return request;
    }

    public void setRequest(org.safehaus.penrose.ldap.DisconnectRequest request) {
        this.request = request;
    }
}
