package org.safehaus.penrose.backend;

import org.safehaus.penrose.ldapbackend.ConnectRequest;

/**
 * @author Endi Sukma Dewata
 */
public class PenroseConnectRequest implements ConnectRequest {

    public org.safehaus.penrose.ldap.ConnectRequest request = new org.safehaus.penrose.ldap.ConnectRequest();

    public void setConnectionId(Object id) throws Exception {
        request.setConnectionId(id);
    }

    public Object getConnectionId() throws Exception {
        return request.getConnectionId();
    }

    public void setClientAddress(String clientAddress) throws Exception {
        request.setClientAddress(clientAddress);
    }

    public String getClientAddress() throws Exception {
        return request.getClientAddress();
    }

    public void setServerAddress(String serverAddress) throws Exception {
        request.setServerAddress(serverAddress);
    }

    public String getServerAddress() throws Exception {
        return request.getServerAddress();
    }

    public void setProtocol(String protocol) throws Exception {
        request.setProtocol(protocol);
    }

    public String getProtocol() throws Exception {
        return request.getProtocol();
    }

    public org.safehaus.penrose.ldap.ConnectRequest getRequest() {
        return request;
    }

    public void setRequest(org.safehaus.penrose.ldap.ConnectRequest request) {
        this.request = request;
    }
}
