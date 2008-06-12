package org.safehaus.penrose.ldap;

/**
 * @author Endi Sukma Dewata
 */
public class ConnectRequest {

    public Object connectionId;
    public String clientAddress;
    public String serverAddress;
    public String protocol;

    public void setConnectionId(Object connectionId) {
        this.connectionId = connectionId;
    }

    public Object getConnectionId() throws Exception {
        return connectionId;
    }

    public void setClientAddress(String clientAddress) {
        this.clientAddress = clientAddress;
    }

    public String getClientAddress() {
        return clientAddress;
    }

    public void setServerAddress(String serverAddress) {
        this.serverAddress = serverAddress;
    }

    public String getServerAddress() {
        return serverAddress;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    public String getProtocol() {
        return protocol;
    }
}