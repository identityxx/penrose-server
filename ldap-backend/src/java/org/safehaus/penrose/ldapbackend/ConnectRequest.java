package org.safehaus.penrose.ldapbackend;

/**
 * @author Endi Sukma Dewata
 */
public interface ConnectRequest {

    public void setConnectionId(Object id) throws Exception;
    public Object getConnectionId() throws Exception;

    public void setClientAddress(String clientAddress) throws Exception;
    public String getClientAddress() throws Exception;

    public void setServerAddress(String serverAddress) throws Exception;
    public String getServerAddress() throws Exception;

    public void setProtocol(String protocol) throws Exception;
    public String getProtocol() throws Exception;
}
