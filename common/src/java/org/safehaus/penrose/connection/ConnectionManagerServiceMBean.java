package org.safehaus.penrose.connection;

import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public interface ConnectionManagerServiceMBean {

    public Collection<String> getConnectionNames() throws Exception;
    public void createConnection(ConnectionConfig connectionConfig) throws Exception;
    public void updateConnection(String name, ConnectionConfig connectionConfig) throws Exception;
    public void removeConnection(String name) throws Exception;
}
