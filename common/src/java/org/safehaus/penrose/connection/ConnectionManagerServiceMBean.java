package org.safehaus.penrose.connection;

import org.safehaus.penrose.ldap.DN;

import java.util.Collection;
import java.util.Map;

/**
 * @author Endi Sukma Dewata
 */
public interface ConnectionManagerServiceMBean {

    public Collection<String> getConnectionNames() throws Exception;

    public void validateConnection(ConnectionConfig connectionConfig) throws Exception;
    public Collection<DN> getNamingContexts(ConnectionConfig connectionConfig) throws Exception;

    public void createConnection(ConnectionConfig connectionConfig) throws Exception;
    public void renameConnection(String connectionName, String newConnectionName) throws Exception;
    public void updateConnection(String connectionName, ConnectionConfig connectionConfig) throws Exception;
    public void removeConnection(String connectionName) throws Exception;
}
