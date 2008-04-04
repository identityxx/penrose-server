package org.safehaus.penrose.connection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.LinkedHashMap;
import java.util.Collection;
import java.io.Serializable;

/**
 * @author Endi Sukma Dewata
 */
public class ConnectionConfigManager implements Serializable, Cloneable {

    static {
        log = LoggerFactory.getLogger(ConnectionConfigManager.class);
    }

    public static transient Logger log;
    public static boolean debug = log.isDebugEnabled();

    private Map<String,ConnectionConfig> connectionConfigs = new LinkedHashMap<String,ConnectionConfig>();

    public void addConnectionConfig(ConnectionConfig connectionConfig) {
        String name = connectionConfig.getName();
        //if (debug) log.debug("Adding connection "+name+".");
        connectionConfigs.put(name, connectionConfig);
    }

    public ConnectionConfig getConnectionConfig(String name) {
        return connectionConfigs.get(name);
    }

    public Collection<String> getConnectionNames() {
        return connectionConfigs.keySet();
    }
    
    public Collection<ConnectionConfig> getConnectionConfigs() {
        return connectionConfigs.values();
    }

    public void updateConnectionConfig(String name, ConnectionConfig connectionConfig) throws Exception {

        ConnectionConfig oldConnectionConfig = connectionConfigs.get(name);
        oldConnectionConfig.copy(connectionConfig);

        if (!name.equals(connectionConfig.getName())) {
            connectionConfigs.remove(name);
            connectionConfigs.put(connectionConfig.getName(), connectionConfig);
        }
    }

    public ConnectionConfig removeConnectionConfig(String connectionName) {
        return connectionConfigs.remove(connectionName);
    }

    public Object clone() throws CloneNotSupportedException {
        ConnectionConfigManager connections = (ConnectionConfigManager)super.clone();

        connections.connectionConfigs = new LinkedHashMap<String,ConnectionConfig>();
        for (ConnectionConfig connectionConfig : connectionConfigs.values()) {
            connections.connectionConfigs.put(connectionConfig.getName(), (ConnectionConfig)connectionConfig.clone());
        }

        return connections;
    }
}
