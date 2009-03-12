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

    protected Map<String,ConnectionConfig> connectionConfigs = new LinkedHashMap<String,ConnectionConfig>();

    public void addConnectionConfig(ConnectionConfig connectionConfig) throws Exception {

        Logger log = LoggerFactory.getLogger(getClass());
        boolean debug = log.isDebugEnabled();

        String connectionName = connectionConfig.getName();

        if (debug) log.debug("Adding connection \""+connectionName+"\".");

        validate(connectionConfig);

        connectionConfigs.put(connectionConfig.getName(), connectionConfig);
    }

    public void validate(ConnectionConfig connectionConfig) throws Exception {

        String connectionName = connectionConfig.getName();

        if (connectionName == null || "".equals(connectionName)) {
            throw new Exception("Missing connection name.");
        }

        char startingChar = connectionName.charAt(0);
        if (!Character.isLetter(startingChar)) {
            throw new Exception("Invalid connection name: "+connectionName);
        }

        for (int i = 1; i<connectionName.length(); i++) {
            char c = connectionName.charAt(i);
            if (Character.isLetterOrDigit(c) || c == '_') continue;
            throw new Exception("Invalid connection name: "+connectionName);
        }

        if (connectionConfigs.containsKey(connectionName)) {
            throw new Exception("Connection "+connectionName+" already exists.");
        }
    }

    public ConnectionConfig getConnectionConfig(String connectionName) {
        return connectionConfigs.get(connectionName);
    }

    public Collection<String> getConnectionNames() {
        return connectionConfigs.keySet();
    }
    
    public Collection<ConnectionConfig> getConnectionConfigs() {
        return connectionConfigs.values();
    }

    public void renameConnectionConfig(String name, String newName) throws Exception {
        ConnectionConfig connectionConfig = connectionConfigs.remove(name);
        connectionConfig.setName(newName);
        connectionConfigs.put(newName, connectionConfig);
    }

    public void updateConnectionConfig(ConnectionConfig connectionConfig) throws Exception {
        String connectionName = connectionConfig.getName();

        ConnectionConfig oldConnectionConfig = connectionConfigs.get(connectionName);
        if (oldConnectionConfig == null) {
            throw new Exception("Connection "+connectionName+" not found.");
        }

        oldConnectionConfig.copy(connectionConfig);
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
