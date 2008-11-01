package org.safehaus.penrose.connection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.adapter.Adapter;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.PartitionConfig;
import org.safehaus.penrose.partition.PartitionContext;
import org.safehaus.penrose.source.SourceConfig;
import org.safehaus.penrose.source.SourceConfigManager;

import java.util.Map;
import java.util.LinkedHashMap;
import java.util.Collection;
import java.util.ArrayList;

/**
 * @author Endi Sukma Dewata
 */
public class ConnectionManager {

    public Logger log = LoggerFactory.getLogger(getClass());
    public boolean debug = log.isDebugEnabled();

    protected Partition partition;
    protected ConnectionConfigManager connectionConfigManager;

    protected Map<String,Connection> connections = new LinkedHashMap<String,Connection>();

    public ConnectionManager(Partition partition) {
        this.partition = partition;

        PartitionConfig partitionConfig = partition.getPartitionConfig();
        connectionConfigManager = partitionConfig.getConnectionConfigManager();
    }

    public void init() throws Exception {

        Collection<String> names = new ArrayList<String>();
        names.addAll(getConnectionNames());

        for (String name : names) {

            ConnectionConfig connectionConfig = getConnectionConfig(name);
            if (!connectionConfig.isEnabled()) continue;

            startConnection(name);
        }
    }

    public void destroy() throws Exception {

        Collection<String> names = new ArrayList<String>();
        names.addAll(connections.keySet());

        for (String name : names) {
            stopConnection(name);
        }
    }

    public Collection<String> getConnectionNames() {
        return connectionConfigManager.getConnectionNames();
    }

    public ConnectionConfig getConnectionConfig(String name) {
        return connectionConfigManager.getConnectionConfig(name);
    }

    public void startConnection(String name) throws Exception {
        if (debug) log.debug("Starting connection "+name+".");
        ConnectionConfig connectionConfig = getConnectionConfig(name);
        createConnection(connectionConfig);
    }

    public void stopConnection(String name) throws Exception {
        if (debug) log.debug("Stopping connection "+name+".");
        Connection connection = removeConnection(name);
        connection.destroy();
    }

    public boolean isRunning(String name) {
        return connections.containsKey(name);
    }
    
    public Connection createConnection(ConnectionConfig connectionConfig) throws Exception {

        if (debug) log.debug("Creating connection "+connectionConfig.getName()+".");

        String adapterName = connectionConfig.getAdapterName();
        if (adapterName == null) throw new Exception("Missing adapter name.");

        Adapter adapter = partition.getAdapterManager().getAdapter(adapterName);
        if (adapter == null) throw new Exception("Unknown adapter "+adapterName+".");

        PartitionContext partitionContext = partition.getPartitionContext();
        ClassLoader cl = partitionContext.getClassLoader();

        ConnectionContext connectionContext = new ConnectionContext();
        connectionContext.setPartition(partition);
        connectionContext.setAdapter(adapter);
        connectionContext.setClassLoader(cl);

        Connection connection = adapter.createConnection(connectionConfig, connectionContext);

        addConnection(connection);

        return connection;
    }

    public void addConnection(Connection connection) {
        connections.put(connection.getName(), connection);
    }

    public Connection removeConnection(String name) throws Exception {
        return connections.remove(name);
    }

    public Collection<Connection> getConnections() {
        return connections.values();
    }

    public Connection getConnection(String name) {
        Connection connection = connections.get(name);
        if (connection != null) return connection;

        if (partition.getName().equals("DEFAULT")) return null;
        Partition defaultPartition = partition.getPartitionContext().getPartition("DEFAULT");

        ConnectionManager connectionManager = defaultPartition.getConnectionManager();
        return connectionManager.getConnection(name);
    }

    public void updateConnectionConfig(String name, ConnectionConfig connectionConfig) throws Exception {

        PartitionConfig partitionConfig = partition.getPartitionConfig();
        connectionConfigManager.updateConnectionConfig(name, connectionConfig);

        // fix references
        SourceConfigManager sourceConfigManager = partitionConfig.getSourceConfigManager();
        for (SourceConfig sourceConfig : sourceConfigManager.getSourceConfigs()) {
            if (!sourceConfig.getConnectionName().equals(name)) continue;
            sourceConfig.setConnectionName(connectionConfig.getName());
        }
    }

    public ConnectionConfig removeConnectionConfig(String name) {
        return connectionConfigManager.removeConnectionConfig(name);
    }

    public ConnectionConfigManager getConnectionManager() {
        return connectionConfigManager;
    }
}
