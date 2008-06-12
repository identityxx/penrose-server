package org.safehaus.penrose.connection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.adapter.Adapter;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.PartitionConfig;
import org.safehaus.penrose.partition.PartitionContext;
import org.safehaus.penrose.naming.PenroseContext;
import org.safehaus.penrose.source.SourceConfig;
import org.safehaus.penrose.source.SourceConfigManager;

import java.util.Map;
import java.util.LinkedHashMap;
import java.util.Collection;

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

        for (ConnectionConfig connectionConfig : connectionConfigManager.getConnectionConfigs()) {
            if (!connectionConfig.isEnabled()) continue;

            createConnection(connectionConfig);
        }
    }

    public void destroy() throws Exception {
        for (Connection connection : connections.values()) {
            if (debug) log.debug("Stopping "+connection.getName()+" connection.");
            connection.destroy();
        }
    }

    public ConnectionConfig getConnectionConfig(String name) {
        return connectionConfigManager.getConnectionConfig(name);
    }

    public void startConnection(String name) throws Exception {
        ConnectionConfig connectionConfig = connectionConfigManager.getConnectionConfig(name);
        createConnection(connectionConfig);
    }

    public void stopConnection(String name) throws Exception {
        Connection connection = connections.remove(name);
        connection.destroy();
    }

    public boolean isRunning(String name) {
        return connections.containsKey(name);
    }
    
    public Connection createConnection(ConnectionConfig connectionConfig) throws Exception {

        PartitionContext partitionContext = partition.getPartitionContext();

        String adapterName = connectionConfig.getAdapterName();
        if (adapterName == null) throw new Exception("Missing adapter name.");

        Adapter adapter = partition.getAdapterManager().getAdapter(adapterName);
        if (adapter == null) {
            PenroseContext penroseContext = partitionContext.getPenroseContext();
            Partition defaultPartition = penroseContext.getPartitionManager().getPartition("DEFAULT");
            if (defaultPartition != null) {
                adapter = defaultPartition.getAdapterManager().getAdapter(adapterName);
            }
        }

        if (adapter == null) {
            throw new Exception("Unknown adapter "+adapterName+".");
        }

        ClassLoader cl = partitionContext.getClassLoader();

        ConnectionContext connectionContext = new ConnectionContext();
        connectionContext.setPartition(partition);
        connectionContext.setAdapter(adapter);
        connectionContext.setClassLoader(cl);

        String connectionClass = adapter.getConnectionClassName();
        Class clazz = cl.loadClass(connectionClass);
        Connection connection = (Connection)clazz.newInstance();

        connection.init(connectionConfig, connectionContext);

        addConnection(connection);

        return connection;
    }

    public void addConnection(Connection connection) {
        connections.put(connection.getName(), connection);
    }

    public void removeConnection(String name) throws Exception {
        Connection connection = connections.remove(name);
        if (connection != null) connection.destroy();
        connectionConfigManager.removeConnectionConfig(name);
    }
    
    public Connection getConnection(String name) {
        return connections.get(name);
    }

    public ConnectionConfigManager getConnectionConfigs() {
        return connectionConfigManager;
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

    public Collection<Connection> getConnections() {
        return connections.values();
    }
}
