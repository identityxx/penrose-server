package org.safehaus.penrose.connection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.adapter.Adapter;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.PartitionConfig;
import org.safehaus.penrose.partition.PartitionContext;
import org.safehaus.penrose.Penrose;

import java.util.Map;
import java.util.LinkedHashMap;
import java.util.Collection;
import java.util.ArrayList;

/**
 * @author Endi Sukma Dewata
 */
public class ConnectionManager {

    public Logger log = LoggerFactory.getLogger(getClass());

    protected Partition partition;
    protected ConnectionConfigManager connectionConfigManager;

    protected Map<String,Connection> connections = new LinkedHashMap<String,Connection>();

    public ConnectionManager(Partition partition) {
        this.partition = partition;

        PartitionConfig partitionConfig = partition.getPartitionConfig();
        connectionConfigManager = partitionConfig.getConnectionConfigManager();
    }

    public void init() throws Exception {

        Collection<String> connectionNames = new ArrayList<String>();
        connectionNames.addAll(getConnectionNames());

        for (String connectionName : connectionNames) {

            ConnectionConfig connectionConfig = getConnectionConfig(connectionName);
            if (!connectionConfig.isEnabled()) continue;

            try {
                startConnection(connectionName);
            } catch (Exception e) {
                Penrose.errorLog.error("Failed creating connection "+connectionName+" in partition "+partition.getName()+".", e);
            }
        }
    }

    public void destroy() throws Exception {

        Collection<String> connectionNames = new ArrayList<String>();
        connectionNames.addAll(connections.keySet());

        for (String connectionName : connectionNames) {
            try {
                stopConnection(connectionName);
            } catch (Exception e) {
                Penrose.errorLog.error("Failed removing connection "+connectionName+" in partition "+partition.getName()+".", e);
            }
        }
    }

    public Collection<String> getConnectionNames() {
        return connectionConfigManager.getConnectionNames();
    }

    public ConnectionConfig getConnectionConfig(String name) {
        return connectionConfigManager.getConnectionConfig(name);
    }

    public void startConnection(String connectionName) throws Exception {

        boolean debug = log.isDebugEnabled();
        if (debug) log.debug("Starting connection "+connectionName+".");

        ConnectionConfig connectionConfig = getConnectionConfig(connectionName);
        Connection connection = createConnection(connectionConfig);

        connections.put(connection.getName(), connection);
    }

    public void stopConnection(String connectionName) throws Exception {

        boolean debug = log.isDebugEnabled();
        if (debug) log.debug("Stopping connection "+connectionName+".");

        Connection connection = connections.get(connectionName);
        connection.destroy();

        connections.remove(connectionName);
    }

    public boolean isRunning(String name) {
        return connections.containsKey(name);
    }
    
    public Connection createConnection(ConnectionConfig connectionConfig) throws Exception {

        boolean debug = log.isDebugEnabled();
        String connectionName = connectionConfig.getName();
        if (debug) log.debug("Creating connection "+connectionName+".");

        PartitionContext partitionContext = partition.getPartitionContext();
        ClassLoader cl = partitionContext.getClassLoader();

        ConnectionContext connectionContext = new ConnectionContext();
        connectionContext.setPartition(partition);
        connectionContext.setClassLoader(cl);

        String className = connectionConfig.getConnectionClass();
        if (className == null) {
            String adapterName = connectionConfig.getAdapterName();
            if (adapterName == null) throw new Exception("Missing adapter name.");

            Adapter adapter = partition.getAdapterManager().getAdapter(adapterName);
            if (adapter == null) throw new Exception("Unknown adapter "+adapterName+".");

            connectionContext.setAdapter(adapter);
            className = adapter.getConnectionClassName();
        }

        Class clazz = cl.loadClass(className);

        if (debug) log.debug("Creating "+className+".");
        Connection connection = (Connection)clazz.newInstance();

        connection.init(connectionConfig, connectionContext);

        return connection;
    }

    public void updateConnection(ConnectionConfig connectionConfig) throws Exception {

        connectionConfigManager.updateConnectionConfig(connectionConfig);

        String connectionName = connectionConfig.getName();

        if (isRunning(connectionName)) {
            stopConnection(connectionName);
            startConnection(connectionName);
        }
    }

    public Collection<Connection> getConnections() {
        return connections.values();
    }

    public Connection getConnection(String name) {
        Connection connection = connections.get(name);
        if (connection != null) return connection;

        if (partition.getName().equals(PartitionConfig.ROOT)) return null;
        Partition rootPartition = partition.getPartitionContext().getPartition(PartitionConfig.ROOT);

        ConnectionManager connectionManager = rootPartition.getConnectionManager();
        return connectionManager.getConnection(name);
    }

    public ConnectionConfigManager getConnectionManager() {
        return connectionConfigManager;
    }
}
