package org.safehaus.penrose.connection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.adapter.Adapter;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.PartitionConfig;
import org.safehaus.penrose.partition.PartitionContext;

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

            try {
                startConnection(name);
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
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
        Connection connection = createConnection(connectionConfig);
        addConnection(connection);
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

    public void addConnection(Connection connection) {
        connections.put(connection.getName(), connection);
    }

    public Connection removeConnection(String name) throws Exception {
        if (debug) log.debug("Removing connection "+name+".");
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

    public void updateConnectionConfig(ConnectionConfig connectionConfig) throws Exception {
        connectionConfigManager.updateConnectionConfig(connectionConfig);
    }

    public ConnectionConfig removeConnectionConfig(String name) {
        return connectionConfigManager.removeConnectionConfig(name);
    }

    public ConnectionConfigManager getConnectionManager() {
        return connectionConfigManager;
    }
}
