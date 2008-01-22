package org.safehaus.penrose.connection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.adapter.Adapter;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.PartitionConfig;
import org.safehaus.penrose.partition.PartitionContext;
import org.safehaus.penrose.naming.PenroseContext;

import java.util.Map;
import java.util.LinkedHashMap;
import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public class Connections {

    public Logger log = LoggerFactory.getLogger(getClass());
    public boolean debug = log.isDebugEnabled();

    Partition partition;

    protected Map<String,Connection> connections = new LinkedHashMap<String,Connection>();

    public void init(Partition partition) throws Exception {
        this.partition = partition;

        PartitionConfig partitionConfig = partition.getPartitionConfig();
        for (ConnectionConfig connectionConfig : partitionConfig.getConnectionConfigs().getConnectionConfigs()) {
            if (!connectionConfig.isEnabled()) continue;

            Connection connection = createConnection(connectionConfig);
            addConnection(connection);
        }
    }

    public void destroy() throws Exception {
        for (Connection connection : connections.values()) {
            log.debug("Stopping "+connection.getName()+" connection.");
            connection.destroy();
        }
    }

    public Connection createConnection(ConnectionConfig connectionConfig) throws Exception {

        PartitionContext partitionContext = partition.getPartitionContext();

        String adapterName = connectionConfig.getAdapterName();
        if (adapterName == null) throw new Exception("Missing adapter name.");

        Adapter adapter = partition.getAdapters().getAdapter(adapterName);
        if (adapter == null) {
            PenroseContext penroseContext = partitionContext.getPenroseContext();
            Partition defaultPartition = penroseContext.getPartitions().getPartition("DEFAULT");
            if (defaultPartition != null) {
                adapter = defaultPartition.getAdapters().getAdapter(adapterName);
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

        return connection;
    }

    public void addConnection(Connection connection) {
        connections.put(connection.getName(), connection);
    }

    public Collection<Connection> getConnections() {
        return connections.values();
    }

    public Connection getConnection(String name) {
        return connections.get(name);
    }

}
