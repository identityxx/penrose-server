package org.safehaus.penrose.connection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.adapter.AdapterConfig;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.PartitionConfig;
import org.safehaus.penrose.partition.PartitionContext;

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
            connection.destroy();
        }
    }

    public Connection createConnection(ConnectionConfig connectionConfig) throws Exception {

        PartitionConfig partitionConfig = partition.getPartitionConfig();
        PartitionContext partitionContext = partition.getPartitionContext();

        String adapterName = connectionConfig.getAdapterName();
        if (adapterName == null) throw new Exception("Missing adapter name.");

        AdapterConfig adapterConfig = partitionConfig.getAdapterConfig(adapterName);

        if (adapterConfig == null) {
            adapterConfig = partitionContext.getPenroseConfig().getAdapterConfig(adapterName);
        }

        if (adapterConfig == null) throw new Exception("Undefined adapter "+adapterName+".");

        ConnectionContext connectionContext = new ConnectionContext();
        connectionContext.setPartition(partition);

        Connection connection = new Connection();
        connection.init(connectionConfig, connectionContext, adapterConfig);

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
