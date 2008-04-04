package org.safehaus.penrose.management.connection;

import org.safehaus.penrose.connection.Connection;
import org.safehaus.penrose.connection.ConnectionConfig;
import org.safehaus.penrose.connection.ConnectionManager;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.PartitionManager;
import org.safehaus.penrose.partition.PartitionConfig;
import org.safehaus.penrose.management.BaseService;
import org.safehaus.penrose.management.PenroseJMXService;

/**
 * @author Endi Sukma Dewata
 */
public class ConnectionService extends BaseService implements ConnectionServiceMBean {

    private PartitionManager partitionManager;
    private String partitionName;
    private String connectionName;

    public ConnectionService(
            PenroseJMXService jmxService,
            PartitionManager partitionManager,
            String partitionName,
            String connectionName
    ) throws Exception {
        super(ConnectionServiceMBean.class);

        this.jmxService = jmxService;
        this.partitionManager = partitionManager;
        this.partitionName = partitionName;
        this.connectionName = connectionName;
    }

    public String getObjectName() {
        return ConnectionClient.getStringObjectName(partitionName, connectionName);
    }

    public Object getObject() {
        return getConnection();
    }

    public ConnectionConfig getConnectionConfig() throws Exception {
        return getPartitionConfig().getConnectionConfigManager().getConnectionConfig(connectionName);
    }

    public Connection getConnection() {
        return getPartition().getConnection(connectionName);
    }

    public PartitionConfig getPartitionConfig() {
        return partitionManager.getPartitionConfig(partitionName);
    }

    public Partition getPartition() {
        return partitionManager.getPartition(partitionName);
    }

    public String getAdapterName() throws Exception {
        ConnectionConfig connectionConfig = getConnectionConfig();
        return connectionConfig.getAdapterName();
    }
    
    public void start() throws Exception {

        log.debug("Starting connection "+partitionName+"/"+connectionName+"...");

        Partition partition = getPartition();
        ConnectionManager connectionManager = partition.getConnectionManager();
        connectionManager.startConnection(connectionName);

        log.debug("Connection started.");
    }

    public void stop() throws Exception {

        log.debug("Stopping connection "+partitionName+"/"+connectionName+"...");

        Partition partition = getPartition();
        ConnectionManager connectionManager = partition.getConnectionManager();
        connectionManager.stopConnection(connectionName);

        log.debug("Connection stopped.");
    }

    public void restart() throws Exception {

        log.debug("Restarting connection "+partitionName+"/"+connectionName+"...");

        Partition partition = getPartition();
        ConnectionManager connectionManager = partition.getConnectionManager();
        connectionManager.stopConnection(connectionName);
        connectionManager.startConnection(connectionName);

        log.debug("Connection restarted.");
    }
}
