package org.safehaus.penrose.management.connection;

import org.safehaus.penrose.connection.*;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.PartitionManager;
import org.safehaus.penrose.partition.PartitionConfig;
import org.safehaus.penrose.management.BaseService;
import org.safehaus.penrose.management.PenroseJMXService;
import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.session.SessionManager;

/**
 * @author Endi Sukma Dewata
 */
public class ConnectionService extends BaseService implements ConnectionServiceMBean {

    protected PartitionManager partitionManager;
    protected String partitionName;
    protected String connectionName;

    public ConnectionService(
            PenroseJMXService jmxService,
            PartitionManager partitionManager,
            String partitionName,
            String connectionName
    ) throws Exception {

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

    public void setConnectionConfig(ConnectionConfig connectionConfig) throws Exception {
        getPartitionConfig().getConnectionConfigManager().updateConnectionConfig(connectionConfig);
    }

    public Connection getConnection() {
        Partition partition = getPartition();
        if (partition == null) return null;

        ConnectionManager connectionManager = partition.getConnectionManager();
        return connectionManager.getConnection(connectionName);
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

    public String getStatus() throws Exception {
        Connection connection = getConnection();
        return connection == null ? ConnectionServiceMBean.STOPPED : ConnectionServiceMBean.STARTED;
    }

    public Session createAdminSession() throws Exception {
        Partition partition = getPartition();
        SessionManager sessionManager = partition.getPartitionContext().getSessionManager();
        return sessionManager.createAdminSession();
    }
}
