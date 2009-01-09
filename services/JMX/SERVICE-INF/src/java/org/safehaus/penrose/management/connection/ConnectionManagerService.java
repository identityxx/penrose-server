package org.safehaus.penrose.management.connection;

import org.safehaus.penrose.management.BaseService;
import org.safehaus.penrose.management.PenroseJMXService;
import org.safehaus.penrose.connection.*;
import org.safehaus.penrose.partition.PartitionManager;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.PartitionConfig;
import org.safehaus.penrose.source.SourceConfigManager;
import org.safehaus.penrose.source.SourceConfig;
import org.safehaus.penrose.util.TextUtil;

import java.util.Collection;
import java.util.ArrayList;

/**
 * @author Endi Sukma Dewata
 */
public class ConnectionManagerService extends BaseService implements ConnectionManagerServiceMBean {

    private PartitionManager partitionManager;
    private String partitionName;

    public ConnectionManagerService(PenroseJMXService jmxService, PartitionManager partitionManager, String partitionName) throws Exception {

        this.jmxService = jmxService;
        this.partitionManager = partitionManager;
        this.partitionName = partitionName;
    }

    public String getObjectName() {
        return ConnectionManagerClient.getStringObjectName(partitionName);
    }

    public Object getObject() {
        return getConnectionManager();
    }

    public PartitionConfig getPartitionConfig() {
        return partitionManager.getPartitionConfig(partitionName);
    }

    public Partition getPartition() {
        return partitionManager.getPartition(partitionName);
    }

    public ConnectionConfigManager getConnectionConfigManager() {
        PartitionConfig partitionConfig = getPartitionConfig();
        if (partitionConfig == null) return null;
        return partitionConfig.getConnectionConfigManager();
    }

    public ConnectionManager getConnectionManager() {
        Partition partition = getPartition();
        if (partition == null) return null;
        return partition.getConnectionManager();
    }

    public ConnectionService getConnectionService(String connectionName) throws Exception {

        ConnectionService connectionService = new ConnectionService(jmxService, partitionManager, partitionName, connectionName);
        connectionService.init();

        return connectionService;
    }

    public void register() throws Exception {

        super.register();

        ConnectionConfigManager connectionConfigManager = getConnectionConfigManager();
        for (String connectionName : connectionConfigManager.getConnectionNames()) {
            ConnectionService connectionService = getConnectionService(connectionName);
            connectionService.register();
        }
    }

    public void unregister() throws Exception {
        ConnectionConfigManager connectionConfigManager = getConnectionConfigManager();
        for (String connectionName : connectionConfigManager.getConnectionNames()) {
            ConnectionService connectionService = getConnectionService(connectionName);
            connectionService.unregister();
        }

        super.unregister();
    }

    ////////////////////////////////////////////////////////////////////////////////
    // Connections
    ////////////////////////////////////////////////////////////////////////////////

    public Collection<String> getConnectionNames() {

        Collection<String> list = new ArrayList<String>();

        PartitionConfig partitionConfig = getPartitionConfig();
        if (partitionConfig == null) return list;

        list.addAll(partitionConfig.getConnectionConfigManager().getConnectionNames());
        return list;
    }

    public Collection<ConnectionConfig> getConnectionConfigs() {

        Collection<ConnectionConfig> list = new ArrayList<ConnectionConfig>();

        PartitionConfig partitionConfig = getPartitionConfig();
        if (partitionConfig == null) return list;

        list.addAll(partitionConfig.getConnectionConfigManager().getConnectionConfigs());
        return list;
    }

    public void validateConnection(ConnectionConfig connectionConfig) throws Exception {

        String connectionName = connectionConfig.getName();

        if (debug) {
            log.debug(TextUtil.repeat("-", 70));
            log.debug("Validating connection "+connectionName+".");
        }

        Partition partition = getPartition();
        if (partition == null) {
            throw new Exception("Partition is stopped.");
        }

        ConnectionManager connectionManager = partition.getConnectionManager();
        Connection connection = connectionManager.createConnection(connectionConfig);
        connection.validate();
        connection.destroy();
    }

    public void createConnection(ConnectionConfig connectionConfig) throws Exception {

        String connectionName = connectionConfig.getName();

        if (debug) {
            log.debug(TextUtil.repeat("-", 70));
            log.debug("Creating connection "+connectionName+".");
        }

        PartitionConfig partitionConfig = getPartitionConfig();
        ConnectionConfigManager connectionConfigManager = partitionConfig.getConnectionConfigManager();
        connectionConfigManager.addConnectionConfig(connectionConfig);

        Partition partition = getPartition();
        if (partition != null) {
            ConnectionManager connectionManager = partition.getConnectionManager();
            connectionManager.startConnection(connectionName);
        }

        ConnectionService connectionService = getConnectionService(connectionName);
        connectionService.register();
    }

    public void renameConnection(String connectionName, String newName) throws Exception {

        if (debug) {
            log.debug(TextUtil.repeat("-", 70));
            log.debug("Renaming connection "+connectionName+" to "+newName+".");
        }

        PartitionConfig partitionConfig = getPartitionConfig();
        SourceConfigManager sourceConfigManager = partitionConfig.getSourceConfigManager();

        Collection<SourceConfig> sourceConfigs = sourceConfigManager.getSourceConfigsByConnectionName(connectionName);
        if (sourceConfigs != null && !sourceConfigs.isEmpty()) {
            throw new Exception("Connection "+connectionName+" is in use.");
        }

        boolean running = false;

        Partition partition = getPartition();
        if (partition != null) {
            ConnectionManager connectionManager = partition.getConnectionManager();
            running = connectionManager.isRunning(connectionName);
            if (running) connectionManager.stopConnection(connectionName);
        }

        ConnectionConfigManager connectionConfigManager = partitionConfig.getConnectionConfigManager();
        connectionConfigManager.renameConnectionConfig(connectionName, newName);

        if (partition != null) {
            ConnectionManager connectionManager = partition.getConnectionManager();
            if (running) connectionManager.startConnection(newName);
        }
    }

    public void updateConnection(ConnectionConfig connectionConfig) throws Exception {

        String connectionName = connectionConfig.getName();

        if (debug) {
            log.debug(TextUtil.repeat("-", 70));
            log.debug("Updating connection "+connectionName+".");
        }

        boolean running = false;

        Partition partition = getPartition();
        if (partition != null) {
            ConnectionManager connectionManager = partition.getConnectionManager();
            running = connectionManager.isRunning(connectionName);
            if (running) connectionManager.stopConnection(connectionName);
        }

        PartitionConfig partitionConfig = getPartitionConfig();
        ConnectionConfigManager connectionConfigManager = partitionConfig.getConnectionConfigManager();
        connectionConfigManager.updateConnectionConfig(connectionConfig);

        if (partition != null) {
            ConnectionManager connectionManager = partition.getConnectionManager();
            if (running) connectionManager.startConnection(connectionConfig.getName());
        }
    }

    public void removeConnection(String connectionName) throws Exception {

        if (debug) {
            log.debug(TextUtil.repeat("-", 70));
            log.debug("Removing connection "+connectionName+".");
        }

        PartitionConfig partitionConfig = getPartitionConfig();
        SourceConfigManager sourceConfigManager = partitionConfig.getSourceConfigManager();

        Collection<SourceConfig> sourceConfigs = sourceConfigManager.getSourceConfigsByConnectionName(connectionName);
        if (sourceConfigs != null && !sourceConfigs.isEmpty()) {
            throw new Exception("Connection "+connectionName+" is in use.");
        }

        ConnectionService connectionService = getConnectionService(connectionName);
        connectionService.unregister();

        Partition partition = getPartition();
        if (partition != null) {
            ConnectionManager connectionManager = partition.getConnectionManager();
            boolean running = connectionManager.isRunning(connectionName);
            if (running) connectionManager.stopConnection(connectionName);
        }

        ConnectionConfigManager connectionConfigManager = partitionConfig.getConnectionConfigManager();
        connectionConfigManager.removeConnectionConfig(connectionName);
    }
}
