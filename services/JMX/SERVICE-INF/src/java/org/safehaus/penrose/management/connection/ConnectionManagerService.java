package org.safehaus.penrose.management.connection;

import org.safehaus.penrose.management.BaseService;
import org.safehaus.penrose.management.PenroseJMXService;
import org.safehaus.penrose.management.ldap.LDAPConnectionService;
import org.safehaus.penrose.connection.*;
import org.safehaus.penrose.partition.PartitionManager;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.PartitionConfig;
import org.safehaus.penrose.source.SourceConfigManager;
import org.safehaus.penrose.source.SourceConfig;
import org.safehaus.penrose.util.TextUtil;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.ldap.connection.LDAPConnection;
import org.safehaus.penrose.session.SessionManager;
import org.safehaus.penrose.session.Session;

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

        ConnectionConfigManager connectionConfigManager = getConnectionConfigManager();
        ConnectionConfig connectionConfig = connectionConfigManager.getConnectionConfig(connectionName);

        ConnectionService connectionService;

        if ("LDAP".equals(connectionConfig.getAdapterName())) {
            connectionService = new LDAPConnectionService(jmxService, partitionManager, partitionName, connectionName);

        } else {
            connectionService = new ConnectionService(jmxService, partitionManager, partitionName, connectionName);
        }
        
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

        boolean debug = log.isDebugEnabled();
        String connectionName = connectionConfig.getName();

        if (debug) {
            log.debug(TextUtil.repeat("-", 70));
            log.debug("Validating connection "+connectionName+".");
        }

        Partition partition = getPartition();
        if (partition == null) {
            throw new Exception("Partition is stopped.");
        }

        log.debug("Class: "+connectionConfig.getConnectionClass());
        log.debug("Adapter: "+connectionConfig.getAdapterName());

        log.debug("Parameters:");
        for (String name : connectionConfig.getParameterNames()) {
            log.debug(" - "+name+": "+connectionConfig.getParameter(name));
        }

        ConnectionManager connectionManager = partition.getConnectionManager();
        Connection connection = connectionManager.createConnection(connectionConfig);
        connection.validate();
        connection.destroy();
    }

    public Collection<DN> getNamingContexts(ConnectionConfig connectionConfig) throws Exception {

        boolean debug = log.isDebugEnabled();

        String connectionName = connectionConfig.getName();

        if (debug) {
            log.debug(TextUtil.repeat("-", 70));
            log.debug("Getting naming contexts from "+connectionName+".");
        }

        Partition partition = getPartition();
        if (partition == null) {
            throw new Exception("Partition is stopped.");
        }

        Collection<DN> list = new ArrayList<DN>();

        Session session = null;
        LDAPConnection connection = null;
        LDAPClient client = null;

        try {
            SessionManager sessionManager = partition.getPartitionContext().getSessionManager();
            session = sessionManager.createAdminSession();

            ConnectionManager connectionManager = partition.getConnectionManager();
            connection = (LDAPConnection)connectionManager.createConnection(connectionConfig);

            client = connection.getClient(session);

            SearchRequest request = new SearchRequest();
            request.setScope(SearchRequest.SCOPE_BASE);
            request.setAttributes(new String[] { "*", "+" });

            SearchResponse response = new SearchResponse();

            client.search(request, response);

            if (response.hasNext()) {
                SearchResult result = response.next();
                Attribute namingContexts = result.getAttributes().get("namingContexts");
                if (namingContexts != null) {
                    for (Object value : namingContexts.getValues()) {
                        DN dn = new DN(value.toString());
                        if (debug) log.debug(" - "+dn);
                        list.add(dn);
                    }
                }
            }

        } finally {
            if (client != null) try { client.close(); } catch (Exception e) { log.error(e.getMessage(), e); }
            if (connection != null) try { connection.destroy(); } catch (Exception e) { log.error(e.getMessage(), e); }
            if (session != null) try { session.close(); } catch (Exception e) { log.error(e.getMessage(), e); }
        }

        return list;
    }

    public void createConnection(ConnectionConfig connectionConfig) throws Exception {

        boolean debug = log.isDebugEnabled();

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

    public void renameConnection(String name, String newName) throws Exception {

        boolean debug = log.isDebugEnabled();

        if (debug) {
            log.debug(TextUtil.repeat("-", 70));
            log.debug("Renaming connection "+name+" to "+newName+".");
        }

        PartitionConfig partitionConfig = getPartitionConfig();
        SourceConfigManager sourceConfigManager = partitionConfig.getSourceConfigManager();

        Collection<SourceConfig> sourceConfigs = sourceConfigManager.getSourceConfigsByConnectionName(name);
        if (sourceConfigs != null && !sourceConfigs.isEmpty()) {
            throw new Exception("Connection "+name+" is in use.");
        }

        Partition partition = getPartition();
        boolean running = false;

        if (partition != null) {
            ConnectionManager connectionManager = partition.getConnectionManager();
            running = connectionManager.isRunning(name);
            if (running) connectionManager.stopConnection(name);
        }

        ConnectionService connectionService = getConnectionService(name);
        connectionService.unregister();

        ConnectionConfigManager connectionConfigManager = partitionConfig.getConnectionConfigManager();
        connectionConfigManager.renameConnectionConfig(name, newName);

        if (partition != null) {
            ConnectionManager connectionManager = partition.getConnectionManager();
            if (running) connectionManager.startConnection(newName);
        }

        ConnectionService newConnectionService = getConnectionService(newName);
        newConnectionService.register();
    }

    public void updateConnection(String connectionName, ConnectionConfig connectionConfig) throws Exception {

        boolean debug = log.isDebugEnabled();

        if (debug) {
            log.debug(TextUtil.repeat("-", 70));
            log.debug("Updating connection "+connectionName+".");
        }

        Partition partition = getPartition();
        if (partition == null) {
            PartitionConfig partitionConfig = getPartitionConfig();
            ConnectionConfigManager connectionConfigManager = partitionConfig.getConnectionConfigManager();
            connectionConfigManager.updateConnectionConfig(connectionConfig);

        } else {
            ConnectionManager connectionManager = partition.getConnectionManager();
            connectionManager.updateConnection(connectionConfig);
        }
    }

    public void removeConnection(String connectionName) throws Exception {

        boolean debug = log.isDebugEnabled();

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
