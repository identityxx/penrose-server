package org.safehaus.penrose.management.partition;

import org.safehaus.penrose.connection.ConnectionConfig;
import org.safehaus.penrose.connection.ConnectionConfigManager;
import org.safehaus.penrose.connection.ConnectionManager;
import org.safehaus.penrose.directory.Directory;
import org.safehaus.penrose.directory.DirectoryConfig;
import org.safehaus.penrose.directory.Entry;
import org.safehaus.penrose.directory.EntryConfig;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.management.BaseService;
import org.safehaus.penrose.management.PenroseJMXService;
import org.safehaus.penrose.management.connection.ConnectionService;
import org.safehaus.penrose.management.directory.EntryService;
import org.safehaus.penrose.management.directory.DirectoryService;
import org.safehaus.penrose.management.mapping.MappingService;
import org.safehaus.penrose.management.module.ModuleService;
import org.safehaus.penrose.management.scheduler.SchedulerService;
import org.safehaus.penrose.management.source.SourceService;
import org.safehaus.penrose.mapping.MappingConfig;
import org.safehaus.penrose.mapping.MappingConfigManager;
import org.safehaus.penrose.mapping.MappingManager;
import org.safehaus.penrose.module.ModuleConfig;
import org.safehaus.penrose.module.ModuleConfigManager;
import org.safehaus.penrose.module.ModuleManager;
import org.safehaus.penrose.module.ModuleMapping;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.PartitionConfig;
import org.safehaus.penrose.partition.PartitionManager;
import org.safehaus.penrose.partition.PartitionClient;
import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.session.SessionManager;
import org.safehaus.penrose.source.Source;
import org.safehaus.penrose.source.SourceConfig;
import org.safehaus.penrose.source.SourceConfigManager;
import org.safehaus.penrose.source.SourceManager;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public class PartitionService extends BaseService implements PartitionServiceMBean {

    private PartitionManager partitionManager;
    private String partitionName;

    public PartitionService(PenroseJMXService jmxService, PartitionManager partitionManager, String partitionName) throws Exception {

        this.jmxService = jmxService;
        this.partitionManager = partitionManager;
        this.partitionName = partitionName;
    }

    public String getObjectName() {
        return PartitionClient.getStringObjectName(partitionName);
    }

    public Object getObject() {
        return getPartition();
    }

    public PartitionConfig getPartitionConfig() throws Exception {
        return partitionManager.getPartitionConfig(partitionName);
    }

    public Partition getPartition() {
        return partitionManager.getPartition(partitionName);
    }

    public void start() throws Exception {
        partitionManager.startPartition(partitionName);
    }

    public void stop() throws Exception {
        partitionManager.stopPartition(partitionName);
    }

    public String getStatus() {
        return getPartition() == null ? "STOPPED" : "RUNNING";
    }

    public void store() throws Exception {
        File baseDir;

        if (partitionName.equals("DEFAULT")) {
            baseDir = partitionManager.getHome();

        } else {
            File partitionsDir = partitionManager.getPartitionsDir();
            baseDir = new File(partitionsDir, partitionName);
        }

        PartitionConfig partitionConfig = getPartitionConfig();
        partitionConfig.store(baseDir);
    }

    ////////////////////////////////////////////////////////////////////////////////
    // Directory
    ////////////////////////////////////////////////////////////////////////////////

    public DirectoryService getDirectoryService() throws Exception {

        DirectoryService directoryService = new DirectoryService(jmxService, partitionManager, partitionName);
        directoryService.init();

        return directoryService;
    }

    public Collection<DN> getSuffixes() throws Exception {
        Collection<DN> list = new ArrayList<DN>();
        PartitionConfig partitionConfig = getPartitionConfig();
        for (EntryConfig entryConfig : partitionConfig.getDirectoryConfig().getRootEntryConfigs()) {
            list.add(entryConfig.getDn());
        }
        return list;
    }

    public Collection<String> getRootEntryIds() throws Exception {
        Collection<String> list = new ArrayList<String>();
        PartitionConfig partitionConfig = getPartitionConfig();
        for (EntryConfig entryConfig : partitionConfig.getDirectoryConfig().getRootEntryConfigs()) {
            list.add(entryConfig.getId());
        }
        return list;
    }

    public Collection<String> getEntryIds() throws Exception {
        Collection<String> list = new ArrayList<String>();
        PartitionConfig partitionConfig = getPartitionConfig();
        for (EntryConfig entryConfig : partitionConfig.getDirectoryConfig().getEntryConfigs()) {
            list.add(entryConfig.getId());
        }
        return list;
    }

    public EntryService getEntryService(String entryId) throws Exception {

        EntryService entryService = new EntryService(jmxService, partitionManager, partitionName, entryId);
        entryService.init();

        return entryService;
    }

    public String createEntry(EntryConfig entryConfig) throws Exception {

        PartitionConfig partitionConfig = getPartitionConfig();
        DirectoryConfig directoryConfig = partitionConfig.getDirectoryConfig();
        directoryConfig.addEntryConfig(entryConfig);

        Partition partition = getPartition();
        if (partition != null) {
            try {
                Directory directory = partition.getDirectory();
                directory.createEntry(entryConfig);
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }

        EntryService entryService = getEntryService(entryConfig.getId());
        entryService.register();

        return entryConfig.getId();
    }

    public void updateEntry(String id, EntryConfig entryConfig) throws Exception {

        Partition partition = getPartition();
        Collection<Entry> children = null;

        if (partition != null) {
            try {
                Directory directory = partition.getDirectory();
                Entry oldEntry = directory.removeEntry(id);
                children = oldEntry.getChildren();
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }

        EntryService oldEntryService = getEntryService(id);
        oldEntryService.unregister();

        PartitionConfig partitionConfig = getPartitionConfig();
        DirectoryConfig directoryConfig = partitionConfig.getDirectoryConfig();
        directoryConfig.updateEntryConfig(id, entryConfig);

        if (partition != null) {
            try {
                Directory directory = partition.getDirectory();
                Entry newEntry = directory.createEntry(entryConfig);
                if (children != null) newEntry.addChildren(children);
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }

        EntryService newEntryService = getEntryService(entryConfig.getId());
        newEntryService.register();
    }

    public void removeEntry(String id) throws Exception {

        Partition partition = getPartition();
        if (partition != null) {
            try {
                Directory directory = partition.getDirectory();
                directory.removeEntry(id);
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }

        PartitionConfig partitionConfig = getPartitionConfig();
        DirectoryConfig directoryConfig = partitionConfig.getDirectoryConfig();
        directoryConfig.removeEntryConfig(id);

        EntryService entryService = getEntryService(id);
        entryService.unregister();
    }

    ////////////////////////////////////////////////////////////////////////////////
    // Connections
    ////////////////////////////////////////////////////////////////////////////////

    public Collection<String> getConnectionNames() throws Exception {

        PartitionConfig partitionConfig = getPartitionConfig();
        ConnectionConfigManager connectionConfigManager = partitionConfig.getConnectionConfigManager();

        Collection<String> list = new ArrayList<String>();
        list.addAll(connectionConfigManager.getConnectionNames());

        return list;
    }

    public ConnectionService getConnectionService(String connectionName) throws Exception {

        ConnectionService connectionService = new ConnectionService(jmxService, partitionManager, partitionName, connectionName);
        connectionService.init();

        return connectionService;
    }

    public void startConnection(String connectionName) throws Exception {

        Partition partition = getPartition();
        ConnectionManager connectionManager = partition.getConnectionManager();
        connectionManager.startConnection(connectionName);
    }

    public void stopConnection(String connectionName) throws Exception {

        Partition partition = getPartition();
        ConnectionManager connectionManager = partition.getConnectionManager();
        connectionManager.stopConnection(connectionName);
    }

    public void createConnection(ConnectionConfig connectionConfig) throws Exception {

        PartitionConfig partitionConfig = getPartitionConfig();
        ConnectionConfigManager connectionConfigManager = partitionConfig.getConnectionConfigManager();
        connectionConfigManager.addConnectionConfig(connectionConfig);

        Partition partition = getPartition();
        if (partition != null) {
            String connectionName = connectionConfig.getName();
            ConnectionManager connectionManager = partition.getConnectionManager();
            connectionManager.startConnection(connectionName);
        }

        ConnectionService connectionService = getConnectionService(connectionConfig.getName());
        connectionService.register();
    }

    public void updateConnection(String connectionName, ConnectionConfig connectionConfig) throws Exception {

        PartitionConfig partitionConfig = getPartitionConfig();
        SourceConfigManager sourceConfigManager = partitionConfig.getSourceConfigManager();

        Collection<SourceConfig> sourceConfigs = sourceConfigManager.getSourceConfigsByConnectionName(connectionName);
        if (sourceConfigs != null && !sourceConfigs.isEmpty()) {
            throw new Exception("Connection "+connectionName+" is in use.");
        }

        Partition partition = getPartition();
        if (partition != null) {
            ConnectionManager connectionManager = partition.getConnectionManager();
            boolean running = connectionManager.isRunning(connectionName);
            if (running) connectionManager.stopConnection(connectionName);
        }

        ConnectionConfigManager connectionConfigManager = partitionConfig.getConnectionConfigManager();
        connectionConfigManager.updateConnectionConfig(connectionName, connectionConfig);

        if (partition != null) {
            ConnectionManager connectionManager = partition.getConnectionManager();
            boolean running = connectionManager.isRunning(connectionName);
            if (running) connectionManager.startConnection(connectionConfig.getName());
        }
    }

    public void removeConnection(String connectionName) throws Exception {

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
            if (running) stopConnection(connectionName);
        }

        ConnectionConfigManager connectionConfigManager = partitionConfig.getConnectionConfigManager();
        connectionConfigManager.removeConnectionConfig(connectionName);
    }

    ////////////////////////////////////////////////////////////////////////////////
    // Sources
    ////////////////////////////////////////////////////////////////////////////////

    public Collection<String> getSourceNames() throws Exception {

        PartitionConfig partitionConfig = getPartitionConfig();
        SourceConfigManager sourceConfigManager = partitionConfig.getSourceConfigManager();

        Collection<String> list = new ArrayList<String>();
        list.addAll(sourceConfigManager.getSourceNames());

        return list;
    }

    public SourceService getSourceService(String sourceName) throws Exception {

        SourceService sourceService = new SourceService(jmxService, partitionManager, partitionName, sourceName);
        sourceService.init();

        return sourceService;
    }

    public void createSource(SourceConfig sourceConfig) throws Exception {

        Partition partition = getPartition();

        SourceManager sourceManager = partition.getSourceManager();
        Source source = sourceManager.createSource(sourceConfig);

        SourceConfigManager sourceConfigManager = sourceManager.getSourceConfigManager();
        sourceConfigManager.addSourceConfig(sourceConfig);

        SourceService sourceService = getSourceService(source.getName());
        sourceService.register();
    }

    public void updateSource(String name, SourceConfig sourceConfig) throws Exception {

        Partition partition = getPartition();

        SourceManager sourceManager = partition.getSourceManager();

        Source oldSource = sourceManager.removeSource(name);
        SourceService oldSourceService = getSourceService(oldSource.getName());
        oldSourceService.unregister();

        sourceManager.updateSourceConfig(name, sourceConfig);

        Source newSource = sourceManager.createSource(sourceConfig);
        SourceService newSourceService = getSourceService(newSource.getName());
        newSourceService.register();
    }

    public void removeSource(String name) throws Exception {

        Partition partition = getPartition();

        Directory directory = partition.getDirectory();
        Collection<Entry> entries = directory.getEntriesBySourceName(name);
        if (entries != null && !entries.isEmpty()) {
            throw new Exception("Source "+name+" is in use.");
        }

        SourceManager sourceManager = partition.getSourceManager();
        Source source = sourceManager.removeSource(name);

        SourceConfigManager sourceConfigManager = sourceManager.getSourceConfigManager();
        sourceConfigManager.removeSourceConfig(name);

        SourceService sourceService = getSourceService(source.getName());
        sourceService.unregister();
    }

    ////////////////////////////////////////////////////////////////////////////////
    // Mappings
    ////////////////////////////////////////////////////////////////////////////////

    public Collection<String> getMappingNames() throws Exception {

        PartitionConfig partitionConfig = getPartitionConfig();
        MappingConfigManager mappingConfigManager = partitionConfig.getMappingConfigManager();

        Collection<String> list = new ArrayList<String>();
        list.addAll(mappingConfigManager.getMappingNames());

        return list;
    }

    public MappingService getMappingService(String mappingName) throws Exception {

        MappingService mappingService = new MappingService(jmxService, partitionManager, partitionName, mappingName);
        mappingService.init();

        return mappingService;
    }

    public void startMapping(String mappingName) throws Exception {

        Partition partition = getPartition();
        if (partition == null) return;

        MappingManager mappingManager = partition.getMappingManager();
        mappingManager.startMapping(mappingName);
    }

    public void stopMapping(String mappingName) throws Exception {

        Partition partition = getPartition();
        if (partition == null) return;

        MappingManager mappingManager = partition.getMappingManager();
        boolean running = mappingManager.isRunning(mappingName);
        if (running) mappingManager.stopMapping(mappingName);
    }

    public void createMapping(MappingConfig mappingConfig) throws Exception {

        PartitionConfig partitionConfig = getPartitionConfig();
        MappingConfigManager mappingConfigManager = partitionConfig.getMappingConfigManager();
        mappingConfigManager.addMappingConfig(mappingConfig);

        String mappingName = mappingConfig.getName();
        startMapping(mappingName);

        MappingService mappingService = getMappingService(mappingName);
        mappingService.register();
    }

    public void updateMapping(String mappingName, MappingConfig mappingConfig) throws Exception {

        PartitionConfig partitionConfig = getPartitionConfig();

        stopMapping(mappingName);

        MappingConfigManager mappingConfigManager = partitionConfig.getMappingConfigManager();
        mappingConfigManager.updateMappingConfig(mappingName, mappingConfig);

        startMapping(mappingName);
    }

    public void removeMapping(String mappingName) throws Exception {

        PartitionConfig partitionConfig = getPartitionConfig();

        MappingService mappingService = getMappingService(mappingName);
        mappingService.unregister();

        stopMapping(mappingName);

        MappingConfigManager mappingConfigManager = partitionConfig.getMappingConfigManager();
        mappingConfigManager.removeMappingConfig(mappingName);
    }

    ////////////////////////////////////////////////////////////////////////////////
    // Modules
    ////////////////////////////////////////////////////////////////////////////////

    public Collection<String> getModuleNames() throws Exception {

        PartitionConfig partitionConfig = getPartitionConfig();
        ModuleConfigManager moduleConfigManager = partitionConfig.getModuleConfigManager();

        Collection<String> list = new ArrayList<String>();
        list.addAll(moduleConfigManager.getModuleNames());

        return list;
    }

    public ModuleService getModuleService(String moduleName) throws Exception {

        ModuleService moduleService = new ModuleService(jmxService, partitionManager, partitionName, moduleName);
        moduleService.init();

        return moduleService;
    }

    public void createModule(ModuleConfig moduleConfig) throws Exception {

        Partition partition = getPartition();
        if (partition != null) {
            ModuleManager moduleManager = partition.getModuleManager();
            moduleManager.createModule(moduleConfig);
        }

        String moduleName = moduleConfig.getName();

        PartitionConfig partitionConfig = getPartitionConfig();
        ModuleConfigManager moduleConfigManager = partitionConfig.getModuleConfigManager();
        moduleConfigManager.addModuleConfig(moduleConfig);

        ModuleService moduleService = getModuleService(moduleName);
        moduleService.register();
    }

    public void createModule(ModuleConfig moduleConfig, Collection<ModuleMapping> moduleMappings) throws Exception {

        Partition partition = getPartition();
        if (partition != null) {
            ModuleManager moduleManager = partition.getModuleManager();
            moduleManager.createModule(moduleConfig);
        }

        String moduleName = moduleConfig.getName();

        PartitionConfig partitionConfig = getPartitionConfig();
        ModuleConfigManager moduleConfigManager = partitionConfig.getModuleConfigManager();
        moduleConfigManager.addModuleConfig(moduleConfig);
        moduleConfigManager.addModuleMappings(moduleMappings);

        ModuleService moduleService = getModuleService(moduleName);
        moduleService.register();
    }

    public void updateModule(String name, ModuleConfig moduleConfig) throws Exception {

        Partition partition = getPartition();

        if (partition != null) {
            ModuleManager moduleManager = partition.getModuleManager();
            moduleManager.removeModule(name);
        }

        ModuleService oldModuleService = getModuleService(name);
        oldModuleService.unregister();

        PartitionConfig partitionConfig = getPartitionConfig();
        ModuleConfigManager moduleConfigManager = partitionConfig.getModuleConfigManager();
        moduleConfigManager.updateModuleConfig(name, moduleConfig);

        if (partition != null) {
            ModuleManager moduleManager = partition.getModuleManager();
            moduleManager.createModule(moduleConfig);
        }

        ModuleService newModuleService = getModuleService(moduleConfig.getName());
        newModuleService.register();
    }

    public void removeModule(String name) throws Exception {

        Partition partition = getPartition();

        if (partition != null) {
            ModuleManager moduleManager = partition.getModuleManager();
            moduleManager.removeModule(name);
        }

        PartitionConfig partitionConfig = getPartitionConfig();
        ModuleConfigManager moduleConfigManager = partitionConfig.getModuleConfigManager();
        moduleConfigManager.removeModuleConfig(name);

        ModuleService moduleService = getModuleService(name);
        moduleService.unregister();
    }

    ////////////////////////////////////////////////////////////////////////////////
    // Scheduler
    ////////////////////////////////////////////////////////////////////////////////

    public SchedulerService getSchedulerService() throws Exception {

        SchedulerService schedulerService = new SchedulerService(jmxService, partitionManager, partitionName);
        schedulerService.init();

        return schedulerService;
    }

    public void register() throws Exception {
        jmxService.register(getObjectName(), this);

        PartitionConfig partitionConfig = getPartitionConfig();

        ConnectionConfigManager connectionConfigManager = partitionConfig.getConnectionConfigManager();
        for (String connectionName : connectionConfigManager.getConnectionNames()) {
            ConnectionService connectionService = getConnectionService(connectionName);
            connectionService.register();
        }

        SourceConfigManager sourceConfigManager = partitionConfig.getSourceConfigManager();
        for (String sourceName : sourceConfigManager.getSourceNames()) {
            SourceService sourceService = getSourceService(sourceName);
            sourceService.register();
        }

        MappingConfigManager mappingConfigManager = partitionConfig.getMappingConfigManager();
        for (String mappingName : mappingConfigManager.getMappingNames()) {
            MappingService mappingService = getMappingService(mappingName);
            mappingService.register();
        }

        DirectoryService directoryService = getDirectoryService();
        directoryService.register();

        ModuleConfigManager moduleConfigManager = partitionConfig.getModuleConfigManager();
        for (String moduleName : moduleConfigManager.getModuleNames()) {
            ModuleService moduleService = getModuleService(moduleName);
            moduleService.register();
        }

        SchedulerService schedulerService = getSchedulerService();
        if (schedulerService != null) schedulerService.register();
    }

    public void unregister() throws Exception {

        PartitionConfig partitionConfig = getPartitionConfig();
        if (partitionConfig == null) return;

        SchedulerService schedulerService = getSchedulerService();
        if (schedulerService != null) schedulerService.unregister();

        ModuleConfigManager moduleConfigManager = partitionConfig.getModuleConfigManager();
        for (String moduleName : moduleConfigManager.getModuleNames()) {
            ModuleService moduleService = getModuleService(moduleName);
            moduleService.unregister();
        }

        DirectoryService directoryService = getDirectoryService();
        directoryService.unregister();

        MappingConfigManager mappingConfigManager = partitionConfig.getMappingConfigManager();
        for (String mappingName : mappingConfigManager.getMappingNames()) {
            MappingService mappingService = getMappingService(mappingName);
            mappingService.unregister();
        }

        SourceConfigManager sourceConfigManager = partitionConfig.getSourceConfigManager();
        for (String sourceName : sourceConfigManager.getSourceNames()) {
            SourceService sourceService = getSourceService(sourceName);
            sourceService.unregister();
        }

        ConnectionConfigManager connectionConfigManager = partitionConfig.getConnectionConfigManager();
        for (String connectionName : connectionConfigManager.getConnectionNames()) {
            ConnectionService connectionService = getConnectionService(connectionName);
            connectionService.unregister();
        }

        jmxService.unregister(getObjectName());
    }

    public PenroseJMXService getJmxService() {
        return jmxService;
    }

    public void setJmxService(PenroseJMXService jmxService) {
        this.jmxService = jmxService;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Add
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public AddResponse add(
            String dn,
            Attributes attributes
    ) throws Exception {

        AddRequest request = new AddRequest();
        request.setDn(dn);
        request.setAttributes(attributes);

        AddResponse response = new AddResponse();

        return add(request, response);
    }

    public AddResponse add(
            RDN rdn,
            Attributes attributes
    ) throws Exception {

        AddRequest request = new AddRequest();
        request.setDn(rdn);
        request.setAttributes(attributes);

        AddResponse response = new AddResponse();

        return add(request, response);
    }

    public AddResponse add(
            DN dn,
            Attributes attributes
    ) throws Exception {

        AddRequest request = new AddRequest();
        request.setDn(dn);
        request.setAttributes(attributes);

        AddResponse response = new AddResponse();

        return add(request, response);
    }

    public AddResponse add(
            AddRequest request,
            AddResponse response
    ) throws Exception {

        Session session = getSession();

        try {
            Partition partition = getPartition();
            partition.add(session, request, response);

            int rc = response.getReturnCode();
            log.debug("RC: "+rc);

            return response;

        } finally {
            session.close();
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Delete
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public DeleteResponse delete(
            String dn
    ) throws Exception {

        DeleteRequest request = new DeleteRequest();
        request.setDn(dn);

        DeleteResponse response = new DeleteResponse();

        return delete(request, response);
    }

    public DeleteResponse delete(
            RDN rdn
    ) throws Exception {

        DeleteRequest request = new DeleteRequest();
        request.setDn(rdn);

        DeleteResponse response = new DeleteResponse();

        return delete(request, response);
    }

    public DeleteResponse delete(
            DN dn
    ) throws Exception {

        DeleteRequest request = new DeleteRequest();
        request.setDn(dn);

        DeleteResponse response = new DeleteResponse();

        return delete(request, response);
    }

    public DeleteResponse delete(
            DeleteRequest request,
            DeleteResponse response
    ) throws Exception {

        Session session = getSession();

        try {
            Partition partition = getPartition();
            partition.delete(session, request, response);

            int rc = response.getReturnCode();
            log.debug("RC: "+rc);

            return response;

        } finally {
            session.close();
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Find
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public SearchResult find(
            String dn
    ) throws Exception {

        return find(new DN(dn));
    }

    public SearchResult find(
            RDN rdn
    ) throws Exception {

        return find(new DN(rdn));
    }

    public SearchResult find(
            DN dn
    ) throws Exception {

        Session session = getSession();

        try {
            Partition partition = getPartition();
            return partition.find(session, dn);

        } finally {
            session.close();
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Modify
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public ModifyResponse modify(
            String dn,
            Collection<Modification> modifications
    ) throws Exception {

        ModifyRequest request = new ModifyRequest();
        request.setDn(dn);
        request.setModifications(modifications);

        ModifyResponse response = new ModifyResponse();

        return modify(request, response);
    }

    public ModifyResponse modify(
            RDN rdn,
            Collection<Modification> modifications
    ) throws Exception {

        ModifyRequest request = new ModifyRequest();
        request.setDn(rdn);
        request.setModifications(modifications);

        ModifyResponse response = new ModifyResponse();

        return modify(request, response);
    }

    public ModifyResponse modify(
            DN dn,
            Collection<Modification> modifications
    ) throws Exception {

        ModifyRequest request = new ModifyRequest();
        request.setDn(dn);
        request.setModifications(modifications);

        ModifyResponse response = new ModifyResponse();

        return modify(request, response);
    }

    public ModifyResponse modify(
            ModifyRequest request,
            ModifyResponse response
    ) throws Exception {

        Session session = getSession();

        try {
            Partition partition = getPartition();
            partition.modify(session, request, response);

            int rc = response.getReturnCode();
            log.debug("RC: "+rc);

            return response;

        } finally {
            session.close();
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // ModRdn
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public ModRdnResponse modrdn(
            String dn,
            String newRdn,
            boolean deleteOldRdn
    ) throws Exception {

        ModRdnRequest request = new ModRdnRequest();
        request.setDn(dn);
        request.setNewRdn(newRdn);
        request.setDeleteOldRdn(deleteOldRdn);

        ModRdnResponse response = new ModRdnResponse();

        return modrdn(request, response);
    }

    public ModRdnResponse modrdn(
            RDN dn,
            RDN newRdn,
            boolean deleteOldRdn
    ) throws Exception {

        ModRdnRequest request = new ModRdnRequest();
        request.setDn(dn);
        request.setNewRdn(newRdn);
        request.setDeleteOldRdn(deleteOldRdn);

        ModRdnResponse response = new ModRdnResponse();

        return modrdn(request, response);
    }

    public ModRdnResponse modrdn(
            DN dn,
            RDN newRdn,
            boolean deleteOldRdn
    ) throws Exception {

        ModRdnRequest request = new ModRdnRequest();
        request.setDn(dn);
        request.setNewRdn(newRdn);
        request.setDeleteOldRdn(deleteOldRdn);

        ModRdnResponse response = new ModRdnResponse();

        return modrdn(request, response);
    }

    public ModRdnResponse modrdn(
            ModRdnRequest request,
            ModRdnResponse response
    ) throws Exception {

        Session session = getSession();

        try {
            Partition partition = getPartition();
            partition.modrdn(session, request, response);

            int rc = response.getReturnCode();
            log.debug("RC: "+rc);

            return response;

        } finally {
            session.close();
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Search
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public SearchResponse search(
            String dn,
            String filter,
            Integer scope
    ) throws Exception {

        SearchRequest request = new SearchRequest();
        request.setDn(dn);
        request.setFilter(filter);
        request.setScope(scope);

        SearchResponse response = new SearchResponse();

        return search(request, response);
    }

    public SearchResponse search(
            RDN rdn,
            Filter filter,
            Integer scope
    ) throws Exception {

        SearchRequest request = new SearchRequest();
        request.setDn(rdn);
        request.setFilter(filter);
        request.setScope(scope);

        SearchResponse response = new SearchResponse();

        return search(request, response);
    }

    public SearchResponse search(
            DN dn,
            Filter filter,
            Integer scope
    ) throws Exception {

        SearchRequest request = new SearchRequest();
        request.setDn(dn);
        request.setFilter(filter);
        request.setScope(scope);

        SearchResponse response = new SearchResponse();

        return search(request, response);
    }

    public SearchResponse search(
            SearchRequest request,
            SearchResponse response
    ) throws Exception {

        Session session = getSession();

        try {
            Partition partition = getPartition();
            partition.search(session, request, response);

            int rc = response.waitFor();
            log.debug("RC: "+rc);

            return response;

        } finally {
            session.close();
        }
    }

    public Session getSession() throws Exception {
        Partition partition = getPartition();
        SessionManager sessionManager = partition.getPartitionContext().getSessionManager();
        return sessionManager.createAdminSession();
    }
}
