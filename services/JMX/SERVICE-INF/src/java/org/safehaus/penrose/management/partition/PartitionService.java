package org.safehaus.penrose.management.partition;

import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.management.BaseService;
import org.safehaus.penrose.management.PenroseJMXService;
import org.safehaus.penrose.management.connection.ConnectionManagerService;
import org.safehaus.penrose.management.directory.DirectoryService;
import org.safehaus.penrose.management.mapping.MappingManagerService;
import org.safehaus.penrose.management.module.ModuleManagerService;
import org.safehaus.penrose.management.scheduler.SchedulerService;
import org.safehaus.penrose.management.source.SourceManagerService;
import org.safehaus.penrose.partition.*;
import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.session.SessionManager;

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
        return getPartition() == null ? "STOPPED" : "STARTED";
    }

    public void store() throws Exception {
        partitionManager.storePartition(partitionName);
    }

    ////////////////////////////////////////////////////////////////////////////////
    // Directory
    ////////////////////////////////////////////////////////////////////////////////

    public DirectoryService getDirectoryService() throws Exception {

        DirectoryService directoryService = new DirectoryService(jmxService, partitionManager, partitionName);
        directoryService.init();

        return directoryService;
    }

    ////////////////////////////////////////////////////////////////////////////////
    // Connections
    ////////////////////////////////////////////////////////////////////////////////

    public ConnectionManagerService getConnectionManagerService() throws Exception {

        ConnectionManagerService connectionManagerService = new ConnectionManagerService(jmxService, partitionManager, partitionName);
        connectionManagerService.init();

        return connectionManagerService;
    }

    ////////////////////////////////////////////////////////////////////////////////
    // Sources
    ////////////////////////////////////////////////////////////////////////////////

    public SourceManagerService getSourceManagerService() throws Exception {

        SourceManagerService sourceManagerService = new SourceManagerService(jmxService, partitionManager, partitionName);
        sourceManagerService.init();

        return sourceManagerService;
    }

    ////////////////////////////////////////////////////////////////////////////////
    // Mappings
    ////////////////////////////////////////////////////////////////////////////////

    public MappingManagerService getMappingManagerService() throws Exception {

        MappingManagerService mappingManagerService = new MappingManagerService(jmxService, partitionManager, partitionName);
        mappingManagerService.init();

        return mappingManagerService;
    }

    ////////////////////////////////////////////////////////////////////////////////
    // Modules
    ////////////////////////////////////////////////////////////////////////////////

    public ModuleManagerService getModuleManagerService() throws Exception {

        ModuleManagerService moduleManagerService = new ModuleManagerService(jmxService, partitionManager, partitionName);
        moduleManagerService.init();

        return moduleManagerService;
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

        //log.debug("Registering partition "+partitionName+".");

        super.register();

        ConnectionManagerService connectionManagerService = getConnectionManagerService();
        connectionManagerService.register();

        SourceManagerService sourceManagerService = getSourceManagerService();
        sourceManagerService.register();

        MappingManagerService mappingManagerService = getMappingManagerService();
        mappingManagerService.register();

        DirectoryService directoryService = getDirectoryService();
        directoryService.register();

        ModuleManagerService moduleManagerService = getModuleManagerService();
        moduleManagerService.register();

        SchedulerService schedulerService = getSchedulerService();
        if (schedulerService != null) schedulerService.register();
    }

    public void unregister() throws Exception {

        //log.debug("Unregistering partition "+partitionName+".");

        PartitionConfig partitionConfig = getPartitionConfig();
        if (partitionConfig == null) return;

        SchedulerService schedulerService = getSchedulerService();
        if (schedulerService != null) schedulerService.unregister();

        ModuleManagerService moduleManagerService = getModuleManagerService();
        moduleManagerService.unregister();

        DirectoryService directoryService = getDirectoryService();
        directoryService.unregister();

        MappingManagerService mappingManagerService = getMappingManagerService();
        mappingManagerService.unregister();

        SourceManagerService sourceManagerService = getSourceManagerService();
        sourceManagerService.unregister();

        ConnectionManagerService connectionManagerService = getConnectionManagerService();
        connectionManagerService.unregister();

        super.unregister();
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

        boolean debug = log.isDebugEnabled();
        Partition partition = getPartition();
        if (debug) log.debug("Adding "+request.getDn()+".");

        Session session = getSession();

        try {
            partition.add(session, request, response);

            int rc = response.getReturnCode();
            log.debug("RC: "+rc);

            return response;

        } finally {
            session.close();
            if (debug) log.debug("Add completed.");
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

        boolean debug = log.isDebugEnabled();
        Partition partition = getPartition();
        if (debug) log.debug("Deleting "+request.getDn()+".");

        Session session = getSession();

        try {
            partition.delete(session, request, response);

            int rc = response.getReturnCode();
            log.debug("RC: "+rc);

            return response;

        } finally {
            session.close();
            if (debug) log.debug("Delete completed.");
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

        boolean debug = log.isDebugEnabled();
        Partition partition = getPartition();
        if (debug) log.debug("Finding "+dn+".");

        Session session = getSession();

        try {
            return partition.find(session, dn);

        } finally {
            session.close();
            if (debug) log.debug("Find completed.");
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

        boolean debug = log.isDebugEnabled();
        Partition partition = getPartition();
        if (debug) log.debug("Modifying "+request.getDn()+".");

        Session session = getSession();

        try {
            partition.modify(session, request, response);

            int rc = response.getReturnCode();
            log.debug("RC: "+rc);

            return response;

        } finally {
            session.close();
            if (debug) log.debug("Modify completed.");
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

        boolean debug = log.isDebugEnabled();
        Partition partition = getPartition();
        if (debug) log.debug("Renaming "+request.getDn()+".");

        Session session = getSession();

        try {
            partition.modrdn(session, request, response);

            int rc = response.getReturnCode();
            log.debug("RC: "+rc);

            return response;

        } finally {
            session.close();
            if (debug) log.debug("Rename completed.");
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

        boolean debug = log.isDebugEnabled();
        Partition partition = getPartition();
        if (debug) log.debug("Searching "+request.getDn()+".");

        Session session = getSession();

        try {
            partition.search(session, request, response);

            int rc = response.waitFor();
            log.debug("RC: "+rc);

            return response;

        } finally {
            session.close();
            if (debug) log.debug("Search completed.");
        }
    }

    public Session getSession() throws Exception {
        Partition partition = getPartition();
        SessionManager sessionManager = partition.getPartitionContext().getSessionManager();
        return sessionManager.createAdminSession();
    }
}
