package org.safehaus.penrose.management;

import org.safehaus.penrose.partition.*;
import org.safehaus.penrose.source.Source;
import org.safehaus.penrose.module.Module;
import org.safehaus.penrose.connection.Connection;
import org.safehaus.penrose.directory.Entry;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.scheduler.Scheduler;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.session.SessionManager;
import org.safehaus.penrose.session.Session;

import java.util.Collection;
import java.util.ArrayList;

/**
 * @author Endi Sukma Dewata
 */
public class PartitionService extends JMXService implements PartitionServiceMBean {

    private Partition partition;

    public PartitionService(Partition partition) throws Exception {
        super(partition);

        this.partition = partition;
    }

    public String getStatus() {
        return "RUNNING";
    }
    
    public PartitionConfig getPartitionConfig() throws Exception {
        return partition.getPartitionConfig();
    }

    ////////////////////////////////////////////////////////////////////////////////
    // Directory
    ////////////////////////////////////////////////////////////////////////////////

    public Collection<DN> getSuffixes() {
        Collection<DN> list = new ArrayList<DN>();
        for (Entry entry : partition.getDirectory().getRootEntries()) {
            list.add(entry.getDn());
        }
        return list;
    }

    public Collection<String> getRootEntryIds() {
        Collection<String> list = new ArrayList<String>();
        for (Entry entry : partition.getDirectory().getRootEntries()) {
            list.add(entry.getId());
        }
        return list;
    }

    public Collection<String> getEntryIds() {
        Collection<String> list = new ArrayList<String>();
        for (Entry entry : partition.getDirectory().getEntries()) {
            list.add(entry.getId());
        }
        return list;
    }

    public EntryService getEntryService(String entryId) throws Exception {
        Entry entry = partition.getDirectory().getEntry(entryId);
        if (entry == null) return null;

        return getEntryService(entry);
    }

    public EntryService getEntryService(Entry entry) throws Exception {

        EntryService entryService = new EntryService();
        entryService.setJmxService(jmxService);
        entryService.setPartition(partition);
        entryService.setEntry(entry);

        return entryService;
    }

    public Collection<EntryService> getEntryServices() throws Exception {

        Collection<EntryService> list = new ArrayList<EntryService>();
        for (Entry entry : partition.getDirectory().getEntries()) {
            list.add(getEntryService(entry));
        }

        return list;
    }

    ////////////////////////////////////////////////////////////////////////////////
    // Connections
    ////////////////////////////////////////////////////////////////////////////////

    public Collection<String> getConnectionNames() {
        Collection<String> list = new ArrayList<String>();
        for (Connection connection : partition.getConnections().getConnections()) {
            list.add(connection.getName());
        }
        return list;
    }

    public ConnectionService getConnectionService(String connectionName) throws Exception {
        Connection connection = partition.getConnection(connectionName);
        if (connection == null) return null;

        return getConnectionService(connection);
    }

    public ConnectionService getConnectionService(Connection connection) throws Exception {

        ConnectionService connectionService = new ConnectionService(connection);
        connectionService.setJmxService(jmxService);
        connectionService.init();

        return connectionService;
    }

    public Collection<ConnectionService> getConnectionServices() throws Exception {

        Collection<ConnectionService> list = new ArrayList<ConnectionService>();

        for (Connection connection : partition.getConnections().getConnections()) {
            list.add(getConnectionService(connection));
        }

        return list;
    }

    ////////////////////////////////////////////////////////////////////////////////
    // Sources
    ////////////////////////////////////////////////////////////////////////////////

    public Collection<String> getSourceNames() {
        Collection<String> list = new ArrayList<String>();
        for (Source source : partition.getSources()) {
            list.add(source.getName());
        }
        return list;
    }

    public SourceService getSourceService(String sourceName) throws Exception {
        Source source = partition.getSource(sourceName);
        if (source == null) return null;

        return getSourceService(source);
    }

    public SourceService getSourceService(Source source) throws Exception {

        SourceService sourceService = new SourceService(source);
        sourceService.setJmxService(jmxService);
        sourceService.init();

        return sourceService;
    }
    
    public Collection<SourceService> getSourceServices() throws Exception {

        Collection<SourceService> list = new ArrayList<SourceService>();

        for (Source source : partition.getSources()) {
            list.add(getSourceService(source));
        }

        return list;
    }

    ////////////////////////////////////////////////////////////////////////////////
    // Modules
    ////////////////////////////////////////////////////////////////////////////////

    public Collection<String> getModuleNames() {
        Collection<String> list = new ArrayList<String>();
        for (Module module : partition.getModules()) {
            list.add(module.getName());
        }
        return list;
    }

    public ModuleService getModuleService(String moduleName) throws Exception {
        Module module = partition.getModule(moduleName);
        if (module == null) return null;

        return getModuleService(module);
    }

    public ModuleService getModuleService(Module module) throws Exception {

        ModuleService moduleService = new ModuleService(module);
        moduleService.setJmxService(jmxService);
        moduleService.init();

        return moduleService;
    }

    public Collection<ModuleService> getModuleServices() throws Exception {

        Collection<ModuleService> list = new ArrayList<ModuleService>();

        for (Module module : partition.getModules()) {
            list.add(getModuleService(module));
        }

        return list;
    }

    ////////////////////////////////////////////////////////////////////////////////
    // Scheduler
    ////////////////////////////////////////////////////////////////////////////////

    public SchedulerService getSchedulerService() throws Exception {

        Scheduler scheduler = partition.getScheduler();
        if (scheduler == null) return null;

        return getSchedulerService(scheduler);
    }

    public SchedulerService getSchedulerService(Scheduler scheduler) throws Exception {

        SchedulerService schedulerService = new SchedulerService();
        schedulerService.setJmxService(jmxService);
        schedulerService.setPartition(partition);
        schedulerService.setScheduler(scheduler);

        return schedulerService;
    }

    public String getObjectName() {
        return PartitionClient.getObjectName(partition.getName());
    }

    public void register() throws Exception {
        jmxService.register(getObjectName(), this);

        for (ConnectionService connectionService : getConnectionServices()) {
            connectionService.register();
        }

        for (SourceService sourceService : getSourceServices()) {
            sourceService.register();
        }

        for (EntryService entryService : getEntryServices()) {
            entryService.register();
        }

        for (ModuleService moduleService : getModuleServices()) {
            moduleService.register();
        }

        SchedulerService schedulerService = getSchedulerService();
        if (schedulerService != null) schedulerService.register();
    }

    public void unregister() throws Exception {

        SchedulerService schedulerService = getSchedulerService();
        if (schedulerService != null) schedulerService.unregister();

        for (ModuleService moduleService : getModuleServices()) {
            moduleService.unregister();
        }

        for (EntryService entryService : getEntryServices()) {
            entryService.unregister();
        }

        for (SourceService sourceService : getSourceServices()) {
            sourceService.unregister();
        }

        for (ConnectionService connectionService : getConnectionServices()) {
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

        SessionManager sessionManager = partition.getPartitionContext().getSessionManager();
        Session session = sessionManager.newAdminSession();

        try {
            AddRequest request = new AddRequest();
            request.setDn(dn);
            request.setAttributes(attributes);

            AddResponse response = new AddResponse();

            partition.add(session, request, response);

            int rc = response.getReturnCode();
            log.debug("RC: "+rc);

            return response;

        } finally {
            session.close();
        }
    }

    public AddResponse add(
            RDN rdn,
            Attributes attributes
    ) throws Exception {

        SessionManager sessionManager = partition.getPartitionContext().getSessionManager();
        Session session = sessionManager.newAdminSession();

        try {
            AddRequest request = new AddRequest();
            request.setDn(rdn);
            request.setAttributes(attributes);

            AddResponse response = new AddResponse();

            partition.add(session, request, response);

            int rc = response.getReturnCode();
            log.debug("RC: "+rc);

            return response;

        } finally {
            session.close();
        }
    }

    public AddResponse add(
            DN dn,
            Attributes attributes
    ) throws Exception {

        SessionManager sessionManager = partition.getPartitionContext().getSessionManager();
        Session session = sessionManager.newAdminSession();

        try {
            AddRequest request = new AddRequest();
            request.setDn(dn);
            request.setAttributes(attributes);

            AddResponse response = new AddResponse();

            partition.add(session, request, response);

            int rc = response.getReturnCode();
            log.debug("RC: "+rc);

            return response;

        } finally {
            session.close();
        }
    }

    public AddResponse add(
            AddRequest request,
            AddResponse response
    ) throws Exception {

        SessionManager sessionManager = partition.getPartitionContext().getSessionManager();
        Session session = sessionManager.newAdminSession();

        try {
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

        SessionManager sessionManager = partition.getPartitionContext().getSessionManager();
        Session session = sessionManager.newAdminSession();

        try {
            DeleteRequest request = new DeleteRequest();
            request.setDn(dn);

            DeleteResponse response = new DeleteResponse();

            partition.delete(session, request, response);

            int rc = response.getReturnCode();
            log.debug("RC: "+rc);

            return response;

        } finally {
            session.close();
        }
    }

    public DeleteResponse delete(
            RDN rdn
    ) throws Exception {

        SessionManager sessionManager = partition.getPartitionContext().getSessionManager();
        Session session = sessionManager.newAdminSession();

        try {
            DeleteRequest request = new DeleteRequest();
            request.setDn(rdn);

            DeleteResponse response = new DeleteResponse();

            partition.delete(session, request, response);

            int rc = response.getReturnCode();
            log.debug("RC: "+rc);

            return response;

        } finally {
            session.close();
        }
    }

    public DeleteResponse delete(
            DN dn
    ) throws Exception {

        SessionManager sessionManager = partition.getPartitionContext().getSessionManager();
        Session session = sessionManager.newAdminSession();

        try {
            DeleteRequest request = new DeleteRequest();
            request.setDn(dn);

            DeleteResponse response = new DeleteResponse();

            partition.delete(session, request, response);

            int rc = response.getReturnCode();
            log.debug("RC: "+rc);

            return response;

        } finally {
            session.close();
        }
    }

    public DeleteResponse delete(
            DeleteRequest request,
            DeleteResponse response
    ) throws Exception {

        SessionManager sessionManager = partition.getPartitionContext().getSessionManager();
        Session session = sessionManager.newAdminSession();

        try {
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

        SessionManager sessionManager = partition.getPartitionContext().getSessionManager();
        Session session = sessionManager.newAdminSession();

        try {
            return partition.find(session, new DN(dn));

        } finally {
            session.close();
        }
    }

    public SearchResult find(
            RDN rdn
    ) throws Exception {

        SessionManager sessionManager = partition.getPartitionContext().getSessionManager();
        Session session = sessionManager.newAdminSession();

        try {
            return partition.find(session, new DN(rdn));

        } finally {
            session.close();
        }
    }

    public SearchResult find(
            DN dn
    ) throws Exception {

        SessionManager sessionManager = partition.getPartitionContext().getSessionManager();
        Session session = sessionManager.newAdminSession();

        try {
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

        SessionManager sessionManager = partition.getPartitionContext().getSessionManager();
        Session session = sessionManager.newAdminSession();

        try {
            ModifyRequest request = new ModifyRequest();
            request.setDn(dn);
            request.setModifications(modifications);

            ModifyResponse response = new ModifyResponse();

            partition.modify(session, request, response);

            int rc = response.getReturnCode();
            log.debug("RC: "+rc);

            return response;

        } finally {
            session.close();
        }
    }

    public ModifyResponse modify(
            RDN rdn,
            Collection<Modification> modifications
    ) throws Exception {

        SessionManager sessionManager = partition.getPartitionContext().getSessionManager();
        Session session = sessionManager.newAdminSession();

        try {
            ModifyRequest request = new ModifyRequest();
            request.setDn(rdn);
            request.setModifications(modifications);

            ModifyResponse response = new ModifyResponse();

            partition.modify(session, request, response);

            int rc = response.getReturnCode();
            log.debug("RC: "+rc);

            return response;

        } finally {
            session.close();
        }
    }

    public ModifyResponse modify(
            DN dn,
            Collection<Modification> modifications
    ) throws Exception {

        SessionManager sessionManager = partition.getPartitionContext().getSessionManager();
        Session session = sessionManager.newAdminSession();

        try {
            ModifyRequest request = new ModifyRequest();
            request.setDn(dn);
            request.setModifications(modifications);

            ModifyResponse response = new ModifyResponse();

            partition.modify(session, request, response);

            int rc = response.getReturnCode();
            log.debug("RC: "+rc);

            return response;

        } finally {
            session.close();
        }
    }

    public ModifyResponse modify(
            ModifyRequest request,
            ModifyResponse response
    ) throws Exception {

        SessionManager sessionManager = partition.getPartitionContext().getSessionManager();
        Session session = sessionManager.newAdminSession();

        try {
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

        SessionManager sessionManager = partition.getPartitionContext().getSessionManager();
        Session session = sessionManager.newAdminSession();

        try {
            ModRdnRequest request = new ModRdnRequest();
            request.setDn(dn);
            request.setNewRdn(newRdn);
            request.setDeleteOldRdn(deleteOldRdn);

            ModRdnResponse response = new ModRdnResponse();

            partition.modrdn(session, request, response);

            int rc = response.getReturnCode();
            log.debug("RC: "+rc);

            return response;

        } finally {
            session.close();
        }
    }

    public ModRdnResponse modrdn(
            RDN dn,
            RDN newRdn,
            boolean deleteOldRdn
    ) throws Exception {

        SessionManager sessionManager = partition.getPartitionContext().getSessionManager();
        Session session = sessionManager.newAdminSession();

        try {
            ModRdnRequest request = new ModRdnRequest();
            request.setDn(dn);
            request.setNewRdn(newRdn);
            request.setDeleteOldRdn(deleteOldRdn);

            ModRdnResponse response = new ModRdnResponse();

            partition.modrdn(session, request, response);

            int rc = response.getReturnCode();
            log.debug("RC: "+rc);

            return response;

        } finally {
            session.close();
        }
    }

    public ModRdnResponse modrdn(
            DN dn,
            RDN newRdn,
            boolean deleteOldRdn
    ) throws Exception {

        SessionManager sessionManager = partition.getPartitionContext().getSessionManager();
        Session session = sessionManager.newAdminSession();

        try {
            ModRdnRequest request = new ModRdnRequest();
            request.setDn(dn);
            request.setNewRdn(newRdn);
            request.setDeleteOldRdn(deleteOldRdn);

            ModRdnResponse response = new ModRdnResponse();

            partition.modrdn(session, request, response);

            int rc = response.getReturnCode();
            log.debug("RC: "+rc);

            return response;

        } finally {
            session.close();
        }
    }

    public ModRdnResponse modrdn(
            ModRdnRequest request,
            ModRdnResponse response
    ) throws Exception {

        SessionManager sessionManager = partition.getPartitionContext().getSessionManager();
        Session session = sessionManager.newAdminSession();

        try {
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

        SessionManager sessionManager = partition.getPartitionContext().getSessionManager();
        Session session = sessionManager.newAdminSession();

        try {
            SearchRequest request = new SearchRequest();
            request.setDn(dn);
            request.setFilter(filter);
            request.setScope(scope);

            SearchResponse response = new SearchResponse();

            partition.search(session, request, response);

            int rc = response.getReturnCode();
            log.debug("RC: "+rc);

            return response;

        } finally {
            session.close();
        }
    }

    public SearchResponse search(
            RDN rdn,
            Filter filter,
            Integer scope
    ) throws Exception {

        SessionManager sessionManager = partition.getPartitionContext().getSessionManager();
        Session session = sessionManager.newAdminSession();

        try {
            SearchRequest request = new SearchRequest();
            request.setDn(rdn);
            request.setFilter(filter);
            request.setScope(scope);

            SearchResponse response = new SearchResponse();

            partition.search(session, request, response);

            int rc = response.getReturnCode();
            log.debug("RC: "+rc);

            return response;

        } finally {
            session.close();
        }
    }

    public SearchResponse search(
            DN dn,
            Filter filter,
            Integer scope
    ) throws Exception {

        SessionManager sessionManager = partition.getPartitionContext().getSessionManager();
        Session session = sessionManager.newAdminSession();

        try {
            SearchRequest request = new SearchRequest();
            request.setDn(dn);
            request.setFilter(filter);
            request.setScope(scope);

            SearchResponse response = new SearchResponse();

            partition.search(session, request, response);

            int rc = response.getReturnCode();
            log.debug("RC: "+rc);

            return response;

        } finally {
            session.close();
        }
    }

    public SearchResponse search(
            SearchRequest request,
            SearchResponse response
    ) throws Exception {

        SessionManager sessionManager = partition.getPartitionContext().getSessionManager();
        Session session = sessionManager.newAdminSession();

        try {
            partition.search(session, request, response);

            int rc = response.getReturnCode();
            log.debug("RC: "+rc);

            return response;

        } finally {
            session.close();
        }
    }
}
