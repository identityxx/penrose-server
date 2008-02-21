/**
 * Copyright (c) 2000-2006, Identyx Corporation.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */
package org.safehaus.penrose.partition;

import java.util.*;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import org.safehaus.penrose.connection.Connection;
import org.safehaus.penrose.connection.Connections;
import org.safehaus.penrose.source.*;
import org.safehaus.penrose.module.Module;
import org.safehaus.penrose.module.ModuleMapping;
import org.safehaus.penrose.module.ModuleConfig;
import org.safehaus.penrose.module.ModuleContext;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.adapter.Adapters;
import org.safehaus.penrose.directory.*;
import org.safehaus.penrose.engine.Engine;
import org.safehaus.penrose.engine.EngineConfig;
import org.safehaus.penrose.engine.basic.BasicEngine;
import org.safehaus.penrose.scheduler.Scheduler;
import org.safehaus.penrose.scheduler.SchedulerConfig;
import org.safehaus.penrose.scheduler.SchedulerContext;
import org.safehaus.penrose.schema.SchemaManager;
import org.safehaus.penrose.schema.AttributeType;
import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.naming.PenroseContext;
import org.safehaus.penrose.acl.ACLEvaluator;
import org.safehaus.penrose.thread.ThreadManager;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.interpreter.DefaultInterpreter;
import org.safehaus.penrose.filter.FilterTool;
import org.safehaus.penrose.filter.Filter;

/**
 * @author Endi S. Dewata
 */
public class Partition implements PartitionMBean, Cloneable {

    public Logger log = LoggerFactory.getLogger(getClass());

    public final static Collection<String> EMPTY_STRINGS       = new ArrayList<String>();
    public final static Collection<Source> EMPTY_SOURCES       = new ArrayList<Source>();
    public final static Collection<SourceRef> EMPTY_SOURCEREFS = new ArrayList<SourceRef>();

    protected PartitionConfig partitionConfig;
    protected PartitionContext partitionContext;

    protected Map<String, Engine>    engines     = new LinkedHashMap<String,Engine>();
    protected Adapters               adapters    = new Adapters();
    protected Connections            connections = new Connections();
    protected Map<String,Source>     sources     = new LinkedHashMap<String,Source>();
    protected Map<String,SourceSync> sourceSyncs = new LinkedHashMap<String,SourceSync>();
    protected Directory              directory   = new Directory();
    protected Map<String,Module>     modules     = new LinkedHashMap<String,Module>();

    protected Scheduler scheduler;

    protected Engine engine;
    SchemaManager schemaManager;
    ThreadManager threadManager;
    protected ACLEvaluator aclEvaluator;

    public Partition() {
    }

    public void init(PartitionConfig partitionConfig, PartitionContext partitionContext) throws Exception {

        //log.debug("Initializing "+partitionConfig.getName()+" partition.");

        this.partitionConfig = partitionConfig;
        this.partitionContext = partitionContext;

        PenroseContext penroseContext = partitionContext.getPenroseContext();
        schemaManager = penroseContext.getSchemaManager();
        threadManager = penroseContext.getThreadManager();

        aclEvaluator = new ACLEvaluator();

        EngineConfig engineConfig = new EngineConfig();
        engineConfig.setName("DEFAULT");

        engine = new BasicEngine();
        engine.setPartition(this);
        engine.setPenroseContext(partitionContext.getPenroseContext());
        engine.init(engineConfig);

        adapters.init(this);
        connections.init(this);

        for (SourceConfig sourceConfig : partitionConfig.getSourceConfigs().getSourceConfigs()) {
            if (!sourceConfig.isEnabled()) continue;

            Source source = createSource(sourceConfig);
            addSource(source);
        }

        DirectoryConfig directoryConfig = partitionConfig.getDirectoryConfig();
        DirectoryContext directoryContext = new DirectoryContext();
        directoryContext.setPartition(this);

        directory.init(directoryConfig, directoryContext);

        for (ModuleConfig moduleConfig : partitionConfig.getModuleConfigs().getModuleConfigs()) {
            if (!moduleConfig.isEnabled()) continue;

            Module module = createModule(moduleConfig);
            addModule(module);
        }

        scheduler = createScheduler(partitionConfig.getSchedulerConfig());

        //log.debug("Partition "+partitionConfig.getName()+" started.");
    }

    public void destroy() throws Exception {
        //log.debug("Stopping "+partitionConfig.getName()+" partition.");

        if (scheduler != null) {
            scheduler.destroy();
        }

        for (Module module : modules.values()) {
            module.destroy();
        }

        for (SourceSync sourceSync : sourceSyncs.values()) {
            sourceSync.destroy();
        }

        for (Source source : sources.values()) {
            source.destroy();
        }

        connections.destroy();

        //log.debug("Partition "+partitionConfig.getName()+" stopped.");
    }

    public String getName() {
        return partitionConfig.getName();
    }

    public void setName(String name) {
        partitionConfig.setName(name);
    }

    public String getDescription() {
        return partitionConfig.getDescription();
    }

    public void setDescription(String description) {
        partitionConfig.setDescription(description);
    }

    public Map<String,String> getParameters() {
        return partitionConfig.getParameters();
    }

    public Collection<String> getParameterNames() {
        return partitionConfig.getParameterNames();
    }

    public String getParameter(String name) {
        return partitionConfig.getParameter(name);
    }

    public boolean isEnabled() {
        return partitionConfig.isEnabled();
    }

    public PartitionConfig getPartitionConfig() {
        return partitionConfig;
    }

    public void setPartitionConfig(PartitionConfig partitionConfig) {
        this.partitionConfig = partitionConfig;
    }

    public String toString() {
        return partitionConfig.getName();
    }

    public Engine getEngine(String name) {
        return engines.get(name);
    }

    public Connections getConnections() {
        return connections;
    }

    public Connection getConnection(String name) {
        return connections.getConnection(name);
    }

    public Source createSource(
            SourceConfig sourceConfig
    ) throws Exception {
        Connection connection = connections.getConnection(sourceConfig.getConnectionName());
        if (connection == null) throw new Exception("Unknown connection "+sourceConfig.getConnectionName()+".");

        return connection.createSource(sourceConfig);
    }

    public void addSource(Source source) {
        sources.put(source.getName(), source);
    }

    public Collection<Source> getSources() {
        return sources.values();
    }

    public Source getSource() {
        if (sources.isEmpty()) return null;
        return sources.values().iterator().next();
    }

    public Source getSource(String name) {
        return sources.get(name);
    }

    public void addSourceSync(SourceSync sourceSync) {
        sourceSyncs.put(sourceSync.getName(), sourceSync);
    }

    public Collection<SourceSync> getSourceSyncs() {
        return sourceSyncs.values();
    }

    public SourceSync getSourceSync(String name) {
        return sourceSyncs.get(name);
    }

    public Module createModule(ModuleConfig moduleConfig) throws Exception {

        String className = moduleConfig.getModuleClass();

        ClassLoader cl = partitionContext.getClassLoader();
        Class clazz = cl.loadClass(className);
        Module module = (Module)clazz.newInstance();

        ModuleContext moduleContext = new ModuleContext();
        moduleContext.setPartition(this);

        module.init(moduleConfig, moduleContext);

        return module;
    }

    public Scheduler createScheduler(SchedulerConfig schedulerConfig) throws Exception {

        if (schedulerConfig == null) return null;
        if (!schedulerConfig.isEnabled()) return null;

        PenroseContext penroseContext = partitionContext.getPenroseContext();
        if (!penroseContext.isRunning()) return null;

        String className = schedulerConfig.getSchedulerClass();

        ClassLoader cl = partitionContext.getClassLoader();
        Class clazz = cl.loadClass(className);
        Scheduler scheduler = (Scheduler)clazz.newInstance();

        SchedulerContext schedulerContext = new SchedulerContext();
        schedulerContext.setPartition(this);

        scheduler.init(schedulerConfig, schedulerContext);

        return scheduler;
    }

    public void addModule(Module module) {
        modules.put(module.getName(), module);
    }

    public Collection<Module> getModules() {
        return modules.values();
    }

    public Module getModule(String name) {
        return modules.get(name);
    }

    public Collection<Module> getModules(DN dn) throws Exception {

        boolean debug = log.isDebugEnabled();

        if (debug) log.debug("Modules:");

        Collection<Module> list = new ArrayList<Module>();

        for (Collection<ModuleMapping> moduleMappings : partitionConfig.getModuleConfigs().getModuleMappings()) {

            for (ModuleMapping moduleMapping : moduleMappings) {
                String moduleName = moduleMapping.getModuleName();

                boolean b = moduleMapping.match(dn);
                if (debug) log.debug(" - "+moduleName+": "+b);

                if (!b) continue;

                Module module = getModule(moduleName);
                list.add(module);
            }
        }

        return list;
    }

    public PartitionContext getPartitionContext() {
        return partitionContext;
    }

    public void setPartitionContext(PartitionContext partitionContext) {
        this.partitionContext = partitionContext;
    }

    public Scheduler getScheduler() {
        return scheduler;
    }

    public void setScheduler(Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    public Directory getDirectory() {
        return directory;
    }

    public void setDirectory(Directory directory) {
        this.directory = directory;
    }

    public Object clone() throws CloneNotSupportedException {
        Partition partition = (Partition)super.clone();

        partition.partitionConfig = (PartitionConfig)partitionConfig.clone();
        partition.partitionContext = partitionContext;

        return partition;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Add
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public AddResponse add(
            String dn,
            Attributes attributes
    ) throws Exception {
        return add(new DN(dn), attributes);
    }

    public AddResponse add(
            RDN rdn,
            Attributes attributes
    ) throws Exception {
        return add(new DN(rdn), attributes);
    }

    public AddResponse add(
            DN dn,
            Attributes attributes
    ) throws Exception {

        AddRequest request = new AddRequest();
        request.setDn(dn);
        request.setAttributes(attributes);

        AddResponse response = new AddResponse();

        add(request, response);

        return response;
    }

    public void add(
            AddRequest request,
            AddResponse response
    ) throws Exception {
        add(null, request, response);
    }

    public void add(
            Session session,
            AddRequest request,
            AddResponse response
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        PenroseContext penroseContext = partitionContext.getPenroseContext();
        SchemaManager schemaManager = penroseContext.getSchemaManager();

        DN dn = schemaManager.normalize(request.getDn());
        request.setDn(dn);

        DN parentDn = dn.getParentDn();

        Attributes attributes = schemaManager.normalize(request.getAttributes());
        request.setAttributes(attributes);

        Collection<Entry> entries = directory.findEntries(dn);

        if (entries.isEmpty()) {
            if (debug) log.debug("Entry "+dn+" not found.");
            throw LDAP.createException(LDAP.NO_SUCH_OBJECT);
        }

        Exception exception = null;

        for (Entry entry : entries) {
            if (debug) log.debug("Adding " + dn + " into " + entry.getDn());

            Entry parent = entry.getParent();
            int rc = aclEvaluator.checkAdd(session, this, parent, parentDn);

            if (rc != LDAP.SUCCESS) {
                if (debug) log.debug("Not allowed to add " + dn);
                exception = LDAP.createException(LDAP.INSUFFICIENT_ACCESS_RIGHTS);
                continue;
            }

            try {
                entry.add(session, request, response);
                return;
            } catch (Exception e) {
                exception = e;
            }
        }

        throw exception;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Bind
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void bind(
            BindRequest request,
            BindResponse response
    ) throws Exception {
        bind(null, request, response);
    }

    public void bind(
            Session session,
            BindRequest request, 
            BindResponse response
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        PenroseContext penroseContext = partitionContext.getPenroseContext();
        SchemaManager schemaManager = penroseContext.getSchemaManager();

        DN dn = schemaManager.normalize(request.getDn());
        request.setDn(dn);

        Collection<Entry> entries = directory.findEntries(dn);

        if (entries.isEmpty()) {
            if (debug) log.debug("Entry "+dn+" not found.");
            throw LDAP.createException(LDAP.NO_SUCH_OBJECT);
        }

        Exception exception = null;

        for (Entry entry : entries) {
            if (debug) log.debug("Binding " + dn + " in " + entry.getDn());

            try {
                entry.bind(session, request, response);
                return;

            } catch (Exception e) {
                exception = e;
            }
        }

        throw exception;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Compare
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void compare(
            CompareRequest request,
            CompareResponse response
    ) throws Exception {
        compare(null, request, response);
    }

    public void compare(
            Session session,
            CompareRequest request,
            CompareResponse response
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        PenroseContext penroseContext = partitionContext.getPenroseContext();
        SchemaManager schemaManager = penroseContext.getSchemaManager();

        DN dn = schemaManager.normalize(request.getDn());
        request.setDn(dn);

        String attributeName = schemaManager.normalizeAttributeName(request.getAttributeName());
        request.setAttributeName(attributeName);

        Collection<Entry> entries = directory.findEntries(dn);

        if (entries.isEmpty()) {
            if (debug) log.debug("Entry "+dn+" not found.");
            throw LDAP.createException(LDAP.NO_SUCH_OBJECT);
        }

        Exception exception = null;

        for (Entry entry : entries) {
            if (debug) log.debug("Comparing " + dn + " in " + entry.getDn());

            int rc = aclEvaluator.checkRead(session, this, entry, dn);

            if (rc != LDAP.SUCCESS) {
                if (debug) log.debug("Not allowed to compare " + dn);
                exception = LDAP.createException(LDAP.INSUFFICIENT_ACCESS_RIGHTS);
                continue;
            }

            try {
                entry.compare(session, request, response);
                return;

            } catch (Exception e) {
                exception = e;
            }
        }

        throw exception;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Delete
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public DeleteResponse delete(
            String dn
    ) throws Exception {
        return delete(new DN(dn));
    }

    public DeleteResponse delete(
            RDN rdn
    ) throws Exception {
        return delete(new DN(rdn));
    }

    public DeleteResponse delete(
            DN dn
    ) throws Exception {

        DeleteRequest request = new DeleteRequest();
        request.setDn(dn);

        DeleteResponse response = new DeleteResponse();

        delete(request, response);

        return response;
    }

    public void delete(
            DeleteRequest request,
            DeleteResponse response
    ) throws Exception {
        delete(null, request, response);
    }

    public void delete(
            Session session,
            DeleteRequest request,
            DeleteResponse response
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        PenroseContext penroseContext = partitionContext.getPenroseContext();
        SchemaManager schemaManager = penroseContext.getSchemaManager();

        DN dn = schemaManager.normalize(request.getDn());
        request.setDn(dn);

        Collection<Entry> entries = directory.findEntries(dn);

        if (entries.isEmpty()) {
            if (debug) log.debug("Entry "+dn+" not found.");
            throw LDAP.createException(LDAP.NO_SUCH_OBJECT);
        }

        Exception exception = null;

        for (Entry entry : entries) {
            if (debug) log.debug("Deleting " + dn + " from " + entry.getDn());

            int rc = aclEvaluator.checkDelete(session, this, entry, dn);

            if (rc != LDAP.SUCCESS) {
                if (debug) log.debug("Not allowed to delete " + dn);
                exception = LDAP.createException(LDAP.INSUFFICIENT_ACCESS_RIGHTS);
                continue;
            }

            try {
                entry.delete(session, request, response);
                return;

            } catch (Exception e) {
                exception = e;
            }
        }

        throw exception;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Find
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public SearchResult find(String dn) throws Exception {
        return find(new DN(dn));
    }

    public SearchResult find(RDN rdn) throws Exception {
        return find(new DN(rdn));
    }

    public SearchResult find(DN dn) throws Exception {
        return find(null, dn);
    }

    public SearchResult find(Session session, DN dn) throws Exception {

        boolean debug = log.isDebugEnabled();

        if (debug) log.debug("Finding "+dn);

        SearchResponse response = search(session, dn, null, SearchRequest.SCOPE_BASE);

        if (response.getReturnCode() != LDAP.SUCCESS) {
            if (debug) log.debug("Entry "+dn+" not found: "+response.getErrorMessage());
            throw LDAP.createException(response.getReturnCode());
        }

        if (!response.hasNext()) {
            if (debug) log.debug("Entry "+dn+" not found.");
            throw LDAP.createException(LDAP.NO_SUCH_OBJECT);
        }

        SearchResult result = response.next();
        if (debug) log.debug("Found entry "+dn+".");
        
        return result;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Modify
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public ModifyResponse modify(
            String dn,
            Collection<Modification> modifications
    ) throws Exception {
        return modify(new DN(dn), modifications);
    }

    public ModifyResponse modify(
            RDN rdn,
            Collection<Modification> modifications
    ) throws Exception {
        return modify(new DN(rdn), modifications);
    }

    public ModifyResponse modify(
            DN dn,
            Collection<Modification> modifications
    ) throws Exception {

        ModifyRequest request = new ModifyRequest();
        request.setDn(dn);
        request.setModifications(modifications);

        ModifyResponse response = new ModifyResponse();

        modify(request, response);

        return response;
    }

    public void modify(
            ModifyRequest request,
            ModifyResponse response
    ) throws Exception {
        modify(null, request, response);
    }

    public void modify(
            Session session,
            ModifyRequest request,
            ModifyResponse response
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        PenroseContext penroseContext = partitionContext.getPenroseContext();
        SchemaManager schemaManager = penroseContext.getSchemaManager();

        DN dn = schemaManager.normalize(request.getDn());
        request.setDn(dn);

        Collection<Modification> modifications = schemaManager.normalizeModifications(request.getModifications());
        request.setModifications(modifications);

        Collection<Entry> entries = directory.findEntries(dn);

        if (entries.isEmpty()) {
            if (debug) log.debug("Entry "+dn+" not found.");
            throw LDAP.createException(LDAP.NO_SUCH_OBJECT);
        }

        Exception exception = null;

        for (Entry entry : entries) {
            if (debug) log.debug("Modifying " + dn + " in " + entry.getDn());

            int rc = aclEvaluator.checkModify(session, this, entry, dn);

            if (rc != LDAP.SUCCESS) {
                if (debug) log.debug("Not allowed to modify " + dn);
                exception = LDAP.createException(LDAP.INSUFFICIENT_ACCESS_RIGHTS);
                continue;
            }

            try {
                entry.modify(session, request, response);
                return;

            } catch (Exception e) {
                exception = e;
            }
        }

        throw exception;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // ModRdn
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public ModRdnResponse modrdn(
            String dn,
            String newRdn,
            boolean deleteOldRdn
    ) throws Exception {
        return modrdn(new DN(dn), new RDN(newRdn), deleteOldRdn);
    }

    public ModRdnResponse modrdn(
            RDN rdn,
            RDN newRdn,
            boolean deleteOldRdn
    ) throws Exception {
        return modrdn(new DN(rdn), newRdn, deleteOldRdn);
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

        modrdn(request, response);

        return response;
    }

    public void modrdn(
            ModRdnRequest request,
            ModRdnResponse response
    ) throws Exception {
        modrdn(null, request, response);
    }

    public void modrdn(
            Session session,
            ModRdnRequest request,
            ModRdnResponse response
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        PenroseContext penroseContext = partitionContext.getPenroseContext();
        SchemaManager schemaManager = penroseContext.getSchemaManager();

        DN dn = schemaManager.normalize(request.getDn());
        request.setDn(dn);

        RDN newRdn = schemaManager.normalize(request.getNewRdn());
        request.setNewRdn(newRdn);

        Collection<Entry> entries = directory.findEntries(dn);

        if (entries.isEmpty()) {
            if (debug) log.debug("Entry "+dn+" not found.");
            throw LDAP.createException(LDAP.NO_SUCH_OBJECT);
        }

        Exception exception = null;

        for (Entry entry : entries) {
            if (debug) log.debug("Renaming " + dn + " in " + entry.getDn());

            int rc = aclEvaluator.checkModify(session, this, entry, dn);

            if (rc != LDAP.SUCCESS) {
                if (debug) log.debug("Not allowed to modify " + dn);
                exception = LDAP.createException(LDAP.INSUFFICIENT_ACCESS_RIGHTS);
                continue;
            }

            try {
                entry.modrdn(session, request, response);
                return;

            } catch (Exception e) {
                exception = e;
            }
        }

        throw exception;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Search
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public SearchResponse search(
            String dn,
            String filter,
            int scope
    ) throws Exception {
        return search(new DN(dn), FilterTool.parseFilter(filter), scope);
    }

    public SearchResponse search(
            RDN rdn,
            Filter filter,
            int scope
    ) throws Exception {
        return search(new DN(rdn), filter, scope);
    }

    public SearchResponse search(
            DN dn,
            Filter filter,
            int scope
    ) throws Exception {
        return search(null, dn, filter, scope);
    }

    public SearchResponse search(
            Session session,
            DN dn,
            Filter filter,
            int scope
    ) throws Exception {

        SearchRequest request = new SearchRequest();
        request.setDn(dn);
        request.setFilter(filter);
        request.setScope(scope);

        SearchResponse response = new SearchResponse();

        search(session, request, response);

        return response;
    }

    public void search(
            final SearchRequest request,
            final SearchResponse response
    ) throws Exception {
        search(null, request, response);
    }

    public void search(
            final Session session,
            final SearchRequest request,
            final SearchResponse response
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        DN baseDn = schemaManager.normalize(request.getDn());
        request.setDn(baseDn);

        Collection<String> requestedAttributes = schemaManager.normalize(request.getAttributes());
        request.setAttributes(requestedAttributes);

        boolean allRegularAttributes = requestedAttributes.isEmpty() || requestedAttributes.contains("*");
        boolean allOpAttributes = requestedAttributes.contains("+");

        if (debug) {
            log.debug("Normalized base DN: "+baseDn);
            log.debug("Normalized attributes: "+requestedAttributes);
        }

        Collection<Entry> entries = directory.findEntries(baseDn);

        if (entries.isEmpty()) {
            if (debug) log.debug("Base DN "+baseDn+" not found.");
            response.close();
            throw LDAP.createException(LDAP.NO_SUCH_OBJECT);
        }

        final PartitionSearchResponse sr = new PartitionSearchResponse(
                session,
                request,
                response,
                this,
                requestedAttributes,
                allRegularAttributes,
                allOpAttributes,
                entries
        );

        for (final Entry entry : entries) {
            if (debug) log.debug("Searching " + baseDn + " in " + entry.getDn());

            int rc = aclEvaluator.checkSearch(session, this, entry, baseDn);

            if (rc != LDAP.SUCCESS) {
                if (debug) log.debug("Not allowed to search " + baseDn);
                sr.setResult(entry, LDAP.createException(LDAP.INSUFFICIENT_ACCESS_RIGHTS));
                sr.close();
                continue;
            }

            Runnable runnable = new Runnable() {
                public void run() {
                    try {
                        entry.search(
                                session,
                                request,
                                sr
                        );

                        sr.setResult(entry, LDAP.createException(LDAP.SUCCESS));

                    } catch (Exception e) {
                        log.error(e.getMessage(), e);
                        sr.setResult(entry, LDAP.createException(e));

                    } finally {
                        try {
                            sr.close();
                        } catch (Exception e) {
                            log.error(e.getMessage(), e);
                        }
                    }
                }
            };

            threadManager.execute(runnable);
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Unbind
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void unbind(
            Session session,
            UnbindRequest request,
            UnbindResponse response
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        DN bindDn = session.getBindDn();
        if (bindDn == null || bindDn.isEmpty()) return;

        Collection<Entry> entries = directory.findEntries(bindDn);

        if (entries.isEmpty()) {
            if (debug) log.debug("Entry "+bindDn+" not found.");
            throw LDAP.createException(LDAP.NO_SUCH_OBJECT);
        }

        Exception exception = null;

        for (Entry entry : entries) {
            if (debug) log.debug("Unbinding " + bindDn + " from " + entry.getDn());

            try {
                entry.unbind(session, request, response);
                return;

            } catch (Exception e) {
                exception = e;
            }
        }

        throw exception;
    }

    public Collection<String> filterAttributes(
            SearchResult searchResult,
            Collection<String> requestedAttributeNames,
            boolean allRegularAttributes,
            boolean allOpAttributes
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        Collection<String> list = new HashSet<String>();

        Attributes attributes = searchResult.getAttributes();
        Collection<String> attributeNames = attributes.getNames();

        if (debug) {
            log.debug("Attribute names: "+attributeNames);
        }

        if (allRegularAttributes && allOpAttributes) {
            log.debug("Returning all attributes.");
            return list;
        }

        if (allRegularAttributes) {

            // return regular attributes only
            for (String attributeName : attributes.getNames()) {

                AttributeType attributeType = schemaManager.getAttributeType(attributeName);
                if (attributeType == null) {
                    if (debug) log.debug("Attribute " + attributeName + " undefined.");
                    continue;
                }

                if (!attributeType.isOperational()) {
                    //log.debug("Keep regular attribute "+attributeName);
                    continue;
                }

                log.debug("Remove operational attribute " + attributeName);
                list.add(attributeName);
            }

        } else if (allOpAttributes) {

            // return operational attributes only
            for (String attributeName : attributes.getNames()) {

                AttributeType attributeType = schemaManager.getAttributeType(attributeName);
                if (attributeType == null) {
                    if (debug) log.debug("Attribute " + attributeName + " undefined.");
                    list.add(attributeName);
                    continue;
                }

                if (attributeType.isOperational()) {
                    //log.debug("Keep operational attribute "+attributeName);
                    continue;
                }

                log.debug("Remove regular attribute " + attributeName);
                list.add(attributeName);
            }

        } else {

            // return requested attributes
            for (String attributeName : attributes.getNames()) {

                if (requestedAttributeNames.contains(attributeName)) {
                    //log.debug("Keep requested attribute "+attributeName);
                    continue;
                }

                log.debug("Remove unrequested attribute " + attributeName);
                list.add(attributeName);
            }
        }

        return list;
    }

    public void filterAttributes(
            Session session,
            DN dn,
            Entry entry,
            Attributes attributes
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        if (session == null) return;

        Collection<String> attributeNames = attributes.getNormalizedNames();

        Set<String> grants = new HashSet<String>();
        Set<String> denies = new HashSet<String>();
        denies.addAll(attributeNames);

        DN bindDn = session.getBindDn();
        aclEvaluator.getReadableAttributes(bindDn, entry, dn, null, attributeNames, grants, denies);

        if (debug) {
            log.debug("Returned: "+attributeNames);
            log.debug("Granted: "+grants);
            log.debug("Denied: "+denies);
        }

        Collection<String> list = new ArrayList<String>();

        for (String attributeName : attributes.getNames()) {
            String normalizedName = attributeName.toLowerCase();

            if (!denies.contains(normalizedName)) {
                //log.debug("Keep undenied attribute "+normalizedName);
                continue;
            }

            //log.debug("Remove denied attribute "+normalizedName);
            list.add(attributeName);
        }

        removeAttributes(attributes, list);
    }

    public void removeAttributes(Attributes attributes, Collection<String> list) throws Exception {
        for (String attributeName : list) {
            attributes.remove(attributeName);
        }
    }

    public ACLEvaluator getAclEvaluator() {
        return aclEvaluator;
    }

    public void setAclEvaluator(ACLEvaluator aclEvaluator) {
        this.aclEvaluator = aclEvaluator;
    }

    public Engine getEngine() {
        return engine;
    }

    public void setEngine(Engine engine) {
        this.engine = engine;
    }

    public Interpreter newInterpreter() throws Exception {
        ClassLoader classLoader = partitionContext.getClassLoader();

        Interpreter interpreter = new DefaultInterpreter();
        interpreter.setClassLoader(classLoader);

        return interpreter;
    }

    public Adapters getAdapters() {
        return adapters;
    }

    public void setAdapters(Adapters adapters) {
        this.adapters = adapters;
    }
}
