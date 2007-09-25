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
import org.safehaus.penrose.adapter.Adapter;
import org.safehaus.penrose.directory.*;
import org.safehaus.penrose.handler.Handler;
import org.safehaus.penrose.handler.HandlerConfig;
import org.safehaus.penrose.handler.HandlerSearchResponse;
import org.safehaus.penrose.engine.Engine;
import org.safehaus.penrose.engine.EngineConfig;
import org.safehaus.penrose.scheduler.Scheduler;
import org.safehaus.penrose.scheduler.SchedulerConfig;
import org.safehaus.penrose.scheduler.SchedulerContext;
import org.safehaus.penrose.schema.SchemaManager;
import org.safehaus.penrose.schema.AttributeType;
import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.naming.PenroseContext;
import org.safehaus.penrose.acl.ACLEvaluator;
import org.safehaus.penrose.thread.ThreadManager;
import org.safehaus.penrose.config.PenroseConfig;

/**
 * @author Endi S. Dewata
 */
public class Partition implements PartitionMBean, Cloneable {

    public Logger log = LoggerFactory.getLogger(getClass());
    public boolean debug = log.isDebugEnabled();

    public final static Collection<String> EMPTY_STRINGS       = new ArrayList<String>();
    public final static Collection<Source> EMPTY_SOURCES       = new ArrayList<Source>();
    public final static Collection<SourceRef> EMPTY_SOURCEREFS = new ArrayList<SourceRef>();

    protected PartitionConfig partitionConfig;
    protected PartitionContext partitionContext;

    protected Map<String, Handler> handlers = new LinkedHashMap<String,Handler>();
    protected Map<String, Engine>  engines  = new LinkedHashMap<String,Engine>();

    protected Connections            connections = new Connections();
    protected Map<String,Source>     sources     = new LinkedHashMap<String,Source>();
    protected Map<String,SourceSync> sourceSyncs = new LinkedHashMap<String,SourceSync>();
    protected Directory              directory   = new Directory();
    protected Map<String,Module>     modules     = new LinkedHashMap<String,Module>();

    private Scheduler scheduler;

    SchemaManager schemaManager;
    ThreadManager threadManager;
    protected ACLEvaluator aclEvaluator;

    public Partition() {
    }

    public void init(PartitionConfig partitionConfig, PartitionContext partitionContext) throws Exception {

        log.debug("Initializing "+partitionConfig.getName()+" partition.");

        this.partitionConfig = partitionConfig;
        this.partitionContext = partitionContext;

        PenroseContext penroseContext = partitionContext.getPenroseContext();
        schemaManager = penroseContext.getSchemaManager();
        threadManager = penroseContext.getThreadManager();

        aclEvaluator = new ACLEvaluator();

        for (HandlerConfig handlerConfig : partitionConfig.getHandlerConfigs()) {
            Handler handler = createHandler(handlerConfig);
            addHandler(handler);
        }

        for (EngineConfig engineConfig : partitionConfig.getEngineConfigs()) {
            Engine engine = createEngine(engineConfig);
            addEngine(engine);
        }

        connections.init(this);

        for (SourceConfig sourceConfig : partitionConfig.getSourceConfigs().getSourceConfigs()) {
            if (!sourceConfig.isEnabled()) continue;

            Source source = createSource(sourceConfig);
            addSource(source);
        }

        for (SourceSyncConfig sourceSyncConfig : partitionConfig.getSourceConfigs().getSourceSyncConfigs()) {
            if (!sourceSyncConfig.isEnabled()) continue;

            SourceSync sourceSync = createSourceSync(sourceSyncConfig);
            addSourceSync(sourceSync);
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

        log.debug("Partition "+partitionConfig.getName()+" started.");
    }

    public void destroy() throws Exception {
        log.debug("Stopping "+partitionConfig.getName()+" partition.");

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

        log.debug("Partition "+partitionConfig.getName()+" stopped.");
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

    public Handler createHandler(HandlerConfig handlerConfig) throws Exception {

        String handlerName = handlerConfig.getName();
        if (handlerName == null) throw new Exception("Missing handler name.");

        String className = handlerConfig.getHandlerClass();

        ClassLoader cl = partitionContext.getClassLoader();
        Class clazz = cl.loadClass(className);
        Handler handler = (Handler)clazz.newInstance();

        handler.setPartition(this);
        handler.setPenroseContext(partitionContext.getPenroseContext());
        handler.init(handlerConfig);

        return handler;
    }

    public void addHandler(Handler handler) {
        handlers.put(handler.getName(), handler);
    }

    public Handler getHandler(String name) {
        return handlers.get(name);
    }

    public Handler getHandler(Partition partition, Entry entry) {
        String handlerName = entry.getHandlerName();
        if (handlerName != null) return handlers.get(handlerName);

        return handlers.get("DEFAULT");
    }

    public Engine createEngine(EngineConfig engineConfig) throws Exception {

        String engineName = engineConfig.getName();
        if (engineName == null) throw new Exception("Missing engine name.");

        String className = engineConfig.getEngineClass();

        ClassLoader cl = partitionContext.getClassLoader();
        Class clazz = cl.loadClass(className);
        Engine engine = (Engine)clazz.newInstance();

        engine.setPartition(this);
        engine.setPenroseContext(partitionContext.getPenroseContext());
        engine.init(engineConfig);

        return engine;
    }

    public void addEngine(Engine engine) {
        engines.put(engine.getName(), engine);
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

        return createSource(sourceConfig, connection);
    }

    public Source createSource(
            SourceConfig sourceConfig,
            Connection connection
    ) throws Exception {

        SourceContext sourceContext = new SourceContext();
        sourceContext.setPartition(this);
        sourceContext.setConnection(connection);

        Source source = new Source();
        source.init(sourceConfig, sourceContext);

        return source;
    }

    public void addSource(Source source) {
        sources.put(source.getName(), source);
    }

    public Collection<Source> getSources() {
        return sources.values();
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

    public SourceSync createSourceSync(SourceSyncConfig sourceSyncConfig) throws Exception {

        log.debug("Initializing source sync "+sourceSyncConfig.getName()+".");

        SourceConfig sourceConfig = sourceSyncConfig.getSourceConfig();
        String changeLogName = sourceSyncConfig.getParameter(SourceSync.CHANGELOG);

        Connection connection = getConnection(sourceConfig.getConnectionName());
        Adapter adapter = connection.getAdapter();
        String className = adapter.getSyncClassName();
        
        ClassLoader cl = partitionContext.getClassLoader();
        Class clazz = cl.loadClass(className);

        SourceSync sourceSync = (SourceSync)clazz.newInstance();

        SourceSyncContext sourceSyncContext = new SourceSyncContext();
        sourceSyncContext.setPartition(this);
        
        sourceSync.init(sourceSyncConfig, sourceSyncContext);

        return sourceSync;
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

    public void add(
            Session session,
            AddRequest request,
            AddResponse response
    ) throws Exception {

        PenroseContext penroseContext = partitionContext.getPenroseContext();
        SchemaManager schemaManager = penroseContext.getSchemaManager();

        DN dn = schemaManager.normalize(request.getDn());
        request.setDn(dn);

        DN parentDn = dn.getParentDn();

        Attributes attributes = schemaManager.normalize(request.getAttributes());
        request.setAttributes(attributes);

        Collection<Entry> entries = directory.findEntries(dn);

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
            Session session,
            BindRequest request, 
            BindResponse response
    ) throws Exception {

        PenroseContext penroseContext = partitionContext.getPenroseContext();
        SchemaManager schemaManager = penroseContext.getSchemaManager();

        DN dn = schemaManager.normalize(request.getDn());
        request.setDn(dn);

        Collection<Entry> entries = directory.findEntries(dn);

        for (Entry entry : entries) {
            if (debug) log.debug("Binding " + dn + " in " + entry.getDn());

            entry.bind(session, request, response);
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Compare
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void compare(
            Session session,
            CompareRequest request,
            CompareResponse response
    ) throws Exception {

        PenroseContext penroseContext = partitionContext.getPenroseContext();
        SchemaManager schemaManager = penroseContext.getSchemaManager();

        DN dn = schemaManager.normalize(request.getDn());
        request.setDn(dn);

        String attributeName = schemaManager.normalizeAttributeName(request.getAttributeName());
        request.setAttributeName(attributeName);

        Collection<Entry> entries = directory.findEntries(dn);

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

    public void delete(
            Session session,
            DeleteRequest request,
            DeleteResponse response
    ) throws Exception {

        PenroseContext penroseContext = partitionContext.getPenroseContext();
        SchemaManager schemaManager = penroseContext.getSchemaManager();

        DN dn = schemaManager.normalize(request.getDn());
        request.setDn(dn);

        Collection<Entry> entries = directory.findEntries(dn);

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
    // Modify
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void modify(
            Session session,
            ModifyRequest request,
            ModifyResponse response
    ) throws Exception {

        PenroseContext penroseContext = partitionContext.getPenroseContext();
        SchemaManager schemaManager = penroseContext.getSchemaManager();

        DN dn = schemaManager.normalize(request.getDn());
        request.setDn(dn);

        Collection<Modification> modifications = schemaManager.normalizeModifications(request.getModifications());
        request.setModifications(modifications);

        Collection<Entry> entries = directory.findEntries(dn);

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

    public void modrdn(
            Session session,
            ModRdnRequest request,
            ModRdnResponse response
    ) throws Exception {

        PenroseContext penroseContext = partitionContext.getPenroseContext();
        SchemaManager schemaManager = penroseContext.getSchemaManager();

        DN dn = schemaManager.normalize(request.getDn());
        request.setDn(dn);

        RDN newRdn = schemaManager.normalize(request.getNewRdn());
        request.setNewRdn(newRdn);

        Collection<Entry> entries = directory.findEntries(dn);

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

    public void search(
            final Session session,
            final SearchRequest request,
            final SearchResponse response
    ) throws Exception {

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
            throw LDAP.createException(LDAP.NO_SUCH_OBJECT);
        }

        final HandlerSearchResponse sr = new HandlerSearchResponse(
                response,
                session,
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

        DN bindDn = request.getDn();

        Collection<Entry> entries = directory.findEntries(bindDn);

        for (Entry entry : entries) {
            if (debug) log.debug("Unbinding " + bindDn + " from " + entry.getDn());

            entry.unbind(session, request, response);
        }
    }

    public Collection<String> filterAttributes(
            Session session,
            SearchResult searchResult,
            Collection<String> requestedAttributeNames,
            boolean allRegularAttributes,
            boolean allOpAttributes
    ) throws Exception {

        Collection<String> list = new HashSet<String>();

        if (session == null) return list;

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

        if (session == null) return;

        Collection<String> attributeNames = new ArrayList<String>();
        for (String attributeName : attributes.getNames()) {
            attributeNames.add(attributeName.toLowerCase());
        }

        Set<String> grants = new HashSet<String>();
        Set<String> denies = new HashSet<String>();
        denies.addAll(attributeNames);

        DN bindDn = session.getBindDn();
        aclEvaluator.getReadableAttributes(bindDn, this, entry, dn, null, attributeNames, grants, denies);

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
}
