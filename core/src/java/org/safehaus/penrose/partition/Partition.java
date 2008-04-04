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
import java.io.File;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import org.safehaus.penrose.connection.Connection;
import org.safehaus.penrose.connection.ConnectionManager;
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

/**
 * @author Endi S. Dewata
 */
public class Partition implements Cloneable {

    public Logger log = LoggerFactory.getLogger(getClass());

    public final static Collection<String> EMPTY_STRINGS       = new ArrayList<String>();
    public final static Collection<Source> EMPTY_SOURCES       = new ArrayList<Source>();
    public final static Collection<SourceRef> EMPTY_SOURCEREFS = new ArrayList<SourceRef>();

    protected PartitionConfig partitionConfig;
    protected PartitionContext partitionContext;

    protected Map<String, Engine>    engines           = new LinkedHashMap<String,Engine>();
    protected Adapters               adapters          = new Adapters();
    protected ConnectionManager      connectionManager;
    protected SourceManager          sourceManager;
    protected Directory              directory;
    protected Map<String,Module>     modules           = new LinkedHashMap<String,Module>();

    Scheduler scheduler;

    Engine engine;
    SchemaManager schemaManager;
    ThreadManager threadManager;
    ACLEvaluator aclEvaluator;

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

        connectionManager = new ConnectionManager(this);
        connectionManager.init();

        sourceManager = new SourceManager(this);
        for (SourceConfig sourceConfig : partitionConfig.getSourceConfigManager().getSourceConfigs()) {
            if (!sourceConfig.isEnabled()) continue;

            sourceManager.createSource(sourceConfig);
        }

        DirectoryConfig directoryConfig = partitionConfig.getDirectoryConfig();
        DirectoryContext directoryContext = new DirectoryContext();
        directoryContext.setPartition(this);

        directory = new Directory();
        directory.init(directoryConfig, directoryContext);

        for (ModuleConfig moduleConfig : partitionConfig.getModuleConfigManager().getModuleConfigs()) {
            if (!moduleConfig.isEnabled()) continue;

            createModule(moduleConfig);
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

        sourceManager.destroy();
        connectionManager.destroy();

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

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Connections
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public ConnectionManager getConnectionManager() {
        return connectionManager;
    }

    public Connection getConnection(String connectionName) {
        return connectionManager.getConnection(connectionName);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Sources
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public SourceManager getSourceManager() {
        return sourceManager;
    }

    public SourceConfigManager getSourceConfigManager() {
        return sourceManager.getSourceConfigManager();
    }

    public Source getSource(String sourceName) {
        return sourceManager.getSource(sourceName);
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

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Modules
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public Module createModule(ModuleConfig moduleConfig) throws Exception {

        String className = moduleConfig.getModuleClass();

        ClassLoader cl = partitionContext.getClassLoader();
        Class clazz = cl.loadClass(className);
        Module module = (Module)clazz.newInstance();

        ModuleContext moduleContext = new ModuleContext();
        moduleContext.setPartition(this);

        module.init(moduleConfig, moduleContext);

        addModule(module);

        return module;
    }


    public void addModule(Module module) {
        modules.put(module.getName(), module);
    }

    public Module removeModule(String name) {
        return modules.remove(name);
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

        for (ModuleMapping moduleMapping : partitionConfig.getModuleConfigManager().getModuleMappings()) {
            String moduleName = moduleMapping.getModuleName();

            boolean b = moduleMapping.match(dn);
            if (debug) log.debug(" - "+moduleName+": "+b);

            if (!b) continue;

            Module module = getModule(moduleName);
            list.add(module);
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
            int rc = aclEvaluator.checkAdd(session, parent, parentDn);

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

            int rc = aclEvaluator.checkRead(session, entry, dn);

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

            int rc = aclEvaluator.checkDelete(session, entry, dn);

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

    public Collection<Entry> findEntries(DN dn) throws Exception {
        return directory.findEntries(dn);
    }
    
    public SearchResult find(Session session, DN dn) throws Exception {

        boolean debug = log.isDebugEnabled();

        if (debug) log.debug("Finding "+dn);

        SearchRequest request = new SearchRequest();
        request.setDn(dn);
        request.setScope(SearchRequest.SCOPE_BASE);

        SearchResponse response = new SearchResponse();

        search(session, request, response);

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

            int rc = aclEvaluator.checkModify(session, entry, dn);

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

            int rc = aclEvaluator.checkModify(session, entry, dn);

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

            int rc = aclEvaluator.checkSearch(session, entry, baseDn);

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
            DN bindDn,
            DN entryDn,
            Entry entry,
            Attributes attributes
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        Collection<String> attributeNames = attributes.getNormalizedNames();

        Set<String> grants = new HashSet<String>();
        Set<String> denies = new HashSet<String>();
        denies.addAll(attributeNames);

        aclEvaluator.getReadableAttributes(bindDn, entry, entryDn, null, attributeNames, grants, denies);

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
