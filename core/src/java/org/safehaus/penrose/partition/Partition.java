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

import org.safehaus.penrose.acl.ACLEvaluator;
import org.safehaus.penrose.adapter.AdapterManager;
import org.safehaus.penrose.connection.ConnectionManager;
import org.safehaus.penrose.directory.Directory;
import org.safehaus.penrose.directory.Entry;
import org.safehaus.penrose.interpreter.DefaultInterpreter;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.mapping.MappingManager;
import org.safehaus.penrose.module.Module;
import org.safehaus.penrose.module.ModuleChain;
import org.safehaus.penrose.module.ModuleManager;
import org.safehaus.penrose.naming.PenroseContext;
import org.safehaus.penrose.scheduler.Scheduler;
import org.safehaus.penrose.scheduler.SchedulerConfig;
import org.safehaus.penrose.scheduler.SchedulerContext;
import org.safehaus.penrose.schema.ObjectClass;
import org.safehaus.penrose.schema.SchemaManager;
import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.operation.SearchOperation;
import org.safehaus.penrose.operation.PipelineSearchOperation;
import org.safehaus.penrose.source.SourceManager;
import org.safehaus.penrose.thread.ThreadManager;
import org.safehaus.penrose.thread.ThreadManagerConfig;
import org.safehaus.penrose.Penrose;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ietf.ldap.LDAPException;

import java.lang.reflect.Constructor;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.io.File;

/**
 * @author Endi S. Dewata
 */
public class Partition implements Cloneable {

    public Logger log = LoggerFactory.getLogger(getClass());
    public boolean debug = log.isDebugEnabled();
    public boolean info = log.isInfoEnabled();

    public final static String     STARTING = "STARTING";
    public final static String     STARTED  = "STARTED";
    public final static String     STOPPING = "STOPPING";
    public final static String     STOPPED  = "STOPPED";

    public final static String     SCHEMA_CHECKING         = "schemaChecking";
    public final static boolean    DEFAULT_SCHEMA_CHECKING = false; // disabled

    protected PartitionConfig      partitionConfig;
    protected PartitionContext     partitionContext;

    protected AdapterManager       adapterManager;
    protected ConnectionManager    connectionManager;
    protected SourceManager        sourceManager;
    protected MappingManager       mappingManager;
    protected Directory            directory;
    protected ModuleManager        moduleManager;

    protected Scheduler            scheduler;
    protected ThreadManager        threadManager;

    protected SchemaManager        schemaManager;
    protected ACLEvaluator         aclEvaluator;

    protected boolean              schemaChecking;
    protected String               status = STOPPED;

    public Partition() {
    }

    public synchronized void init(PartitionConfig partitionConfig, PartitionContext partitionContext) throws Exception {

        //log.debug("Initializing "+partitionConfig.getName()+" partition.");
        if (STARTING.equals(status)) return;
        status = STARTING;

        this.partitionConfig = partitionConfig;
        this.partitionContext = partitionContext;

        PenroseContext penroseContext = partitionContext.getPenroseContext();
        schemaManager = penroseContext.getSchemaManager();

        threadManager = createThreadManager(partitionConfig.getThreadManagerConfig());

        aclEvaluator = new ACLEvaluator();
        aclEvaluator.init(this);

        adapterManager = new AdapterManager();
        adapterManager.init(this);

        connectionManager = new ConnectionManager(this);
        connectionManager.init();

        sourceManager = new SourceManager(this);
        sourceManager.init();

        mappingManager = new MappingManager(this);
        mappingManager.init();

        directory = new Directory(this);
        directory.init();

        moduleManager = new ModuleManager(this);
        moduleManager.init();
        
        scheduler = createScheduler(partitionConfig.getSchedulerConfig());

        String s = getParameter(SCHEMA_CHECKING);
        schemaChecking = s == null ? DEFAULT_SCHEMA_CHECKING : Boolean.valueOf(s);

        init();

        status = STARTED;
        //log.debug("Partition "+partitionConfig.getName()+" started.");
    }

    public void init() throws Exception {
    }

    public void destroy() throws Exception {
        //log.debug("Stopping "+partitionConfig.getName()+" partition.");
        if (STOPPING.equals(status)) return;
        status = STOPPING;

        if (scheduler != null) scheduler.destroy();
        if (threadManager != null) threadManager.destroy();

        moduleManager.destroy();
        directory.destroy();
        mappingManager.destroy();
        sourceManager.destroy();
        connectionManager.destroy();

        status = STOPPED;
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

    public String getStatus() {
        return status;
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

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Managers
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public ConnectionManager getConnectionManager() {
        return connectionManager;
    }

    public SourceManager getSourceManager() {
        return sourceManager;
    }

    public MappingManager getMappingManager() {
        return mappingManager;
    }

    public Directory getDirectory() {
        return directory;
    }

    public ModuleManager getModuleManager() {
        return moduleManager;
    }

    public Scheduler createScheduler(SchedulerConfig schedulerConfig) throws Exception {

        if (schedulerConfig == null) return null;
        if (!schedulerConfig.isEnabled()) return null;

        String className = schedulerConfig.getSchedulerClass();

        ClassLoader cl = partitionContext.getClassLoader();
        Class clazz = cl.loadClass(className);
        Scheduler scheduler = (Scheduler)clazz.newInstance();

        SchedulerContext schedulerContext = new SchedulerContext();
        schedulerContext.setPartition(this);

        scheduler.init(schedulerConfig, schedulerContext);

        return scheduler;
    }

    public ThreadManager createThreadManager(ThreadManagerConfig threadManagerConfig) throws Exception {

        if (threadManagerConfig == null) return null;
        if (!threadManagerConfig.isEnabled()) return null;

        Class clazz;

        String className = threadManagerConfig.getThreadManagerClass();
        if (className == null) {
            clazz = ThreadManager.class;

        } else {
            ClassLoader cl = partitionContext.getClassLoader();
            clazz = cl.loadClass(className);
        }

        Constructor constructor = clazz.getConstructor(String.class);

        ThreadManager threadManager = (ThreadManager)constructor.newInstance(Penrose.PRODUCT_NAME+"-"+partitionConfig.getName());
        threadManager.init(threadManagerConfig);

        return threadManager;
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

    public Object clone() throws CloneNotSupportedException {
        Partition partition = (Partition)super.clone();

        partition.partitionConfig = (PartitionConfig)partitionConfig.clone();
        partition.partitionContext = partitionContext;

        return partition;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Modules
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public ModuleChain createModuleChain(Entry entry, Collection<Module> modules) {

        ModuleChain lastChain = new ModuleChain(entry);

        Iterator<Module> i = modules.iterator();
        return createModuleChain(entry, i, lastChain);
    }

    public ModuleChain createModuleChain(Entry entry, Iterator<Module> i, ModuleChain lastChain) {
        if (!i.hasNext()) return lastChain;

        Module module = i.next();

        ModuleChain nextChain = createModuleChain(entry, i, lastChain);
        return new ModuleChain(entry, module, nextChain);
    }

    public Collection<Module> findModules(DN dn) throws Exception {
        return moduleManager.findModules(dn);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Normalize
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void normalize(AddRequest request) throws Exception {
        DN dn = schemaManager.normalize(request.getDn());
        request.setDn(dn);

        Attributes attributes = schemaManager.normalize(request.getAttributes());
        request.setAttributes(attributes);
    }

    public void normalize(BindRequest request) throws Exception {
        DN dn = schemaManager.normalize(request.getDn());
        request.setDn(dn);
    }

    public void normalize(CompareRequest request) throws Exception {
        DN dn = schemaManager.normalize(request.getDn());
        request.setDn(dn);

        String attributeName = schemaManager.normalizeAttributeName(request.getAttributeName());
        request.setAttributeName(attributeName);
    }

    public void normalize(DeleteRequest request) throws Exception {
        DN dn = schemaManager.normalize(request.getDn());
        request.setDn(dn);
    }

    public void normalize(ModifyRequest request) throws Exception {
        DN dn = schemaManager.normalize(request.getDn());
        request.setDn(dn);

        Collection<Modification> modifications = schemaManager.normalizeModifications(request.getModifications());
        request.setModifications(modifications);
    }

    public void normalize(ModRdnRequest request) throws Exception {
        DN dn = schemaManager.normalize(request.getDn());
        request.setDn(dn);

        RDN newRdn = schemaManager.normalize(request.getNewRdn());
        request.setNewRdn(newRdn);
    }

    public void normalize(SearchRequest request) throws Exception {
        DN baseDn = schemaManager.normalize(request.getDn());
        request.setDn(baseDn);

        Collection<String> requestedAttributes = schemaManager.normalize(request.getAttributes());
        request.setAttributes(requestedAttributes);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // ACL
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void validatePermission(Session session, AddRequest request, Entry entry) throws Exception {

        DN dn = request.getDn();
        DN parentDn = dn.getParentDn();
        Entry parent = entry.getParent();

        int rc = aclEvaluator.checkAdd(session, parent, parentDn);
        if (rc == LDAP.SUCCESS) return;

        if (debug) log.debug("Not allowed to add entry under \""+dn+"\".");
        throw LDAP.createException(LDAP.INSUFFICIENT_ACCESS_RIGHTS);
    }

    public void validatePermission(Session session, CompareRequest request, Entry entry) throws Exception {

        DN dn = request.getDn();

        int rc = aclEvaluator.checkRead(session, entry, dn);
        if (rc == LDAP.SUCCESS) return;

        if (debug) log.debug("Not allowed to compare entry \""+dn+"\".");
        throw LDAP.createException(LDAP.INSUFFICIENT_ACCESS_RIGHTS);
    }

    public void validatePermission(Session session, DeleteRequest request, Entry entry) throws Exception {

        DN dn = request.getDn();

        int rc = aclEvaluator.checkDelete(session, entry, dn);
        if (rc == LDAP.SUCCESS) return;

        if (debug) log.debug("Not allowed to delete entry \""+dn+"\".");
        throw LDAP.createException(LDAP.INSUFFICIENT_ACCESS_RIGHTS);
    }

    public void validatePermission(Session session, ModifyRequest request, Entry entry) throws Exception {

        DN dn = request.getDn();

        int rc = aclEvaluator.checkWrite(session, entry, dn);
        if (rc == LDAP.SUCCESS) return;

        if (debug) log.debug("Not allowed to modify entry \""+dn+"\".");
        throw LDAP.createException(LDAP.INSUFFICIENT_ACCESS_RIGHTS);
    }

    public void validatePermission(Session session, ModRdnRequest request, Entry entry) throws Exception {

        DN dn = request.getDn();

        int rc = aclEvaluator.checkWrite(session, entry, dn);
        if (rc == LDAP.SUCCESS) return;

        if (debug) log.debug("Not allowed to rename entry \""+dn+"\".");
        throw LDAP.createException(LDAP.INSUFFICIENT_ACCESS_RIGHTS);
    }

    public void validatePermission(SearchOperation operation, Entry entry) throws Exception {

        DN dn = operation.getDn();

        int rc = aclEvaluator.checkSearch(operation.getSession(), entry, dn);
        if (rc == LDAP.SUCCESS) return;

        if (debug) log.debug("Not allowed to search entry \""+dn+"\".");
        throw LDAP.createException(LDAP.INSUFFICIENT_ACCESS_RIGHTS);
    }

    public void validatePermission(SearchOperation operation, SearchResult result) throws Exception {

        DN dn = result.getDn();
        String entryId = result.getEntryId();
        Entry entry = directory.getEntry(entryId);

        int rc = aclEvaluator.checkRead(operation.getSession(), entry, dn);
        if (rc == LDAP.SUCCESS) return;

        if (debug) log.debug("Not allowed to read entry \""+dn+"\".");
        throw LDAP.createException(LDAP.INSUFFICIENT_ACCESS_RIGHTS);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Schema
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void validateSchema(AddRequest request, Entry entry) throws Exception {

        if (!schemaChecking) return;

        Collection<ObjectClass> objectClasses = schemaManager.getObjectClasses(entry);

        Attributes attributes = request.getAttributes();

        for (Attribute attribute : attributes.getAll()) {
            String attributeName = attribute.getName();
            boolean found = false;

            for (ObjectClass oc : objectClasses) {
                if (oc.getName().equalsIgnoreCase("extensibleObject")) {
                    found = true;
                    break;
                }

                if (oc.containsRequiredAttribute(attributeName)) {
                    found = true;
                    break;
                }

                if (oc.containsOptionalAttribute(attributeName)) {
                    found = true;
                    break;
                }
            }

            if (!found) {
                throw LDAP.createException(LDAP.OBJECT_CLASS_VIOLATION);
            }
        }

        for (ObjectClass oc : objectClasses) {
            for (String attributeName : oc.getRequiredAttributes()) {
                if (attributes.get(attributeName) == null) {
                    throw LDAP.createException(LDAP.OBJECT_CLASS_VIOLATION);
                }
            }
        }
    }

    public void validateSchema(ModifyRequest request, Entry entry) throws Exception {

        if (!schemaChecking) return;

        Collection<ObjectClass> objectClasses = schemaManager.getObjectClasses(entry);

        Collection<Modification> modifications = request.getModifications();
        for (Modification modification : modifications) {
            int type = modification.getType();
            Attribute attribute = modification.getAttribute();
            String attributeName = attribute.getName();

            if (type == Modification.ADD) {

                boolean found = false;

                for (ObjectClass oc : objectClasses) {
                    if (oc.getName().equalsIgnoreCase("extensibleObject")) {
                        found = true;
                        break;
                    }

                    if (oc.containsRequiredAttribute(attributeName)) {
                        found = true;
                        break;
                    }

                    if (oc.containsOptionalAttribute(attributeName)) {
                        found = true;
                        break;
                    }
                }

                if (!found) {
                    throw LDAP.createException(LDAP.OBJECT_CLASS_VIOLATION);
                }

            } else if (type == Modification.DELETE && attribute.isEmpty()) {

                boolean found = false;

                for (ObjectClass oc : objectClasses) {
                    if (oc.containsRequiredAttribute(attributeName)) {
                        found = true;
                        break;
                    }
                }

                if (found) {
                    throw LDAP.createException(LDAP.OBJECT_CLASS_VIOLATION);
                }
            }
        }
    }

    public void validateSchema(ModRdnRequest request, Entry entry) throws Exception {

        if (!schemaChecking) return;

        Collection<ObjectClass> objectClasses = schemaManager.getObjectClasses(entry);

        RDN newRdn = request.getNewRdn();
        for (String attributeName : newRdn.getNames()) {

            boolean found = false;

            for (ObjectClass oc : objectClasses) {
                if (oc.getName().equalsIgnoreCase("extensibleObject")) {
                    found = true;
                    break;
                }

                if (oc.containsRequiredAttribute(attributeName)) {
                    found = true;
                    break;
                }

                if (oc.containsOptionalAttribute(attributeName)) {
                    found = true;
                    break;
                }
            }

            if (!found) {
                throw LDAP.createException(LDAP.OBJECT_CLASS_VIOLATION);
            }
        }

        for (String attributeName : newRdn.getNames()) {
            for (ObjectClass oc : objectClasses) {
                if (oc.containsRequiredAttribute(attributeName)) {
                    throw LDAP.createException(LDAP.OBJECT_CLASS_VIOLATION);
                }
            }
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Add
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void add(
            Session session,
            AddRequest request,
            AddResponse response
    ) throws Exception {

        normalize(request);

        DN dn = request.getDn();

        Collection<Entry> entries = findEntries(dn);

        if (entries.isEmpty()) {
            if (debug) log.debug("Entry "+dn+" not found.");
            throw LDAP.createException(LDAP.NO_SUCH_OBJECT);
        }

        Collection<Module> modules = findModules(dn);

        Exception exception = null;

        for (Entry entry : entries) {
            try {
                if (debug) log.debug("Adding " + dn + " into " + entry.getDn());

                ModuleChain chain = createModuleChain(entry, modules);
                chain.add(session, request, response);

                return; // return after the first successful operation

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

        normalize(request);

        DN dn = request.getDn();

        Collection<Entry> entries = findEntries(dn);

        if (entries.isEmpty()) {
            if (debug) log.debug("Entry "+dn+" not found.");
            throw LDAP.createException(LDAP.NO_SUCH_OBJECT);
        }

        Collection<Module> modules = findModules(dn);

        boolean found = false;
        Exception exception = null;

        for (Entry entry : entries) {
            try {
                if (debug) log.debug("Binding " + dn + " in " + entry.getDn());

                ModuleChain chain = createModuleChain(entry, modules);
                chain.bind(session, request, response);

                return; // return after the first successful operation

            } catch (LDAPException e) {
                if (e.getResultCode() == LDAP.INVALID_CREDENTIALS) found = true;

            } catch (Exception e) {
                log.error(e.getMessage(), e);
                exception = e;
            }
        }

        if (found) throw LDAP.createException(LDAP.INVALID_CREDENTIALS);
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

        normalize(request);

        DN dn = request.getDn();

        Collection<Entry> entries = findEntries(dn);

        if (entries.isEmpty()) {
            if (debug) log.debug("Entry "+dn+" not found.");
            throw LDAP.createException(LDAP.NO_SUCH_OBJECT);
        }

        Collection<Module> modules = findModules(dn);

        Exception exception = null;

        for (Entry entry : entries) {
            try {
                if (debug) log.debug("Comparing " + dn + " in " + entry.getDn());

                ModuleChain chain = createModuleChain(entry, modules);
                chain.compare(session, request, response);

                return; // return after the first successful operation

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

        normalize(request);

        DN dn = request.getDn();

        Collection<Entry> entries = findEntries(dn);

        if (entries.isEmpty()) {
            if (debug) log.debug("Entry "+dn+" not found.");
            throw LDAP.createException(LDAP.NO_SUCH_OBJECT);
        }

        Collection<Module> modules = findModules(dn);

        Exception exception = null;

        for (Entry entry : entries) {
            try {
                if (debug) log.debug("Deleting " + dn + " from " + entry.getDn());

                ModuleChain chain = createModuleChain(entry, modules);
                chain.delete(session, request, response);

                return; // return after the first successful operation

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

        if (debug) log.debug("Finding "+dn);

        SearchRequest request = new SearchRequest();
        request.setDn(dn);
        request.setScope(SearchRequest.SCOPE_BASE);

        SearchResponse response = new SearchResponse();

        search(session, request, response);

        int rc = response.waitFor();
        if (rc != LDAP.SUCCESS) {
            if (debug) log.debug("Entry "+dn+" not found: "+response.getErrorMessage());
            throw LDAP.createException(rc);
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

        normalize(request);

        DN dn = request.getDn();

        Collection<Entry> entries = findEntries(dn);

        if (entries.isEmpty()) {
            if (debug) log.debug("Entry "+dn+" not found.");
            throw LDAP.createException(LDAP.NO_SUCH_OBJECT);
        }

        Collection<Module> modules = findModules(dn);

        Exception exception = null;

        for (Entry entry : entries) {
            try {
                if (debug) log.debug("Modifying " + dn + " in " + entry.getDn());

                ModuleChain chain = createModuleChain(entry, modules);
                chain.modify(session, request, response);

                return; // return after the first successful operation

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

        normalize(request);

        DN dn = request.getDn();

        Collection<Entry> entries = findEntries(dn);

        if (entries.isEmpty()) {
            if (debug) log.debug("Entry "+dn+" not found.");
            throw LDAP.createException(LDAP.NO_SUCH_OBJECT);
        }

        Collection<Module> modules = findModules(dn);

        Exception exception = null;

        for (Entry entry : entries) {
            try {
                if (debug) log.debug("Renaming " + dn + " in " + entry.getDn());

                ModuleChain chain = createModuleChain(entry, modules);
                chain.modrdn(session, request, response);

                return; // return after the first successful operation

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
            Session session,
            SearchRequest request,
            SearchResponse response
    ) throws Exception {

        SearchOperation operation = session.createSearchOperation(""+request.getMessageId(), request, response);

        search(operation);
    }

    public void search(
            SearchOperation operation
    ) throws Exception {

        DN dn = operation.getDn();
        Collection<String> requestedAttributes = operation.getAttributes();

        if (debug) {
            log.debug("Normalized base DN: "+dn);
            log.debug("Normalized attributes: "+requestedAttributes);
        }

        Collection<Entry> entries;

        try {
            entries = findEntries(dn);
        } catch (Exception e) {
            operation.close();
            throw e;
        }

        if (entries.isEmpty()) {
            if (debug) log.debug("Entry "+dn+" not found.");
            operation.close();
            throw LDAP.createException(LDAP.NO_SUCH_OBJECT);
        }

        searchEntries(operation, entries, true);
    }

    public void searchEntry(
            final SearchOperation operation,
            final Entry entry
    ) throws Exception {

        SearchOperation op = new PipelineSearchOperation(operation) {
            public void add(SearchResult result) throws Exception {
                //if (debug) log.debug("Result: \""+result.getDn()+"\".");
                super.add(result);
            }
            public void add(SearchReference reference) throws Exception {
                //if (debug) log.debug("Reference: \""+ reference.getDn()+"\".");
                super.add(reference);
            }
            public void close() throws Exception {
                //if (debug) log.debug("Done.");
                //super.close();
            }
        };

        Collection<Module> modules = findModules(entry.getDn());
        ModuleChain chain = createModuleChain(entry, modules);
        chain.search(op);

        DN baseDn = op.getDn();
        int scope = op.getScope();

        if (scope == SearchRequest.SCOPE_BASE || scope == SearchRequest.SCOPE_ONE && entry.getParentDn().matches(baseDn)) {
            if (debug) log.debug("Children of "+entry.getDn()+" are out of scope.");
            return;
        }

        Collection<Entry> children = entry.getChildren();

        if (children.size() == 0) {
            if (debug) log.debug("Entry "+entry.getDn()+" has no children.");
            return;
        }

        if (debug) log.debug("Searching children of "+entry.getDn()+".");

        searchEntries(op, children, true);
    }

    public void searchEntries(
            final SearchOperation operation,
            final Collection<Entry> entries,
            boolean wait
    ) throws Exception {

        final ParallelSearchOperation op = new ParallelSearchOperation(operation, entries.size());

        if (threadManager == null) {
            // important for deterministic merging
            if (debug) log.debug("Searching "+entries.size()+" entries sequentially.");
        } else {
            // could affect merging outcome
            if (debug) log.debug("Searching "+entries.size()+" entries in parallel.");
        }

        for (final Entry entry : entries) {
            if (debug) log.debug("Searching \""+entry.getDn()+"\".");

            if (op.isAbandoned()) {
                if (debug) log.debug("Operation "+op.getOperationName()+" has been abandoned.");
                op.close();
                continue;
            }

            Runnable runnable = new Runnable() {
                public void run() {
                    try {
                        searchEntry(op, entry);

                    } catch (Throwable e) {
                        log.error(e.getMessage(), e);
                        op.setException(LDAP.createException(e));

                    } finally {
                        try { op.close(entry); } catch (Exception e) { log.error(e.getMessage(), e); }
                    }
                }
            };

            if (threadManager == null) {
                runnable.run();
            } else {
                threadManager.execute(runnable);
            }
        }

        if (wait) {
            if (debug) log.debug("Waiting for "+entries.size()+" entries.");
            op.waitFor();
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

        DN dn = session.getBindDn();
        if (dn == null || dn.isEmpty()) return;

        Collection<Entry> entries = findEntries(dn);

        if (entries.isEmpty()) {
            if (debug) log.debug("Entry "+dn+" not found.");
            throw LDAP.createException(LDAP.NO_SUCH_OBJECT);
        }

        Collection<Module> modules = findModules(dn);

        Exception exception = null;

        for (Entry entry : entries) {
            try {
                if (debug) log.debug("Unbinding " + dn + " from " + entry.getDn());

                ModuleChain chain = createModuleChain(entry, modules);
                chain.unbind(session, request, response);

                return; // return after the first successful operation

            } catch (Exception e) {
                exception = e;
            }
        }

        throw exception;
    }

    public ACLEvaluator getAclEvaluator() {
        return aclEvaluator;
    }

    public void setAclEvaluator(ACLEvaluator aclEvaluator) {
        this.aclEvaluator = aclEvaluator;
    }

    public Interpreter newInterpreter() throws Exception {
        ClassLoader classLoader = partitionContext.getClassLoader();

        Interpreter interpreter = new DefaultInterpreter();
        interpreter.setClassLoader(classLoader);

        return interpreter;
    }

    public AdapterManager getAdapterManager() {
        return adapterManager;
    }

    public void setAdapterManager(AdapterManager adapterManager) {
        this.adapterManager = adapterManager;
    }

    public SchemaManager getSchemaManager() {
        return schemaManager;
    }

    public void setSchemaManager(SchemaManager schemaManager) {
        this.schemaManager = schemaManager;
    }

    public ThreadManager getThreadManager() {
        return threadManager;
    }

    public void setThreadManager(ThreadManager threadManager) {
        this.threadManager = threadManager;
    }

    public void store() throws Exception {

        String partitionName = getName();

        File baseDir;

        File path = partitionContext.getPath();
        if (path == null) {
            baseDir = partitionContext.getPenroseContext().getHome();

        } else {
            File partitionsDir = partitionContext.getPartitionManager().getPartitionsDir();
            baseDir = new File(partitionsDir, partitionName);
        }

        partitionConfig.store(baseDir);
    }

    public Collection<String> getDepends() {
        return partitionConfig.getDepends();
    }
}
