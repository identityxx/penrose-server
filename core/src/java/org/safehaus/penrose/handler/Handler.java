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
package org.safehaus.penrose.handler;

import org.safehaus.penrose.session.*;
import org.safehaus.penrose.acl.ACLManager;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.schema.SchemaManager;
import org.safehaus.penrose.schema.AttributeType;
import org.safehaus.penrose.schema.matchingRule.EqualityMatchingRule;
import org.safehaus.penrose.engine.Engine;
import org.safehaus.penrose.engine.EngineManager;
import org.safehaus.penrose.engine.EngineTool;
import org.safehaus.penrose.interpreter.InterpreterManager;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.mapping.EntryMapping;
import org.safehaus.penrose.mapping.SourceMapping;
import org.safehaus.penrose.mapping.FieldMapping;
import org.safehaus.penrose.config.PenroseConfig;
import org.safehaus.penrose.thread.ThreadManager;
import org.safehaus.penrose.naming.PenroseContext;
import org.safehaus.penrose.cache.EntryCache;
import org.safehaus.penrose.cache.CacheConfig;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.entry.SourceValues;
import org.safehaus.penrose.source.Sources;
import org.safehaus.penrose.source.SourceConfig;
import org.safehaus.penrose.source.FieldConfig;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public abstract class Handler {

    public Logger log = LoggerFactory.getLogger(getClass());
    public boolean debug = log.isDebugEnabled();

    public final static String STOPPED  = "STOPPED";
    public final static String STARTING = "STARTING";
    public final static String STARTED  = "STARTED";
    public final static String STOPPING = "STOPPING";

    public final static String  FETCH         = "fetch";
    public final static boolean DEFAULT_FETCH = false;

    protected PenroseConfig      penroseConfig;
    protected PenroseContext     penroseContext;
    protected SessionContext     sessionContext;

    protected HandlerConfig      handlerConfig;

    protected ThreadManager      threadManager;
    protected SchemaManager      schemaManager;
    protected InterpreterManager interpreterManager;

    protected SessionManager     sessionManager;
    protected HandlerManager     handlerManager;

    protected EngineManager      engineManager;
    protected ACLManager         aclManager;

    protected EntryCache         entryCache;

    protected String status = STOPPED;
    protected boolean fetch = DEFAULT_FETCH;

    public Handler() throws Exception {
    }

    public void init(HandlerConfig handlerConfig) throws Exception {
        this.handlerConfig = handlerConfig;

        threadManager      = penroseContext.getThreadManager();
        schemaManager      = penroseContext.getSchemaManager();
        interpreterManager = penroseContext.getInterpreterManager();

        engineManager      = sessionContext.getEngineManager();
        aclManager         = sessionContext.getAclManager();

        handlerManager     = sessionContext.getHandlerManager();
        sessionManager     = sessionContext.getSessionManager();

        CacheConfig cacheConfig = penroseConfig.getEntryCacheConfig();
        String cacheClass = cacheConfig.getCacheClass() == null ? EntryCache.class.getName() : cacheConfig.getCacheClass();

        //log.debug("Initializing entry cache "+cacheClass);
        Class clazz = Class.forName(cacheClass);
        entryCache = (EntryCache)clazz.newInstance();

        entryCache.setCacheConfig(cacheConfig);
        entryCache.setPenroseConfig(penroseConfig);
        entryCache.setPenroseContext(penroseContext);
        entryCache.init();
    }

    public void start() throws Exception {

        if (!STOPPED.equals(status)) return;

        //log.debug("Starting SessionHandler...");

        try {
            status = STARTING;
            status = STARTED;

            //log.debug("SessionHandler started.");

        } catch (Exception e) {
            status = STOPPED;
            log.error(e.getMessage(), e);
            throw e;
        }
    }

    public void stop() throws Exception {

        if (!STARTED.equals(status)) return;

        try {
            status = STOPPING;

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

        status = STOPPED;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Add
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void add(
            Session session,
            Partition partition,
            EntryMapping entryMapping,
            AddRequest request,
            AddResponse response
    ) throws Exception {

        DN dn = request.getDn();

        SourceValues sourceValues = new SourceValues();

        boolean fetchEntry = fetch;

        String s = entryMapping.getParameter(FETCH);
        if (s != null) fetchEntry = Boolean.valueOf(s);

        if (fetchEntry) {
            EntryMapping parentMapping = partition.getMappings().getParent(entryMapping);
            DN parentDn = dn.getParentDn();

            SearchResult parent = find(session, partition, parentMapping, parentDn);
            sourceValues.add(parent.getSourceValues());

        } else {
            extractSourceValues(partition, entryMapping, dn, sourceValues);
        }

        EngineTool.propagateDown(partition, entryMapping, sourceValues);

        if (debug) {
            log.debug("Source values:");
            sourceValues.print();
        }

        Engine engine = getEngine(partition, entryMapping);
        engine.add(session, partition, entryMapping, sourceValues, request, response);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Bind
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void bind(
            Session session,
            Partition partition,
            EntryMapping entryMapping,
            BindRequest request,
            BindResponse response
    ) throws Exception {

        DN dn = request.getDn();

        SourceValues sourceValues = new SourceValues();

        boolean fetchEntry = fetch;

        String s = entryMapping.getParameter(FETCH);
        if (s != null) fetchEntry = Boolean.valueOf(s);

        if (fetchEntry) {
            SearchResult entry = find(null, partition, entryMapping, dn);
            sourceValues.add(entry.getSourceValues());
        } else {
            extractSourceValues(partition, entryMapping, dn, sourceValues);
        }

        EngineTool.propagateDown(partition, entryMapping, sourceValues);

        if (debug) {
            log.debug("Source values:");
            sourceValues.print();
        }

        Engine engine = getEngine(partition, entryMapping);
        engine.bind(session, partition, entryMapping, sourceValues, request, response);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Compare
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public boolean compare(
            Session session,
            Partition partition,
            EntryMapping entryMapping,
            CompareRequest request,
            CompareResponse response
    ) throws Exception {

        DN dn = request.getDn();

        SourceValues sourceValues = new SourceValues();

        boolean fetchEntry = fetch;

        String s = entryMapping.getParameter(FETCH);
        if (s != null) fetchEntry = Boolean.valueOf(s);

        if (fetchEntry) {
            SearchResult entry = find(null, partition, entryMapping, dn);
            sourceValues.add(entry.getSourceValues());
        } else {
            extractSourceValues(partition, entryMapping, dn, sourceValues);
        }

        EngineTool.propagateDown(partition, entryMapping, sourceValues);

        if (debug) {
            log.debug("Source values:");
            sourceValues.print();
        }

        Engine engine = getEngine(partition, entryMapping);
        return engine.compare(session, partition, entryMapping, sourceValues, request, response);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Unbind
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void unbind(
            Session session,
            Partition partition,
            EntryMapping entryMapping,
            UnbindRequest request,
            UnbindResponse response
    ) throws Exception {

        //Engine engine = getEngine(partition, entryMapping);

        //engine.unbind(session, partition, entryMapping, bindDn);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Find
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public SearchResult find(
            Session session,
            Partition partition,
            EntryMapping entryMapping,
            DN dn
    ) throws Exception {

        Engine engine = getEngine(partition, entryMapping);
        return engine.find(session, partition, entryMapping, dn);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Delete
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void delete(
            Session session,
            Partition partition,
            EntryMapping entryMapping,
            DeleteRequest request,
            DeleteResponse response
    ) throws Exception {

        DN dn = request.getDn();

        SourceValues sourceValues = new SourceValues();

        boolean fetchEntry = fetch;

        String s = entryMapping.getParameter(FETCH);
        if (s != null) fetchEntry = Boolean.valueOf(s);

        if (fetchEntry) {
            SearchResult entry = find(session, partition, entryMapping, dn);
            sourceValues.add(entry.getSourceValues());
        } else {
            extractSourceValues(partition, entryMapping, dn, sourceValues);
        }

        EngineTool.propagateDown(partition, entryMapping, sourceValues);

        if (debug) {
            log.debug("Source values:");
            sourceValues.print();
        }

        Engine engine = getEngine(partition, entryMapping);
        engine.delete(session, partition, entryMapping, sourceValues, request, response);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Modify
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void modify(
            Session session,
            Partition partition,
            EntryMapping entryMapping,
            ModifyRequest request,
            ModifyResponse response
    ) throws Exception {

        DN dn = request.getDn();

        SourceValues sourceValues = new SourceValues();

        boolean fetchEntry = fetch;

        String s = entryMapping.getParameter(FETCH);
        if (s != null) fetchEntry = Boolean.valueOf(s);

        if (fetchEntry) {
            SearchResult entry = find(session, partition, entryMapping, dn);
            sourceValues.add(entry.getSourceValues());
        } else {
            extractSourceValues(partition, entryMapping, dn, sourceValues);
        }

        EngineTool.propagateDown(partition, entryMapping, sourceValues);

        if (debug) {
            log.debug("Source values:");
            sourceValues.print();
        }

        Engine engine = getEngine(partition, entryMapping);
        engine.modify(session, partition, entryMapping, sourceValues, request, response);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // ModRDN
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void modrdn(
            Session session,
            Partition partition,
            EntryMapping entryMapping,
            ModRdnRequest request,
            ModRdnResponse response
    ) throws Exception {

        DN dn = request.getDn();

        SourceValues sourceValues = new SourceValues();

        boolean fetchEntry = fetch;

        String s = entryMapping.getParameter(FETCH);
        if (s != null) fetchEntry = Boolean.valueOf(s);

        if (fetchEntry) {
            SearchResult entry = find(session, partition, entryMapping, dn);
            sourceValues.add(entry.getSourceValues());
        } else {
            extractSourceValues(partition, entryMapping, dn, sourceValues);
        }

        EngineTool.propagateDown(partition, entryMapping, sourceValues);

        if (debug) {
            log.debug("Source values:");
            sourceValues.print();
        }

        Engine engine = getEngine(partition, entryMapping);
        engine.modrdn(session, partition, entryMapping, sourceValues, request, response);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Search
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void search(
            Session session,
            Partition partition,
            EntryMapping entryMapping,
            SearchRequest request,
            SearchResponse<SearchResult> results
    ) throws Exception {

        search(
                session,
                partition,
                entryMapping,
                entryMapping,
                request,
                results
        );
    }

    public abstract void search(
            Session session,
            Partition partition,
            EntryMapping baseMapping,
            EntryMapping entryMapping,
            SearchRequest request,
            SearchResponse<SearchResult> response
    ) throws Exception;

    public void performSearch(
            final Session session,
            final Partition partition,
            final EntryMapping baseMapping,
            final EntryMapping entryMapping,
            final SearchRequest request,
            final SearchResponse<SearchResult> response
    ) throws Exception {

        Engine engine = getEngine(partition, entryMapping);
        engine.search(
                session,
                partition,
                baseMapping,
                entryMapping,
                request,
                response
        );
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Miscelleanous
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public PenroseConfig getPenroseConfig() {
        return penroseConfig;
    }

    public void setPenroseConfig(PenroseConfig penroseConfig) {
        this.penroseConfig = penroseConfig;
    }

    public EngineManager getEngineManager() {
        return engineManager;
    }

    public void setEngineManager(EngineManager engineManager) {
        this.engineManager = engineManager;
    }

    public Engine getEngine(Partition partition, EntryMapping entryMapping) {
        String engineName = entryMapping.getEngineName();
        if (engineName != null) return engineManager.getEngine(engineName);

        engineName = partition.getEngineName();
        if (engineName != null) return engineManager.getEngine(engineName);

        return engineManager.getEngine("DEFAULT");
    }

    public SchemaManager getSchemaManager() {
        return schemaManager;
    }

    public void setSchemaManager(SchemaManager schemaManager) {
        this.schemaManager = schemaManager;
    }

    public SessionManager getSessionManager() {
        return sessionManager;
    }

    public void setSessionManager(SessionManager sessionManager) {
        this.sessionManager = sessionManager;
    }

    public InterpreterManager getInterpreterFactory() {
        return interpreterManager;
    }

    public void setInterpreterFactory(InterpreterManager interpreterManager) {
        this.interpreterManager = interpreterManager;
    }

    public EntryCache getEntryCache() {
        return entryCache;
    }

    public void setEntryCache(EntryCache entryCache) {
        this.entryCache = entryCache;
    }

    public HandlerConfig getHandlerConfig() {
        return handlerConfig;
    }

    public void setHandlerConfig(HandlerConfig handlerConfig) {
        this.handlerConfig = handlerConfig;
    }

    public PenroseContext getPenroseContext() {
        return penroseContext;
    }

    public void setPenroseContext(PenroseContext penroseContext) {
        this.penroseContext = penroseContext;
    }

    public SessionContext getSessionContext() {
        return sessionContext;
    }

    public void setSessionContext(SessionContext sessionContext) {
        this.sessionContext = sessionContext;
    }

    public void extractSourceValues(
            Partition partition,
            EntryMapping entryMapping,
            DN dn,
            SourceValues sourceValues
    ) throws Exception {

        Interpreter interpreter = interpreterManager.newInstance();

        if (debug) log.debug("Extracting source values from "+dn);

        extractSourceValues(
                partition,
                interpreter,
                dn,
                entryMapping,
                sourceValues
        );
    }

    public void extractSourceValues(
            Partition partition,
            Interpreter interpreter,
            DN dn,
            EntryMapping entryMapping,
            SourceValues sourceValues
    ) throws Exception {

        DN parentDn = dn.getParentDn();
        EntryMapping em = partition.getMappings().getParent(entryMapping);

        if (parentDn != null && em != null) {
            extractSourceValues(partition, interpreter, parentDn, em, sourceValues);
        }

        RDN rdn = dn.getRdn();
        Collection<SourceMapping> sourceMappings = entryMapping.getSourceMappings();

        //if (sourceMappings.isEmpty()) return;
        //SourceMapping sourceMapping = sourceMappings.iterator().next();

        //interpreter.set(sourceValues);
        interpreter.set(rdn);

        for (SourceMapping sourceMapping : sourceMappings) {
            extractSourceValues(
                    partition,
                    interpreter,
                    rdn,
                    entryMapping,
                    sourceMapping,
                    sourceValues
            );
        }

        interpreter.clear();
    }

    public void extractSourceValues(
            Partition partition,
            Interpreter interpreter,
            RDN rdn,
            EntryMapping entryMapping,
            SourceMapping sourceMapping,
            SourceValues sourceValues
    ) throws Exception {

        if (debug) log.debug("Extracting source "+sourceMapping.getName()+" from RDN: "+rdn);

        Attributes attributes = sourceValues.get(sourceMapping.getName());

        Sources sources = partition.getSources();
        SourceConfig sourceConfig = sources.getSourceConfig(sourceMapping.getSourceName());

        Collection<FieldMapping> fieldMappings = sourceMapping.getFieldMappings();
        for (FieldMapping fieldMapping : fieldMappings) {
            FieldConfig fieldConfig = sourceConfig.getFieldConfig(fieldMapping.getName());

            Object value = interpreter.eval(fieldMapping);
            if (value == null) continue;

            if ("INTEGER".equals(fieldConfig.getType()) && value instanceof String) {
                value = Integer.parseInt((String)value);
            }

            attributes.addValue(fieldMapping.getName(), value);

            String fieldName = sourceMapping.getName() + "." + fieldMapping.getName();
            if (debug) log.debug(" => " + fieldName + ": " + value);
        }
    }

}

