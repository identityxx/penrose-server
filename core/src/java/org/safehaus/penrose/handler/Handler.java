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
import org.safehaus.penrose.filter.FilterTool;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.schema.SchemaManager;
import org.safehaus.penrose.schema.AttributeType;
import org.safehaus.penrose.schema.matchingRule.EqualityMatchingRule;
import org.safehaus.penrose.engine.Engine;
import org.safehaus.penrose.engine.EngineManager;
import org.safehaus.penrose.interpreter.InterpreterManager;
import org.safehaus.penrose.mapping.EntryMapping;
import org.safehaus.penrose.config.PenroseConfig;
import org.safehaus.penrose.thread.ThreadManager;
import org.safehaus.penrose.naming.PenroseContext;
import org.safehaus.penrose.entry.*;
import org.safehaus.penrose.cache.EntryCache;
import org.safehaus.penrose.cache.CacheConfig;
import org.safehaus.penrose.util.ExceptionUtil;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import org.ietf.ldap.LDAPException;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public abstract class Handler {

    public Logger log = LoggerFactory.getLogger(getClass());

    public final static String STOPPED  = "STOPPED";
    public final static String STARTING = "STARTING";
    public final static String STARTED  = "STARTED";
    public final static String STOPPING = "STOPPING";

    protected PenroseConfig      penroseConfig;
    protected PenroseContext     penroseContext;

    protected HandlerConfig      handlerConfig;

    protected ThreadManager      threadManager;
    protected SchemaManager      schemaManager;
    protected InterpreterManager interpreterManager;

    protected SessionManager     sessionManager;
    protected HandlerManager     handlerManager;

    protected EngineManager      engineManager;
    protected ACLManager         aclManager;

    protected FilterTool         filterTool;
    protected EntryCache         entryCache;

    protected String status = STOPPED;

    public Handler() throws Exception {
    }

    public void init(HandlerConfig handlerConfig) throws Exception {
        this.handlerConfig = handlerConfig;

        threadManager      = penroseContext.getThreadManager();
        schemaManager      = penroseContext.getSchemaManager();
        interpreterManager = penroseContext.getInterpreterManager();

        sessionManager     = penroseContext.getSessionManager();
        handlerManager     = penroseContext.getHandlerManager();

        engineManager      = penroseContext.getEngineManager();
        aclManager         = penroseContext.getAclManager();


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

        if (status != STOPPED) return;

        //log.debug("Starting SessionHandler...");

        try {
            status = STARTING;

            filterTool = new FilterTool();
            filterTool.setSchemaManager(schemaManager);

            status = STARTED;

            //log.debug("SessionHandler started.");

        } catch (Exception e) {
            status = STOPPED;
            log.error(e.getMessage(), e);
            throw e;
        }
    }

    public void stop() throws Exception {

        if (status != STARTED) return;

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

    public void add(
            Session session,
            Partition partition,
            EntryMapping entryMapping,
            AddRequest request,
            AddResponse response
    ) throws Exception {

        Engine engine = getEngine(partition, entryMapping);

        Entry parent = null; //find(session, partition, entryMapping.getParent(), dn.getParentDn());
        engine.add(session, partition, parent, entryMapping, request, response);
    }

    public void bind(
            Session session,
            Partition partition,
            EntryMapping entryMapping,
            BindRequest request,
            BindResponse response
    ) throws Exception {

        Engine engine = getEngine(partition, entryMapping);

        Entry entry = null; //find(session, partition, entryMapping, dn);
        engine.bind(session, partition, entryMapping, request, response);
    }

    public void unbind(
            Session session,
            Partition partition,
            EntryMapping entryMapping,
            UnbindRequest request,
            UnbindResponse response
    ) throws Exception {

        Engine engine = getEngine(partition, entryMapping);

        Entry entry = null; //find(session, partition, entryMapping, dn);
        //engine.unbind(session, partition, entryMapping, bindDn);
    }

    public Entry find(
            Session session,
            Partition partition,
            EntryMapping entryMapping,
            DN dn
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        SearchRequest request = new SearchRequest();
        request.setDn(dn);
        request.setFilter((Filter)null);
        request.setScope(SearchRequest.SCOPE_BASE);

        SearchResponse response = new SearchResponse();

        search(
                session,
                partition,
                entryMapping,
                request,
                response
        );

        if (response.hasNext()) {
            if (debug) log.debug("Entry "+dn+" not found");
            throw ExceptionUtil.createLDAPException(LDAPException.NO_SUCH_OBJECT);
        }

        return (Entry) response.next();
    }

    public boolean compare(
            Session session,
            Partition partition,
            EntryMapping entryMapping,
            CompareRequest request,
            CompareResponse response
    ) throws Exception {

        Engine engine = getEngine(partition, entryMapping);

        Entry entry = null; //find(session, partition, entryMapping, dn);
        return engine.compare(session, partition, entryMapping, request, response);
    }

    public void delete(
            Session session,
            Partition partition,
            EntryMapping entryMapping,
            DeleteRequest request,
            DeleteResponse response
    ) throws Exception {

        Engine engine = getEngine(partition, entryMapping);

        Entry entry = null; //find(session, partition, entryMapping, dn);
        engine.delete(session, partition, entry, entryMapping, request, response);
    }

    public void modify(
            Session session,
            Partition partition,
            EntryMapping entryMapping,
            ModifyRequest request,
            ModifyResponse response
    ) throws Exception {

        Engine engine = getEngine(partition, entryMapping);

        Entry entry = null; //find(session, partition, entryMapping, dn);
        engine.modify(session, partition, entry, entryMapping, request, response);
    }

    public void modrdn(
            Session session,
            Partition partition,
            EntryMapping entryMapping,
            ModRdnRequest request,
            ModRdnResponse response
    ) throws Exception {

        Engine engine = getEngine(partition, entryMapping);

        Entry entry = null; //find(session, partition, entryMapping, dn);
        engine.modrdn(session, partition, entry, entryMapping, request, response);
    }

    public void search(
            Session session,
            Partition partition,
            EntryMapping entryMapping,
            SearchRequest request,
            SearchResponse results
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
            SearchResponse response
    ) throws Exception;

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

    public FilterTool getFilterTool() {
        return filterTool;
    }

    public void setFilterTool(FilterTool filterTool) {
        this.filterTool = filterTool;
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
}

