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
import org.safehaus.penrose.acl.ACLEngine;
import org.safehaus.penrose.filter.FilterTool;
import org.safehaus.penrose.partition.PartitionManager;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.schema.SchemaManager;
import org.safehaus.penrose.schema.AttributeType;
import org.safehaus.penrose.engine.Engine;
import org.safehaus.penrose.engine.EngineManager;
import org.safehaus.penrose.interpreter.InterpreterManager;
import org.safehaus.penrose.event.*;
import org.safehaus.penrose.mapping.EntryMapping;
import org.safehaus.penrose.entry.AttributeValues;
import org.safehaus.penrose.config.PenroseConfig;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.thread.ThreadManager;
import org.safehaus.penrose.naming.PenroseContext;
import org.safehaus.penrose.entry.*;
import org.safehaus.penrose.cache.EntryCache;
import org.safehaus.penrose.cache.CacheConfig;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import javax.naming.directory.*;
import javax.naming.NamingEnumeration;
import java.util.*;

/**
 * @author Endi S. Dewata
 */
public abstract class Handler {

    Logger log = LoggerFactory.getLogger(getClass());

    public final static String STOPPED  = "STOPPED";
    public final static String STARTING = "STARTING";
    public final static String STARTED  = "STARTED";
    public final static String STOPPING = "STOPPING";

    PenroseConfig penroseConfig;
    PenroseContext penroseContext;

    HandlerConfig handlerConfig;

    SchemaManager schemaManager;
    EngineManager engineManager;

    SessionManager sessionManager;

    InterpreterManager interpreterManager;
    ACLEngine aclEngine;
    FilterTool filterTool;
    EntryCache entryCache;

    ThreadManager threadManager;

    private String status = STOPPED;

    public Handler() throws Exception {
    }

    public void init(HandlerConfig handlerConfig) throws Exception {
        this.handlerConfig = handlerConfig;

        sessionManager     = penroseContext.getSessionManager();
        schemaManager      = penroseContext.getSchemaManager();
        interpreterManager = penroseContext.getInterpreterManager();
        engineManager      = penroseContext.getEngineManager();

        threadManager = penroseContext.getThreadManager();

        aclEngine = new ACLEngine();
        aclEngine.setPenroseConfig(penroseConfig);
        aclEngine.setPenroseContext(penroseContext);

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

    public abstract void bind(
            PenroseSession session,
            Partition partition,
            DN dn,
            String password
    ) throws Exception;

    public abstract void unbind(PenroseSession session) throws Exception;

    public abstract void add(
            PenroseSession session,
            Partition partition,
            DN dn,
            Attributes attributes
    ) throws Exception;

    public abstract boolean compare(
            PenroseSession session,
            Partition partition,
            DN dn,
            String attributeName,
            Object attributeValue
    ) throws Exception;

    public abstract void delete(
            PenroseSession session,
            Partition partition,
            DN dn
    ) throws Exception;

    public abstract void modify(
            PenroseSession session,
            Partition partition,
            DN dn,
            Collection modifications
    ) throws Exception;

    public abstract void modrdn(
            PenroseSession session,
            Partition partition,
            DN dn,
            RDN newRdn,
            boolean deleteOldRdn
    ) throws Exception;

    public abstract void search(
            PenroseSession session,
            Partition partition,
            DN baseDn,
            String filter,
            PenroseSearchControls sc,
            Results results
    ) throws Exception;

    public Entry createRootDSE() throws Exception {

        Entry entry = new Entry("", null);

        AttributeValues attributeValues = entry.getAttributeValues();
        attributeValues.set("objectClass", "top");
        attributeValues.add("objectClass", "extensibleObject");
        attributeValues.set("vendorName", Penrose.VENDOR_NAME);
        attributeValues.set("vendorVersion", Penrose.PRODUCT_NAME+" "+Penrose.PRODUCT_VERSION);

        PartitionManager partitionManager = penroseContext.getPartitionManager();
        for (Iterator i=partitionManager.getPartitions().iterator(); i.hasNext(); ) {
            Partition p = (Partition)i.next();
            for (Iterator j=p.getRootEntryMappings().iterator(); j.hasNext(); ) {
                EntryMapping e = (EntryMapping)j.next();
                if (e.getDn().isEmpty()) continue;
                attributeValues.add("namingContexts", e.getDn());
            }
        }

        return entry;
    }

    public void printResult(Entry entry) throws Exception {
        log.debug("dn: "+entry.getDn());
        AttributeValues attributeValues = entry.getAttributeValues();
        for (Iterator i = attributeValues.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Collection values = attributeValues.get(name);
            for (Iterator j = values.iterator(); j.hasNext(); ) {
                Object value = j.next();
                log.debug(name+": "+value);
            }
        }
    }

    public void removeAttributes(Entry entry, Collection list) throws Exception {
        AttributeValues attributeValues = entry.getAttributeValues();
        for (Iterator i=list.iterator(); i.hasNext(); ) {
            String attributeName = (String)i.next();
            attributeValues.remove(attributeName);
        }
    }

    public Collection filterAttributes(
            PenroseSession session,
            Partition partition,
            Entry entry,
            Collection requestedAttributeNames,
            boolean allRegularAttributes,
            boolean allOpAttributes
    ) throws Exception {

        Collection list = new HashSet();

        if (session == null) return list;

        AttributeValues attributeValues = entry.getAttributeValues();
        Collection attributeNames = attributeValues.getNames();

        boolean debug = log.isDebugEnabled();
        if (debug) {
            log.debug("Returned: "+attributeNames);
        }
        
        if (allRegularAttributes && allOpAttributes) {
            log.debug("Returning all attributes.");
            return list;
        }

        if (allRegularAttributes) {

            // return regular attributes only
            for (Iterator i=attributeValues.getNames().iterator(); i.hasNext(); ) {
                String attributeName = (String)i.next();

                AttributeType attributeType = schemaManager.getAttributeType(attributeName);
                if (attributeType == null) {
                    if (debug) log.debug("Attribute "+attributeName+" undefined.");
                    continue;
                }
                
                if (!attributeType.isOperational()) {
                    //log.debug("Keep regular attribute "+attributeName);
                    continue;
                }

                //log.debug("Remove operational attribute "+attributeName);
                list.add(attributeName);
            }

        } else if (allOpAttributes) {

            // return operational attributes only
            for (Iterator i=attributeValues.getNames().iterator(); i.hasNext(); ) {
                String attributeName = (String)i.next();

                AttributeType attributeType = schemaManager.getAttributeType(attributeName);
                if (attributeType == null) {
                    if (debug) log.debug("Attribute "+attributeName+" undefined.");
                    list.add(attributeName);
                    continue;
                }

                if (attributeType.isOperational()) {
                    //log.debug("Keep operational attribute "+attributeName);
                    continue;
                }

                //log.debug("Remove regular attribute "+attributeName);
                list.add(attributeName);
            }

        } else {

            // return requested attributes
            for (Iterator i=attributeValues.getNames().iterator(); i.hasNext(); ) {
                String attributeName = (String)i.next();

                if (requestedAttributeNames.contains(attributeName)) {
                    //log.debug("Keep requested attribute "+attributeName);
                    continue;
                }

                //log.debug("Remove unrequested attribute "+attributeName);
                list.add(attributeName);
            }
        }

        return list;
    }

    public void filterAttributes(
            PenroseSession session,
            Partition partition,
            Entry entry
    ) throws Exception {

        if (session == null) return;

        EntryMapping entryMapping = entry.getEntryMapping();
        AttributeValues attributeValues = entry.getAttributeValues();

        Collection attributeNames = new ArrayList();
        for (Iterator i=attributeValues.getNames().iterator(); i.hasNext(); ) {
            String attributeName = (String)i.next();
            attributeNames.add(attributeName.toLowerCase());
        }

        Set grants = new HashSet();
        Set denies = new HashSet();
        denies.addAll(attributeNames);

        DN bindDn = session.getBindDn();
        DN targetDn = entry.getDn();
        aclEngine.getReadableAttributes(bindDn, partition, entryMapping, targetDn, null, attributeNames, grants, denies);

        boolean debug = log.isDebugEnabled();
        if (debug) {
            log.debug("Returned: "+attributeNames);
            log.debug("Granted: "+grants);
            log.debug("Denied: "+denies);
        }

        Collection list = new ArrayList();

        for (Iterator i=attributeValues.getNames().iterator(); i.hasNext(); ) {
            String attributeName = (String)i.next();
            String normalizedName = attributeName.toLowerCase();

            if (!denies.contains(normalizedName)) {
                //log.debug("Keep undenied attribute "+normalizedName);
                continue;
            }

            //log.debug("Remove denied attribute "+normalizedName);
            list.add(attributeName);
        }

        removeAttributes(entry, list);
    }

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

    public Attributes normalize(Attributes attributes) throws Exception{

        BasicAttributes newAttributes = new BasicAttributes();

        for (NamingEnumeration e=attributes.getAll(); e.hasMore(); ) {
            Attribute attribute = (Attribute)e.next();
            String attributeName = schemaManager.getNormalizedAttributeName(attribute.getID());

            BasicAttribute newAttribute = new BasicAttribute(attributeName);
            for (NamingEnumeration e2=attribute.getAll(); e2.hasMore(); ) {
                Object value = e2.next();
                newAttribute.add(value);
            }

            newAttributes.put(newAttribute);
        }

        return newAttributes;
    }

    public DN normalize(DN oldDn) {
        DNBuilder db = new DNBuilder();
        RDNBuilder rb = new RDNBuilder();

        for (Iterator i=oldDn.getRdns().iterator(); i.hasNext(); ) {
            RDN rdn = (RDN)i.next();

            rb.clear();
            for (Iterator j=rdn.getNames().iterator(); j.hasNext(); ) {
                String name = (String)j.next();
                Object value = rdn.get(name);
                rb.set(schemaManager.getNormalizedAttributeName(name), value);
            }
            db.append(rb.toRdn());
        }
        return db.toDn();
    }

    public Collection normalize(Collection attributeNames) {
        if (attributeNames == null) return null;

        Collection list = new ArrayList();
        for (Iterator i = attributeNames.iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            list.add(schemaManager.getNormalizedAttributeName(name));
        }

        return list;
    }

    public FilterTool getFilterTool() {
        return filterTool;
    }

    public void setFilterTool(FilterTool filterTool) {
        this.filterTool = filterTool;
    }

    public Engine getEngine() {
        return engineManager.getEngine("DEFAULT");
    }

    public Engine getEngine(String name) {
        return engineManager.getEngine(name);
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

    public AttributeValues pushSourceValues(
            AttributeValues oldSourceValues,
            AttributeValues newSourceValues
    ) {
        AttributeValues av = new AttributeValues();

        for (Iterator i=oldSourceValues.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Collection values = oldSourceValues.get(name);

            if (name.startsWith("parent.")) name = "parent."+name;
            av.add(name, values);
        }

        if (newSourceValues != null) {
            for (Iterator i=newSourceValues.getNames().iterator(); i.hasNext(); ) {
                String name = (String)i.next();
                Collection values = newSourceValues.get(name);

                av.add("parent."+name, values);
            }
        }

        return av;
    }

    public void addConnectionListener(ConnectionListener l) {
    }

    public void removeConnectionListener(ConnectionListener l) {
    }

    public void addBindListener(BindListener l) {
    }

    public void removeBindListener(BindListener l) {
    }

    public void addSearchListener(SearchListener l) {
    }

    public void removeSearchListener(SearchListener l) {
    }

    public void addCompareListener(CompareListener l) {
    }

    public void removeCompareListener(CompareListener l) {
    }

    public void addAddListener(AddListener l) {
    }

    public void removeAddListener(AddListener l) {
    }

    public void addDeleteListener(DeleteListener l) {
    }

    public void removeDeleteListener(DeleteListener l) {
    }

    public void addModifyListener(ModifyListener l) {
    }

    public void removeModifyListener(ModifyListener l) {
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

