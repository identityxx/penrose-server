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

import org.safehaus.penrose.config.PenroseConfig;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.PartitionManager;
import org.safehaus.penrose.naming.PenroseContext;
import org.safehaus.penrose.session.*;
import org.safehaus.penrose.entry.DN;
import org.safehaus.penrose.entry.Entry;
import org.safehaus.penrose.entry.AttributeValues;
import org.safehaus.penrose.entry.RDN;
import org.safehaus.penrose.util.*;
import org.safehaus.penrose.mapping.EntryMapping;
import org.safehaus.penrose.schema.SchemaManager;
import org.safehaus.penrose.schema.AttributeType;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.thread.ThreadManager;
import org.safehaus.penrose.acl.ACLManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ietf.ldap.LDAPConnection;
import org.ietf.ldap.LDAPException;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class HandlerManager {

    Logger log = LoggerFactory.getLogger(getClass());
    
    Map handlers = new TreeMap();

    private PenroseConfig penroseConfig;
    private PenroseContext penroseContext;

    ThreadManager threadManager;
    SchemaManager schemaManager;
    ACLManager aclManager;

    public HandlerManager() {
    }

    public PenroseConfig getPenroseConfig() {
        return penroseConfig;
    }

    public void setPenroseConfig(PenroseConfig penroseConfig) {
        this.penroseConfig = penroseConfig;
    }

    public PenroseContext getPenroseContext() {
        return penroseContext;
    }

    public void setPenroseContext(PenroseContext penroseContext) {
        this.penroseContext = penroseContext;

        threadManager = penroseContext.getThreadManager();
        schemaManager = penroseContext.getSchemaManager();
        aclManager = penroseContext.getAclManager();
    }

    public void init(HandlerConfig handlerConfig) throws Exception {

        String name = handlerConfig.getName();
        String className = handlerConfig.getHandlerClass();
        if (className == null) {
            className = DefaultHandler.class.getName();
        }

        log.debug("Initializing handler "+name+": "+className);

        Class clazz = Class.forName(className);
        Handler handler = (Handler)clazz.newInstance();

        handler.setPenroseConfig(penroseConfig);
        handler.setPenroseContext(penroseContext);
        handler.init(handlerConfig);

        handlers.put(handlerConfig.getName(), handler);
    }

    public Handler getHandler(String name) {
        return (Handler)handlers.get(name);
    }

    public Handler getHandler(EntryMapping entryMapping) {
        String handlerName = entryMapping == null ? "DEFAULT" : entryMapping.getHandlerName();
        if (log.isDebugEnabled()) {
            log.debug("Getting handler for entry "+entryMapping.getDn()+": "+handlerName);
        }
        return (Handler)handlers.get(handlerName);
    }

    public Handler getHandler(Partition partition) {
        String handlerName = partition == null ? "DEFAULT" : partition.getHandlerName();
        if (log.isDebugEnabled()) {
            log.debug("Getting handler for partition "+partition+": "+handlerName);
        }
        return (Handler)handlers.get(handlerName);
    }
    
    public void clear() {
        handlers.clear();
    }

    public void start() throws Exception {
        for (Iterator i=handlers.values().iterator(); i.hasNext(); ) {
            Handler handler = (Handler)i.next();
            handler.start();
        }
    }

    public void stop() throws Exception {
        for (Iterator i=handlers.values().iterator(); i.hasNext(); ) {
            Handler handler = (Handler)i.next();
            handler.stop();
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // ADD
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void add(
            Session session,
            Partition partition,
            AddRequest request,
            AddResponse response
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        DN dn = schemaManager.normalize(request.getDn());
        request.setDn(dn);

        AttributeValues attributeValues = schemaManager.normalize(request.getAttributeValues());
        request.setAttributeValues(attributeValues);

        DN parentDn = dn.getParentDn();

        Collection entryMappings = partition.findEntryMappings(dn);
        Exception exception = null;

        for (Iterator i=entryMappings.iterator(); i.hasNext(); ) {
            EntryMapping entryMapping = (EntryMapping)i.next();
            if (debug) log.debug("Adding "+dn+" into "+entryMapping.getDn());

            EntryMapping parentMapping = partition.getParent(entryMapping);
            int rc = aclManager.checkAdd(session, partition, parentMapping, parentDn);

            if (rc != LDAPException.SUCCESS) {
                if (debug) log.debug("Not allowed to add "+dn);
                exception = ExceptionUtil.createLDAPException(LDAPException.INSUFFICIENT_ACCESS_RIGHTS);
                continue;
            }

            try {
                Handler handler = getHandler(entryMapping);
                handler.add(session, partition, entryMapping, request, response);
                return;
            } catch (Exception e) {
                exception = e;
            }
        }

        throw exception;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // BIND
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void bind(
            Session session,
            Partition partition,
            BindRequest request,
            BindResponse response
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        DN dn = schemaManager.normalize(request.getDn());
        request.setDn(dn);

        Collection entryMappings = partition.findEntryMappings(dn);

        for (Iterator i=entryMappings.iterator(); i.hasNext(); ) {
            EntryMapping entryMapping = (EntryMapping)i.next();
            if (debug) log.debug("Binding "+dn+" in "+entryMapping.getDn());

            Handler handler = getHandler(entryMapping);
            handler.bind(session, partition, entryMapping, request, response);
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // COMPARE
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public boolean compare(
            Session session,
            Partition partition,
            CompareRequest request,
            CompareResponse response
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        DN dn = schemaManager.normalize(request.getDn());
        request.setDn(dn);

        String attributeName = schemaManager.normalizeAttributeName(request.getAttributeName());
        request.setAttributeName(attributeName);

        Collection entryMappings = partition.findEntryMappings(dn);
        Exception exception = null;

        for (Iterator i=entryMappings.iterator(); i.hasNext(); ) {
            EntryMapping entryMapping = (EntryMapping)i.next();
            if (debug) log.debug("Comparing "+dn+" in "+entryMapping.getDn());

            int rc = aclManager.checkRead(session, partition, entryMapping, dn);

            if (rc != LDAPException.SUCCESS) {
                if (debug) log.debug("Not allowed to compare "+dn);
                exception = ExceptionUtil.createLDAPException(LDAPException.INSUFFICIENT_ACCESS_RIGHTS);
                continue;
            }

            try {
                Handler handler = getHandler(entryMapping);
                return handler.compare(session, partition, entryMapping, request, response);
            } catch (Exception e) {
                exception = e;
            }
        }

        throw exception;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // DELETE
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void delete(
            Session session,
            Partition partition,
            DeleteRequest request,
            DeleteResponse response
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        DN dn = schemaManager.normalize(request.getDn());
        request.setDn(dn);

        Collection entryMappings = partition.findEntryMappings(dn);
        Exception exception = null;

        for (Iterator i=entryMappings.iterator(); i.hasNext(); ) {
            EntryMapping entryMapping = (EntryMapping)i.next();
            if (debug) log.debug("Deleting "+dn+" from "+entryMapping.getDn());

            int rc = aclManager.checkDelete(session, partition, entryMapping, dn);

            if (rc != LDAPException.SUCCESS) {
                if (debug) log.debug("Not allowed to delete "+dn);
                exception = ExceptionUtil.createLDAPException(LDAPException.INSUFFICIENT_ACCESS_RIGHTS);
                continue;
            }

            try {
                Handler handler = getHandler(entryMapping);
                handler.delete(session, partition, entryMapping, request, response);
                return;
            } catch (Exception e) {
                exception = e;
            }
        }

        throw exception;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // MODIFY
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void modify(
            Session session,
            Partition partition,
            ModifyRequest request,
            ModifyResponse response
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        DN dn = schemaManager.normalize(request.getDn());
        request.setDn(dn);

        Collection modifications = schemaManager.normalizeModifications(request.getModifications());
        request.setModifications(modifications);

        Collection entryMappings = partition.findEntryMappings(dn);
        Exception exception = null;

        for (Iterator i=entryMappings.iterator(); i.hasNext(); ) {
            EntryMapping entryMapping = (EntryMapping)i.next();
            if (debug) log.debug("Modifying "+dn+" in "+entryMapping.getDn());

            int rc = aclManager.checkModify(session, partition, entryMapping, dn);

            if (rc != LDAPException.SUCCESS) {
                if (debug) log.debug("Not allowed to modify "+dn);
                exception = ExceptionUtil.createLDAPException(LDAPException.INSUFFICIENT_ACCESS_RIGHTS);
                continue;
            }

            try {
                Handler handler = getHandler(entryMapping);
                handler.modify(session, partition, entryMapping, request, response);
                return;
            } catch (Exception e) {
                exception = e;
            }
        }

        throw exception;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // MODRDN
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void modrdn(
            Session session,
            Partition partition,
            ModRdnRequest request,
            ModRdnResponse response
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        DN dn = schemaManager.normalize(request.getDn());
        request.setDn(dn);

        RDN newRdn = schemaManager.normalize(request.getNewRdn());
        request.setNewRdn(newRdn);

        Collection entryMappings = partition.findEntryMappings(dn);
        Exception exception = null;

        for (Iterator i=entryMappings.iterator(); i.hasNext(); ) {
            EntryMapping entryMapping = (EntryMapping)i.next();
            if (debug) log.debug("Renaming "+dn+" in "+entryMapping.getDn());

            int rc = aclManager.checkModify(session, partition, entryMapping, dn);

            if (rc != LDAPException.SUCCESS) {
                if (debug) log.debug("Not allowed to modify "+dn);
                exception = ExceptionUtil.createLDAPException(LDAPException.INSUFFICIENT_ACCESS_RIGHTS);
                continue;
            }

            try {
                Handler handler = getHandler(entryMapping);
                handler.modrdn(session, partition, entryMapping, request, response);
                return;
            } catch (Exception e) {
                exception = e;
            }
        }

        throw exception;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // SEARCH
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void search(
            final Session session,
            final Partition partition,
            final SearchRequest request,
            final SearchResponse response
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        DN baseDn = schemaManager.normalize(request.getDn());
        request.setDn(baseDn);

        Collection attributes = schemaManager.normalize(request.getAttributes());
        request.setAttributes(attributes);

        final Set requestedAttributes = new HashSet();
        if (request.getAttributes() != null) requestedAttributes.addAll(request.getAttributes());

        final boolean allRegularAttributes = request.getAttributes() == null || request.getAttributes().isEmpty() || request.getAttributes().contains("*");
        final boolean allOpAttributes = request.getAttributes() != null && request.getAttributes().contains("+");

        if (debug) log.debug("Requested: "+request.getAttributes());

        if (baseDn.isEmpty()) {
            if (request.getScope() == LDAPConnection.SCOPE_BASE) {
                Entry entry = createRootDSE();

                if (debug) {
                    log.debug("Before: "+entry.getDn());
                    entry.getAttributeValues().print();
                }

                Collection list = filterAttributes(session, partition, entry, requestedAttributes, allRegularAttributes, allOpAttributes);
                removeAttributes(entry, list);

                if (debug) {
                    log.debug("After: "+entry.getDn());
                    entry.getAttributeValues().print();
                }

                response.add(entry);
            }
            response.close();
            return;
        }

        Collection entryMappings = partition.findEntryMappings(baseDn);

        if (entryMappings.isEmpty()) {
            if (debug) log.debug("Base DN "+baseDn+" not found.");
            throw ExceptionUtil.createLDAPException(LDAPException.NO_SUCH_OBJECT);
        }

        final HandlerSearchResponse sr = new HandlerSearchResponse(
                response,
                session,
                partition,
                this,
                schemaManager,
                aclManager,
                requestedAttributes,
                allRegularAttributes,
                allOpAttributes,
                entryMappings
        );

        for (Iterator i=entryMappings.iterator(); i.hasNext(); ) {
            final EntryMapping entryMapping = (EntryMapping)i.next();
            if (debug) log.debug("Searching "+baseDn+" in "+entryMapping.getDn());

            int rc = aclManager.checkSearch(session, partition, entryMapping, baseDn);

            if (rc != LDAPException.SUCCESS) {
                if (debug) log.debug("Not allowed to search "+baseDn);
                continue;
            }

            Runnable runnable = new Runnable() {
                public void run() {
                    try {
                        Handler handler = getHandler(entryMapping);

                        handler.search(
                                session,
                                partition,
                                entryMapping,
                                request,
                                sr
                        );

                        sr.setResult(entryMapping, ExceptionUtil.createLDAPException(LDAPException.SUCCESS));

                    } catch (Exception e) {
                        log.error(e.getMessage(), e);
                        sr.setResult(entryMapping, ExceptionUtil.createLDAPException(e));

                    } finally {
                        try { sr.close(); } catch (Exception e) { log.error(e.getMessage(), e); }
                    }
                }
            };

            threadManager.execute(runnable);
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // UNBIND
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void unbind(
            Session session,
            Partition partition,
            UnbindRequest request,
            UnbindResponse response
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        DN bindDn = request.getDn();

        Collection entryMappings = partition.findEntryMappings(bindDn);

        for (Iterator i=entryMappings.iterator(); i.hasNext(); ) {
            EntryMapping entryMapping = (EntryMapping)i.next();
            if (debug) log.debug("Unbinding "+bindDn+" from "+entryMapping.getDn());

            Handler handler = getHandler(entryMapping);
            handler.unbind(session, partition, entryMapping, request, response);
        }
    }

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

    public void removeAttributes(Entry entry, Collection list) throws Exception {
        AttributeValues attributeValues = entry.getAttributeValues();
        for (Iterator i=list.iterator(); i.hasNext(); ) {
            String attributeName = (String)i.next();
            attributeValues.remove(attributeName);
        }
    }

    public Collection filterAttributes(
            Session session,
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
            log.debug("Attribute names: "+attributeNames);
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
            Session session,
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
        aclManager.getReadableAttributes(bindDn, partition, entryMapping, targetDn, null, attributeNames, grants, denies);

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
}
