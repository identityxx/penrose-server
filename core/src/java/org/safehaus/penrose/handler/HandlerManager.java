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
import org.safehaus.penrose.mapping.EntryMapping;
import org.safehaus.penrose.schema.SchemaManager;
import org.safehaus.penrose.schema.AttributeType;
import org.safehaus.penrose.schema.ObjectClass;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.thread.ThreadManager;
import org.safehaus.penrose.acl.ACLManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class HandlerManager {

    public Logger log = LoggerFactory.getLogger(getClass());
    public boolean debug = log.isDebugEnabled();

    public final static DN ROOT_DSE_DN = new DN("");
    public final static DN SCHEMA_DN   = new DN("cn=Subschema");

    Map<String,Handler> handlers = new TreeMap<String,Handler>();

    PenroseConfig penroseConfig;
    PenroseContext penroseContext;
    SessionContext sessionContext;

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
    }

    public SessionContext getSessionContext() {
        return sessionContext;
    }

    public void setSessionContext(SessionContext sessionContext) {
        this.sessionContext = sessionContext;

        aclManager = sessionContext.getAclManager();
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
        handler.setSessionContext(sessionContext);
        handler.init(handlerConfig);

        handlers.put(handlerConfig.getName(), handler);
    }

    public Handler getHandler(String name) {
        return handlers.get(name);
    }

    public Handler getHandler(Partition partition, EntryMapping entryMapping) {
        String handlerName = entryMapping.getHandlerName();
        if (handlerName != null) return handlers.get(handlerName);

        handlerName = partition.getHandlerName();
        if (handlerName != null) return handlers.get(handlerName);

        return handlers.get("DEFAULT");
    }

    public Handler getHandler(Partition partition) {
        String handlerName = partition.getHandlerName();
        if (handlerName != null) return handlers.get(handlerName);

        return handlers.get("DEFAULT");
    }
    
    public void clear() {
        handlers.clear();
    }

    public void start() throws Exception {
        for (Handler handler : handlers.values()) {
            handler.start();
        }
    }

    public void stop() throws Exception {
        for (Handler handler : handlers.values()) {
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

        DN dn = schemaManager.normalize(request.getDn());
        request.setDn(dn);

        Attributes attributes = schemaManager.normalize(request.getAttributes());
        request.setAttributes(attributes);

        DN parentDn = dn.getParentDn();

        Collection<EntryMapping> entryMappings = partition.getMappings().findEntryMappings(dn);
        Exception exception = null;

        for (EntryMapping entryMapping : entryMappings) {
            if (debug) log.debug("Adding " + dn + " into " + entryMapping.getDn());

            EntryMapping parentMapping = partition.getMappings().getParent(entryMapping);
            int rc = aclManager.checkAdd(session, partition, parentMapping, parentDn);

            if (rc != LDAP.SUCCESS) {
                if (debug) log.debug("Not allowed to add " + dn);
                exception = LDAP.createException(LDAP.INSUFFICIENT_ACCESS_RIGHTS);
                continue;
            }

            try {
                Handler handler = getHandler(partition, entryMapping);
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

        DN dn = schemaManager.normalize(request.getDn());
        request.setDn(dn);

        Collection<EntryMapping> entryMappings = partition.getMappings().findEntryMappings(dn);

        for (EntryMapping entryMapping : entryMappings) {
            if (debug) log.debug("Binding " + dn + " in " + entryMapping.getDn());

            Handler handler = getHandler(partition, entryMapping);
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

        DN dn = schemaManager.normalize(request.getDn());
        request.setDn(dn);

        String attributeName = schemaManager.normalizeAttributeName(request.getAttributeName());
        request.setAttributeName(attributeName);

        Collection<EntryMapping> entryMappings = partition.getMappings().findEntryMappings(dn);
        Exception exception = null;

        for (EntryMapping entryMapping : entryMappings) {
            if (debug) log.debug("Comparing " + dn + " in " + entryMapping.getDn());

            int rc = aclManager.checkRead(session, partition, entryMapping, dn);

            if (rc != LDAP.SUCCESS) {
                if (debug) log.debug("Not allowed to compare " + dn);
                exception = LDAP.createException(LDAP.INSUFFICIENT_ACCESS_RIGHTS);
                continue;
            }

            try {
                Handler handler = getHandler(partition, entryMapping);
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

        DN dn = schemaManager.normalize(request.getDn());
        request.setDn(dn);

        Collection<EntryMapping> entryMappings = partition.getMappings().findEntryMappings(dn);
        Exception exception = null;

        for (EntryMapping entryMapping : entryMappings) {
            if (debug) log.debug("Deleting " + dn + " from " + entryMapping.getDn());

            int rc = aclManager.checkDelete(session, partition, entryMapping, dn);

            if (rc != LDAP.SUCCESS) {
                if (debug) log.debug("Not allowed to delete " + dn);
                exception = LDAP.createException(LDAP.INSUFFICIENT_ACCESS_RIGHTS);
                continue;
            }

            try {
                Handler handler = getHandler(partition, entryMapping);
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

        DN dn = schemaManager.normalize(request.getDn());
        request.setDn(dn);

        Collection<Modification> modifications = schemaManager.normalizeModifications(request.getModifications());
        request.setModifications(modifications);

        Collection<EntryMapping> entryMappings = partition.getMappings().findEntryMappings(dn);
        Exception exception = null;

        for (EntryMapping entryMapping : entryMappings) {
            if (debug) log.debug("Modifying " + dn + " in " + entryMapping.getDn());

            int rc = aclManager.checkModify(session, partition, entryMapping, dn);

            if (rc != LDAP.SUCCESS) {
                if (debug) log.debug("Not allowed to modify " + dn);
                exception = LDAP.createException(LDAP.INSUFFICIENT_ACCESS_RIGHTS);
                continue;
            }

            try {
                Handler handler = getHandler(partition, entryMapping);
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

        DN dn = schemaManager.normalize(request.getDn());
        request.setDn(dn);

        RDN newRdn = schemaManager.normalize(request.getNewRdn());
        request.setNewRdn(newRdn);

        Collection<EntryMapping> entryMappings = partition.getMappings().findEntryMappings(dn);
        Exception exception = null;

        for (EntryMapping entryMapping : entryMappings) {
            if (debug) log.debug("Renaming " + dn + " in " + entryMapping.getDn());

            int rc = aclManager.checkModify(session, partition, entryMapping, dn);

            if (rc != LDAP.SUCCESS) {
                if (debug) log.debug("Not allowed to modify " + dn);
                exception = LDAP.createException(LDAP.INSUFFICIENT_ACCESS_RIGHTS);
                continue;
            }

            try {
                Handler handler = getHandler(partition, entryMapping);
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
            final SearchResponse<SearchResult> response
    ) throws Exception {

        DN baseDn = request.getDn();
        Collection<String> requestedAttributes = request.getAttributes();

        boolean allRegularAttributes = requestedAttributes.isEmpty() || requestedAttributes.contains("*");
        boolean allOpAttributes = requestedAttributes.contains("+");

        if (debug) log.debug("Requested: "+request.getAttributes());

        if (partition == null) {

            if (baseDn.matches(HandlerManager.ROOT_DSE_DN) && request.getScope() == SearchRequest.SCOPE_BASE) {

                SearchResult result = createRootDSE();
                Attributes attrs = result.getAttributes();
                if (debug) {
                    log.debug("Before: "+result.getDn());
                    attrs.print();
                }

                Collection<String> list = filterAttributes(session, partition, result, requestedAttributes, allRegularAttributes, allOpAttributes);
                removeAttributes(attrs, list);

                if (debug) {
                    log.debug("After: "+result.getDn());
                    attrs.print();
                }

                response.add(result);

            } else if (baseDn.matches(HandlerManager.SCHEMA_DN)) {

                SearchResult result = createSchema();
                Attributes attrs = result.getAttributes();
                if (debug) {
                    log.debug("Before: "+result.getDn());
                    attrs.print();
                }

                Collection<String> list = filterAttributes(session, partition, result, requestedAttributes, allRegularAttributes, allOpAttributes);
                removeAttributes(attrs, list);

                if (debug) {
                    log.debug("After: "+result.getDn());
                    attrs.print();
                }

                response.add(result);

            } else {
                response.setException(LDAP.createException(LDAP.NO_SUCH_OBJECT));
            }

            response.close();
            return;
        }

        Collection<EntryMapping> entryMappings = partition.getMappings().findEntryMappings(baseDn);

        if (entryMappings.isEmpty()) {
            if (debug) log.debug("Base DN "+baseDn+" not found.");
            throw LDAP.createException(LDAP.NO_SUCH_OBJECT);
        }

        final HandlerSearchResponse sr = new HandlerSearchResponse(
                response,
                session,
                partition,
                this,
                aclManager,
                requestedAttributes,
                allRegularAttributes,
                allOpAttributes,
                entryMappings
        );

        for (final EntryMapping entryMapping : entryMappings) {
            if (debug) log.debug("Searching " + baseDn + " in " + entryMapping.getDn());

            int rc = aclManager.checkSearch(session, partition, entryMapping, baseDn);

            if (rc != LDAP.SUCCESS) {
                if (debug) log.debug("Not allowed to search " + baseDn);
                sr.setResult(entryMapping, LDAP.createException(LDAP.INSUFFICIENT_ACCESS_RIGHTS));
                sr.close();
                continue;
            }

            Runnable runnable = new Runnable() {
                public void run() {
                    try {
                        Handler handler = getHandler(partition, entryMapping);

                        handler.search(
                                session,
                                partition,
                                entryMapping,
                                request,
                                sr
                        );

                        sr.setResult(entryMapping, LDAP.createException(LDAP.SUCCESS));

                    } catch (Exception e) {
                        log.error(e.getMessage(), e);
                        sr.setResult(entryMapping, LDAP.createException(e));

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
    // UNBIND
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void unbind(
            Session session,
            Partition partition,
            UnbindRequest request,
            UnbindResponse response
    ) throws Exception {

        DN bindDn = request.getDn();

        Collection<EntryMapping> entryMappings = partition.getMappings().findEntryMappings(bindDn);

        for (EntryMapping entryMapping : entryMappings) {
            if (debug) log.debug("Unbinding " + bindDn + " from " + entryMapping.getDn());

            Handler handler = getHandler(partition, entryMapping);
            handler.unbind(session, partition, entryMapping, request, response);
        }
    }

    public SearchResult createRootDSE() throws Exception {

        Attributes attributes = new Attributes();
        attributes.addValue("objectClass", "top");
        attributes.addValue("objectClass", "extensibleObject");
        attributes.addValue("vendorName", Penrose.VENDOR_NAME);
        attributes.addValue("vendorVersion", Penrose.PRODUCT_NAME+" "+Penrose.PRODUCT_VERSION);
        attributes.addValue("supportedLDAPVersion", "3");
        attributes.addValue("subschemaSubentry", SCHEMA_DN.toString());

        PartitionManager partitionManager = penroseContext.getPartitionManager();
        for (Partition partition : partitionManager.getPartitions()) {
            for (EntryMapping entryMapping : partition.getMappings().getRootEntryMappings()) {
                if (entryMapping.getDn().isEmpty()) continue;
                attributes.addValue("namingContexts", entryMapping.getDn().toString());
            }
        }

        return new SearchResult(ROOT_DSE_DN, attributes);
    }

    public SearchResult createSchema() throws Exception {

        Attributes attributes = new Attributes();
        attributes.addValue("objectClass", "top");
        attributes.addValue("objectClass", "subentry");
        attributes.addValue("objectClass", "subschema");
        attributes.addValue("objectClass", "extensibleObject");

        for (AttributeType attributeType : schemaManager.getAttributeTypes()) {
            attributes.addValue("attributeTypes", "( "+attributeType+" )");
        }

        for (ObjectClass objectClass : schemaManager.getObjectClasses()) {
            attributes.addValue("objectClasses", "( "+objectClass+" )");
        }

        return new SearchResult(SCHEMA_DN, attributes);
    }

    public void removeAttributes(Attributes attributes, Collection<String> list) throws Exception {
        for (String attributeName : list) {
            attributes.remove(attributeName);
        }
    }

    public Collection<String> filterAttributes(
            Session session,
            Partition partition,
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
            Partition partition,
            DN dn,
            EntryMapping entryMapping,
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
        aclManager.getReadableAttributes(bindDn, partition, entryMapping, dn, null, attributeNames, grants, denies);

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
}
