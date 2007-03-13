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
import org.safehaus.penrose.session.PenroseSession;
import org.safehaus.penrose.session.PenroseSearchControls;
import org.safehaus.penrose.session.Results;
import org.safehaus.penrose.entry.DN;
import org.safehaus.penrose.entry.Entry;
import org.safehaus.penrose.entry.AttributeValues;
import org.safehaus.penrose.entry.RDN;
import org.safehaus.penrose.util.*;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.filter.FilterTool;
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

import javax.naming.directory.*;
import javax.naming.NamingEnumeration;
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

    public void bind(
            PenroseSession session,
            Partition partition,
            DN dn,
            String password
    ) throws Exception {

        if (log.isDebugEnabled()) {
            log.debug("----------------------------------------------------------------------------------");
            log.debug("BIND:");
            log.debug(" - Bind DN: "+dn);
            log.debug(" - Bind Password: "+password);
            log.debug("");
        }

        Collection entryMappings = partition.findEntryMappings(dn);

        for (Iterator i=entryMappings.iterator(); i.hasNext(); ) {
            EntryMapping entryMapping = (EntryMapping)i.next();

            Handler handler = getHandler(entryMapping);
            handler.bind(session, partition, entryMapping, dn, password);
        }
    }

    public void unbind(
            PenroseSession session,
            Partition partition,
            DN bindDn
    ) throws Exception {

        Collection entryMappings = partition.findEntryMappings(bindDn);

        for (Iterator i=entryMappings.iterator(); i.hasNext(); ) {
            EntryMapping entryMapping = (EntryMapping)i.next();

            Handler handler = getHandler(entryMapping);
            handler.unbind(session, partition, entryMapping, bindDn);
        }
    }

    public void add(
            PenroseSession session,
            Partition partition,
            DN dn,
            Attributes attributes
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        if (log.isWarnEnabled()) {
            log.warn("Add entry \""+dn+"\".");
        }
        
        if (debug) {
            log.debug("----------------------------------------------------------------------------------");
            log.debug("ADD:");
            if (session != null && session.getBindDn() != null) log.debug(" - Bind DN: "+session.getBindDn());
            log.debug(" - Entry: "+dn);
            log.debug("");
        }

        DN parentDn = dn.getParentDn();
        attributes = schemaManager.normalize(attributes);

        if (debug) log.debug("Adding entry "+dn);

        Attributes normalizedAttributes = new BasicAttributes();

        for (NamingEnumeration ne = attributes.getAll(); ne.hasMore(); ) {
            Attribute attribute = (Attribute)ne.next();

            String attributeName = attribute.getID();
            String normalizedAttributeName = attributeName;

            AttributeType at = schemaManager.getAttributeType(attributeName);
            if (debug) log.debug("Attribute "+attributeName+": "+at);
            if (at != null) {
                normalizedAttributeName = at.getName();
            }

            Attribute normalizedAttribute = new BasicAttribute(normalizedAttributeName);
            for (NamingEnumeration j=attribute.getAll(); j.hasMore(); ) {
                Object value = j.next();
                normalizedAttribute.add(value);
            }

            normalizedAttributes.put(normalizedAttribute);
        }

        if (debug) {
            log.debug("Original attributes:");
            for (NamingEnumeration ne = attributes.getAll(); ne.hasMore(); ) {
                Attribute attribute = (Attribute)ne.next();
                String attributeName = attribute.getID();

                for (NamingEnumeration j=attribute.getAll(); j.hasMore(); ) {
                    Object value = j.next();
                    log.debug(" - "+attributeName+": "+value);
                }
            }

            log.debug("Normalized attributes:");
            for (NamingEnumeration ne = normalizedAttributes.getAll(); ne.hasMore(); ) {
                Attribute attribute = (Attribute)ne.next();
                String attributeName = attribute.getID();

                for (NamingEnumeration j=attribute.getAll(); j.hasMore(); ) {
                    Object value = j.next();
                    log.debug(" - "+attributeName+": "+value);
                }
            }
        }

        attributes = normalizedAttributes;

        Collection entryMappings = partition.findEntryMappings(dn);
        Exception exception = null;

        for (Iterator i=entryMappings.iterator(); i.hasNext(); ) {
            EntryMapping entryMapping = (EntryMapping)i.next();

            EntryMapping parentMapping = partition.getParent(entryMapping);
            int rc = aclManager.checkAdd(session, partition, parentMapping, parentDn);

            if (rc != LDAPException.SUCCESS) {
                log.debug("Not allowed to add "+dn);
                exception = ExceptionUtil.createLDAPException(LDAPException.INSUFFICIENT_ACCESS_RIGHTS);
                continue;
            }

            try {
                Handler handler = getHandler(entryMapping);
                handler.add(session, partition, entryMapping, dn, attributes);
                return;
            } catch (Exception e) {
                exception = e;
            }
        }

        throw exception;
    }

    public boolean compare(
            PenroseSession session,
            Partition partition,
            DN dn,
            String attributeName,
            Object attributeValue
    ) throws Exception {

        if (log.isWarnEnabled()) {
            log.warn("Compare attribute "+attributeName+" in \""+dn+"\" with \""+attributeValue+"\".");
        }
        if (log.isDebugEnabled()) {
            log.debug("----------------------------------------------------------------------------------");
            log.debug("COMPARE:");
            if (session != null && session.getBindDn() != null) log.debug(" - Bind DN: " + session.getBindDn());
            log.debug(" - DN: " + dn);
            log.debug(" - Attribute Name: " + attributeName);
            if (attributeValue instanceof byte[]) {
                log.debug(" - Attribute Value: " + BinaryUtil.encode(BinaryUtil.BIG_INTEGER, (byte[])attributeValue));
            } else {
                log.debug(" - Attribute Value: " + attributeValue);
            }
        }

        Collection entryMappings = partition.findEntryMappings(dn);
        Exception exception = null;

        for (Iterator i=entryMappings.iterator(); i.hasNext(); ) {
            EntryMapping entryMapping = (EntryMapping)i.next();

            int rc = aclManager.checkRead(session, partition, entryMapping, dn);

            if (rc != LDAPException.SUCCESS) {
                log.debug("Not allowed to compare "+dn);
                exception = ExceptionUtil.createLDAPException(LDAPException.INSUFFICIENT_ACCESS_RIGHTS);
                continue;
            }

            try {
                Handler handler = getHandler(entryMapping);
                return handler.compare(session, partition, entryMapping, dn, attributeName, attributeValue);
            } catch (Exception e) {
                exception = e;
            }
        }

        throw exception;
    }

    public void delete(
            PenroseSession session,
            Partition partition,
            DN dn
    ) throws Exception {

        if (log.isWarnEnabled()) {
            log.warn("Delete entry \""+dn+"\".");
        }
        if (log.isDebugEnabled()) {
            log.debug("----------------------------------------------------------------------------------");
            log.debug("DELETE:");
            if (session != null && session.getBindDn() != null) log.debug(" - Bind DN: "+session.getBindDn());
            log.debug(" - DN: "+dn);
            log.debug("");
        }

        Collection entryMappings = partition.findEntryMappings(dn);
        Exception exception = null;

        for (Iterator i=entryMappings.iterator(); i.hasNext(); ) {
            EntryMapping entryMapping = (EntryMapping)i.next();
            int rc = aclManager.checkDelete(session, partition, entryMapping, dn);

            if (rc != LDAPException.SUCCESS) {
                log.debug("Not allowed to delete "+dn);
                exception = ExceptionUtil.createLDAPException(LDAPException.INSUFFICIENT_ACCESS_RIGHTS);
                continue;
            }

            try {
                Handler handler = getHandler(entryMapping);
                handler.delete(session, partition, entryMapping, dn);
                return;
            } catch (Exception e) {
                exception = e;
            }
        }

        throw exception;
    }

    public void modify(
            PenroseSession session,
            Partition partition,
            DN dn,
            Collection modifications
    ) throws Exception {

        dn = schemaManager.normalize(dn);

        if (log.isWarnEnabled()) {
            log.warn("Modify entry \""+dn+"\".");
        }
        if (log.isDebugEnabled()) {
            log.debug("----------------------------------------------------------------------------------");
            log.debug(org.safehaus.penrose.util.Formatter.displaySeparator(80));
            log.debug(org.safehaus.penrose.util.Formatter.displayLine("MODIFY:", 80));
            if (session != null && session.getBindDn() != null) {
                log.debug(org.safehaus.penrose.util.Formatter.displayLine(" - Bind DN: " + session.getBindDn(), 80));
            }
            log.debug(org.safehaus.penrose.util.Formatter.displayLine(" - DN: " + dn, 80));

            log.debug(org.safehaus.penrose.util.Formatter.displayLine(" - Attributes: ", 80));
            for (Iterator i=modifications.iterator(); i.hasNext(); ) {
                ModificationItem mi = (ModificationItem)i.next();
                Attribute attribute = mi.getAttribute();
                String op = "replace";
                switch (mi.getModificationOp()) {
                    case DirContext.ADD_ATTRIBUTE:
                        op = "add";
                        break;
                    case DirContext.REMOVE_ATTRIBUTE:
                        op = "delete";
                        break;
                    case DirContext.REPLACE_ATTRIBUTE:
                        op = "replace";
                        break;
                }

                if (attribute.size() == 0) {
                    log.debug(org.safehaus.penrose.util.Formatter.displayLine("   - "+op+": "+attribute.getID()+" => "+null, 80));
                } else {
                    log.debug(org.safehaus.penrose.util.Formatter.displayLine("   - "+op+": "+attribute.getID()+" => "+attribute.get(), 80));
                }
            }

            log.debug(org.safehaus.penrose.util.Formatter.displaySeparator(80));
        }

        log.debug("Modifying "+dn);

        Collection normalizedModifications = new ArrayList();

        for (Iterator i = modifications.iterator(); i.hasNext();) {
            ModificationItem modification = (ModificationItem) i.next();

            Attribute attribute = modification.getAttribute();
            String attributeName = attribute.getID();

            AttributeType at = schemaManager.getAttributeType(attributeName);
            if (at != null) {
                attributeName = at.getName();
            }

            switch (modification.getModificationOp()) {
                case DirContext.ADD_ATTRIBUTE:
                    log.debug("add: " + attributeName);
                    break;
                case DirContext.REMOVE_ATTRIBUTE:
                    log.debug("delete: " + attributeName);
                    break;
                case DirContext.REPLACE_ATTRIBUTE:
                    log.debug("replace: " + attributeName);
                    break;
            }

            Attribute normalizedAttribute = new BasicAttribute(attributeName);
            for (NamingEnumeration j=attribute.getAll(); j.hasMore(); ) {
                Object value = j.next();
                normalizedAttribute.add(value);
                log.debug(attributeName + ": "+value);
            }

            log.debug("-");

            ModificationItem normalizedModification = new ModificationItem(modification.getModificationOp(), normalizedAttribute);
            normalizedModifications.add(normalizedModification);
        }

        modifications = normalizedModifications;

        log.debug("");

        Collection entryMappings = partition.findEntryMappings(dn);
        Exception exception = null;

        for (Iterator i=entryMappings.iterator(); i.hasNext(); ) {
            EntryMapping entryMapping = (EntryMapping)i.next();

            int rc = aclManager.checkModify(session, partition, entryMapping, dn);

            if (rc != LDAPException.SUCCESS) {
                log.debug("Not allowed to modify "+dn);
                exception = ExceptionUtil.createLDAPException(LDAPException.INSUFFICIENT_ACCESS_RIGHTS);
                continue;
            }

            try {
                Handler handler = getHandler(entryMapping);
                handler.modify(session, partition, entryMapping, dn, modifications);
                return;
            } catch (Exception e) {
                exception = e;
            }
        }

        throw exception;
    }

    public void modrdn(
            PenroseSession session,
            Partition partition,
            DN dn,
            RDN newRdn,
            boolean deleteOldRdn
    ) throws Exception {

        if (log.isWarnEnabled()) {
            log.warn("ModRDN \""+dn+"\" to \""+newRdn+"\".");
        }
        if (log.isDebugEnabled()) {
            log.debug("----------------------------------------------------------------------------------");
            log.debug(org.safehaus.penrose.util.Formatter.displaySeparator(80));
            log.debug(org.safehaus.penrose.util.Formatter.displayLine("MODRDN:", 80));
            if (session != null && session.getBindDn() != null) {
                log.debug(org.safehaus.penrose.util.Formatter.displayLine(" - Bind DN: " + session.getBindDn(), 80));
            }
            log.debug(org.safehaus.penrose.util.Formatter.displayLine(" - DN: " + dn, 80));
            log.debug(org.safehaus.penrose.util.Formatter.displayLine(" - New RDN: " + newRdn, 80));
            log.debug(org.safehaus.penrose.util.Formatter.displaySeparator(80));
        }

        Collection entryMappings = partition.findEntryMappings(dn);
        Exception exception = null;

        for (Iterator i=entryMappings.iterator(); i.hasNext(); ) {
            EntryMapping entryMapping = (EntryMapping)i.next();

            int rc = aclManager.checkModify(session, partition, entryMapping, dn);

            if (rc != LDAPException.SUCCESS) {
                log.debug("Not allowed to modify "+dn);
                exception = ExceptionUtil.createLDAPException(LDAPException.INSUFFICIENT_ACCESS_RIGHTS);
                continue;
            }

            try {
                Handler handler = getHandler(entryMapping);
                handler.modrdn(session, partition, entryMapping, dn, newRdn, deleteOldRdn);
                return;
            } catch (Exception e) {
                exception = e;
            }
        }

        throw exception;
    }

    public void search(
            final PenroseSession session,
            final Partition partition,
            final DN dn,
            final String filter,
            final PenroseSearchControls sc,
            final Results results
    ) throws Exception {

        final DN baseDn = schemaManager.normalize(dn);

        String scope = LDAPUtil.getScope(sc.getScope());

        Collection attributeNames = sc.getAttributes();
        attributeNames = schemaManager.normalize(attributeNames);
        sc.setAttributes(attributeNames);

        if (log.isWarnEnabled()) {
            log.warn("Search \""+baseDn +"\" with scope "+scope+" and filter \""+filter+"\"");
        }

        final boolean debug = log.isDebugEnabled();
        if (debug) {
            log.debug("----------------------------------------------------------------------------------");
            log.debug("SEARCH:");
            if (session != null && session.getBindDn() != null) log.debug(" - Bind DN: " + session.getBindDn());
            log.debug(" - Base DN: "+baseDn);
            log.debug(" - Scope: "+scope);
            log.debug(" - Filter: "+filter);
            log.debug(" - Attribute Names: "+attributeNames);
            log.debug("");
        }

        final Filter f = FilterTool.parseFilter(filter);

        final Set requestedAttributes = new HashSet();
        if (sc.getAttributes() != null) requestedAttributes.addAll(sc.getAttributes());

        final boolean allRegularAttributes = sc.getAttributes() == null || sc.getAttributes().isEmpty() || sc.getAttributes().contains("*");
        final boolean allOpAttributes = sc.getAttributes() != null && sc.getAttributes().contains("+");

        if (debug) log.debug("Requested: "+sc.getAttributes());

        if (baseDn.isEmpty()) {
            if (sc.getScope() == LDAPConnection.SCOPE_BASE) {
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

                results.add(entry);
            }
            results.close();
            return;
        }

        Collection entryMappings = partition.findEntryMappings(baseDn);

        if (entryMappings.isEmpty()) {
            if (debug) log.debug("Base DN "+baseDn+" not found.");
            throw ExceptionUtil.createLDAPException(LDAPException.NO_SUCH_OBJECT);
        }

        final SearchPipeline sr = new SearchPipeline(
                results,
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
            final Handler handler = getHandler(entryMapping);

            int rc = aclManager.checkSearch(session, partition, entryMapping, baseDn);

            if (rc != LDAPException.SUCCESS) {
                if (debug) log.debug("Not allowed to search "+baseDn);
                continue;
            }

            Runnable runnable = new Runnable() {
                public void run() {
                    try {
                        handler.search(
                                session,
                                partition,
                                entryMapping,
                                baseDn,
                                f,
                                sc,
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
