/**
 * Copyright (c) 2000-2005, Identyx Corporation.
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

import org.safehaus.penrose.session.PenroseSearchResults;
import org.safehaus.penrose.session.PenroseSession;
import org.safehaus.penrose.session.PenroseSearchControls;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.util.EntryUtil;
import org.safehaus.penrose.util.ExceptionUtil;
import org.safehaus.penrose.schema.SchemaManager;
import org.safehaus.penrose.schema.AttributeType;
import org.safehaus.penrose.service.ServiceConfig;
import org.safehaus.penrose.config.PenroseConfig;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.engine.Engine;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.PartitionManager;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.filter.FilterTool;
import org.safehaus.penrose.mapping.*;
import org.ietf.ldap.*;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import javax.naming.directory.SearchResult;
import javax.naming.directory.Attributes;
import javax.naming.directory.Attribute;
import javax.naming.NamingEnumeration;
import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class SearchHandler {

    Logger log = LoggerFactory.getLogger(getClass());

    private Handler handler;

    public SearchHandler(Handler handler) {
        this.handler = handler;
    }

    /**
     *
     * @param session
     * @param baseDn
     * @param filter
     * @param sc
     * @param results This will be filled with objects of type Entry.
     * @return
     * @throws Exception
     */
    public int search(
            final PenroseSession session,
            final String baseDn,
            final String filter,
            final PenroseSearchControls sc,
            final PenroseSearchResults results
    ) throws Exception {

        handler.getEngine().getThreadManager().execute(new Runnable() {
            public void run() {
                try {
                    searchInBackground(session, baseDn, filter, sc, results);

                } catch (Throwable e) {
                    log.error(e.getMessage(), e);
                    results.setReturnCode(ExceptionUtil.getReturnCode(e));
                    results.close();
                }
            }
        });

        return LDAPException.SUCCESS;
    }

    public String getNormalizedAttributeName(String attributeName) {

        SchemaManager schemaManager = handler.getSchemaManager();

        AttributeType attributeType = schemaManager.getAttributeType(attributeName);

        String newAttributeName = attributeName;
        if (attributeType != null) newAttributeName = attributeType.getName();

        log.debug("Normalized attribute "+attributeName+" => "+newAttributeName);

        return newAttributeName;
    }

    public String normalizeDn(String dn) {
        String newDn = "";

        while (dn != null) {
            Row rdn = EntryUtil.getRdn(dn);
            String parentDn = EntryUtil.getParentDn(dn);

            Row newRdn = new Row();
            for (Iterator i=rdn.getNames().iterator(); i.hasNext(); ) {
                String name = (String)i.next();
                Object value = rdn.get(name);

                newRdn.set(getNormalizedAttributeName(name), value);
            }

            log.debug("Normalized rdn "+rdn+" => "+newRdn);

            newDn = EntryUtil.append(newDn, newRdn.toString());
            dn = parentDn;
        }

        return newDn;
    }

    public Collection normalizeAttributeNames(Collection attributeNames) {
        if (attributeNames == null) return null;

        Collection list = new ArrayList();
        for (Iterator i = attributeNames.iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            list.add(getNormalizedAttributeName(name));
        }

        return list;
    }

    public int searchInBackground(
            PenroseSession session,
            String baseDn,
            String filter,
            PenroseSearchControls sc,
            PenroseSearchResults results) throws Exception {

        int scope = sc.getScope();
        int deref = sc.getDereference();
        Collection attributeNames = sc.getAttributes();

        int rc;
        try {
            String s = null;
            switch (scope) {
            case PenroseSearchControls.SCOPE_BASE:
                s = "base";
                break;
            case PenroseSearchControls.SCOPE_ONE:
                s = "one level";
                break;
            case PenroseSearchControls.SCOPE_SUB:
                s = "subtree";
                break;
            }

            String d = null;
            switch (deref) {
            case PenroseSearchControls.DEREF_NEVER:
                d = "never";
                break;
            case PenroseSearchControls.DEREF_SEARCHING:
                d = "searching";
                break;
            case PenroseSearchControls.DEREF_FINDING:
                d = "finding";
                break;
            case PenroseSearchControls.DEREF_ALWAYS:
                d = "always";
                break;
            }

            baseDn = normalizeDn(baseDn);

            attributeNames = normalizeAttributeNames(attributeNames);
            sc.setAttributes(attributeNames);

            log.warn("Search \""+baseDn +"\" with scope "+s+" and filter \""+filter+"\"");

            log.debug("----------------------------------------------------------------------------------");
            log.debug("SEARCH:");
            if (session != null && session.getBindDn() != null) log.debug(" - Bind DN: " + session.getBindDn());
            log.debug(" - Base DN: " + baseDn);
            log.debug(" - Scope: " + s);
            log.debug(" - Filter: "+filter);
            log.debug(" - Alias Dereferencing: " + d);
            log.debug(" - Attribute Names: " + attributeNames);
            log.debug("");

            if (session != null && session.getBindDn() == null) {
                PenroseConfig penroseConfig = handler.getPenroseConfig();
                ServiceConfig serviceConfig = penroseConfig.getServiceConfig("LDAP");
                s = serviceConfig == null ? null : serviceConfig.getParameter("allowAnonymousAccess");
                boolean allowAnonymousAccess = s == null ? true : new Boolean(s).booleanValue();
                if (!allowAnonymousAccess) {
                    results.setReturnCode(LDAPException.INSUFFICIENT_ACCESS_RIGHTS);
                    return LDAPException.INSUFFICIENT_ACCESS_RIGHTS;
                }
            }

            rc = performSearch(session, baseDn, filter, sc, results);

        } catch (LDAPException e) {
            rc = e.getResultCode();
            results.setReturnCode(rc);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            rc = ExceptionUtil.getReturnCode(e);
            results.setReturnCode(rc);

        } finally {
            results.close();
        }

        if (rc == LDAPException.SUCCESS) {
            log.warn("Search operation succeded.");
        } else {
            log.warn("Search operation failed. RC="+rc);
        }

        return rc;
    }

    /**
     * @param results of Entries
     */
    public int performSearch(
            PenroseSession session,
            String base,
            String filter,
            PenroseSearchControls sc,
            PenroseSearchResults results) throws Exception {

        int scope = sc.getScope();
        Collection attributeNames = sc.getAttributes();

        String nbase;
        try {
            nbase = LDAPDN.normalize(base);
            if (nbase == null) nbase = "";
        } catch (IllegalArgumentException e) {
            results.setReturnCode(LDAPException.INVALID_DN_SYNTAX);
            return LDAPException.INVALID_DN_SYNTAX;
        }

		List path = handler.getFindHandler().findPath(session, nbase);

		if (path == null) {
            log.debug("Entry \""+nbase+"\" not found.");

            if ("".equals(base) && scope == LDAPConnection.SCOPE_BASE) { // finding root DSE
                log.debug("Creating default Root DSE");

                Entry entry = new Entry("", null);
                AttributeValues attributeValues = entry.getAttributeValues();
                attributeValues.set("objectClass", "top");
                attributeValues.add("objectClass", "extensibleObject");
                attributeValues.set("vendorName", Penrose.VENDOR_NAME);
                attributeValues.set("vendorVersion", Penrose.PRODUCT_NAME+" "+Penrose.PRODUCT_VERSION);

                for (Iterator i=handler.getPartitionManager().getPartitions().iterator(); i.hasNext(); ) {
                    Partition partition = (Partition)i.next();
                    for (Iterator j=partition.getRootEntryMappings().iterator(); j.hasNext(); ) {
                        EntryMapping entryMapping = (EntryMapping)j.next();
                        if ("".equals(entryMapping.getDn())) continue;
                        attributeValues.add("namingContexts", entryMapping.getDn());
                    }
                }

                if (!attributeNames.isEmpty() && !attributeNames.contains("*")) {
                    attributeValues.retain(attributeNames);
                }

                results.add(entry);
                results.close();

                return LDAPException.SUCCESS;
            }

			log.debug("Can't find base entry " + nbase);
			results.setReturnCode(LDAPException.NO_SUCH_OBJECT);
			return LDAPException.NO_SUCH_OBJECT;
		}

        //Map map = (Map)path.iterator().next();
        //Entry baseEntry = (Entry)map.get("entry");
        Entry baseEntry = (Entry)path.iterator().next();

        log.debug("Found base entry: " + baseEntry.getDn());
        EntryMapping entryMapping = baseEntry.getEntryMapping();

        PartitionManager partitionManager = handler.getPartitionManager();
        Partition partition = partitionManager.getPartition(entryMapping);

        Engine engine = handler.getEngine();
        AttributeValues parentSourceValues = new AttributeValues();
        engine.getParentSourceValues(path, entryMapping, parentSourceValues);

        int rc = handler.getACLEngine().checkSearch(session, baseEntry.getDn(), entryMapping);
        if (rc != LDAPException.SUCCESS) {
            log.debug("Checking search permission => FAILED");
            return rc;
        }

        Filter f = FilterTool.parseFilter(filter);
        log.debug("Parsed filter: "+f+" ("+f.getClass().getName()+")");

        if (partition.isProxy(entryMapping)) {

            if (scope == LDAPConnection.SCOPE_BASE) {
                if (handler.getFilterTool().isValid(baseEntry, f)) {

                    Entry e = baseEntry;

                    if (!attributeNames.isEmpty() && !attributeNames.contains("*")) {
                        AttributeValues av = new AttributeValues();
                        av.add(baseEntry.getAttributeValues());
                        av.retain(attributeNames);
                        e = new Entry(baseEntry.getDn(), entryMapping, baseEntry.getSourceValues(), av);
                    }

                    results.add(e);
                }

            } else {
                handler.getEngine().searchProxy(session, partition, entryMapping, base, filter, sc, results);
            }

        } else { // not a proxy

            if (scope == LDAPConnection.SCOPE_BASE || scope == LDAPConnection.SCOPE_SUB) {
                if (handler.getFilterTool().isValid(baseEntry, f)) {

                    Entry e = baseEntry;

                    if (!attributeNames.isEmpty() && !attributeNames.contains("*")) {
                        AttributeValues av = new AttributeValues();
                        av.add(baseEntry.getAttributeValues());
                        av.retain(attributeNames);
                        e = new Entry(baseEntry.getDn(), entryMapping, baseEntry.getSourceValues(), av);
                    }

                    results.add(e);
                }
            }

            if (scope == LDAPConnection.SCOPE_ONE || scope == LDAPConnection.SCOPE_SUB) { // one level or subtree
                log.debug("Searching children of \""+entryMapping.getDn()+"\"");
                searchChildren(session, path, entryMapping, parentSourceValues, scope, f, attributeNames, results);
            }
        }

		//results.setReturnCode(LDAPException.SUCCESS);
		return LDAPException.SUCCESS;
	}

    public void searchChildren(
            PenroseSession session,
            Collection path,
            EntryMapping entryMapping,
            AttributeValues parentSourceValues,
            int scope,
            Filter filter,
            Collection attributeNames,
            PenroseSearchResults results) throws Exception {

        PartitionManager partitionManager = handler.getPartitionManager();
        Partition partition = partitionManager.getPartition(entryMapping);
        Collection children = partition.getChildren(entryMapping);

        for (Iterator i = children.iterator(); i.hasNext();) {
            EntryMapping childMapping = (EntryMapping) i.next();
            log.info("Search child mapping \""+childMapping.getDn()+"\":");

            PenroseSearchResults sr = new PenroseSearchResults();

            PenroseSearchControls sc = new PenroseSearchControls();
            sc.setAttributes(attributeNames);

            if (partition.isProxy(childMapping)) {
                sc.setScope(scope == LDAPConnection.SCOPE_ONE ? LDAPConnection.SCOPE_BASE : scope);

                handler.getEngine().searchProxy(session, partition, childMapping, childMapping.getDn(), filter.toString(), sc, sr);

            } else if (handler.getFilterTool().isValid(childMapping, filter)) {

                handler.getEngine().search(
                        path,
                        parentSourceValues,
                        childMapping,
                        false,
                        filter,
                        sc,
                        sr
                );
            } else {
                sr.close();
            }

            log.debug("Processing search results:");

            // check each result agains acl, filter, and attribute list
            while (sr.hasNext()) {
                Entry child = (Entry)sr.next();

                int rc = handler.getACLEngine().checkSearch(session, child.getDn(), child.getEntryMapping());
                if (rc != LDAPException.SUCCESS) continue;

                Entry e = checkEntry(session, child, filter, sc);
                if (e == null) continue;

                results.add(e);
            }

            log.debug("Done processing search results:");

            int rc = sr.getReturnCode();

            log.debug("RC: "+rc);

            if (rc != LDAPException.SUCCESS) {
                log.debug("RC: "+rc);
                results.setReturnCode(rc);
                continue;
            }

            if (scope == LDAPConnection.SCOPE_SUB) {
                log.debug("Searching children of " + childMapping.getDn());

                AttributeValues newParentSourceValues = new AttributeValues();
                for (Iterator j=parentSourceValues.getNames().iterator(); j.hasNext(); ) {
                    String name = (String)j.next();
                    Collection values = parentSourceValues.get(name);

                    if (name.startsWith("parent.")) name = "parent."+name;

                    newParentSourceValues.add(name, values);
                }

                Engine engine = handler.getEngine();
                Interpreter interpreter = engine.getInterpreterFactory().newInstance();

                AttributeValues av = engine.computeAttributeValues(childMapping, interpreter);
                for (Iterator j=av.getNames().iterator(); j.hasNext(); ) {
                    String name = (String)j.next();
                    Collection values = av.get(name);

                    name = "parent."+name;
                    newParentSourceValues.add(name, values);
                }

                interpreter.clear();

                for (Iterator j=newParentSourceValues.getNames().iterator(); j.hasNext(); ) {
                    String name = (String)j.next();
                    Collection values = newParentSourceValues.get(name);
                    log.debug(" - "+name+": "+values);
                }

                Collection newPath = new ArrayList();
                newPath.add(null);
                newPath.addAll(path);

                searchChildren(session, newPath, childMapping, newParentSourceValues, scope, filter, attributeNames, results);
            }
        }
    }

    public Entry checkEntry(PenroseSession session, Entry entry, Filter filter, PenroseSearchControls sc) throws Exception {
        if (!handler.getFilterTool().isValid(entry, filter)) return null;

        Entry newEntry = entry;

        if (!sc.getAttributes().isEmpty() && !sc.getAttributes().contains("*")) {
            AttributeValues av = new AttributeValues();
            av.add(entry.getAttributeValues());
            av.retain(sc.getAttributes());

            newEntry = new Entry(entry.getDn(), entry.getEntryMapping(), entry.getSourceValues(), av);
        }

        return newEntry;
    }

    public SearchResult createSearchResult(
            PenroseSession session,
            Entry entry)
            throws Exception {

        log.debug("Converting Entry to SearchResult: "+entry.getDn());

        int rc = handler.getACLEngine().checkRead(session, entry.getDn(), entry.getEntryMapping());
        if (rc != LDAPException.SUCCESS) return null;

        SchemaManager schemaManager = handler.getSchemaManager();
        //log.debug("Schema manager: "+schemaManager);

        String bindDn = schemaManager.normalize(session == null ? null : session.getBindDn());
        String targetDn = schemaManager.normalize(entry.getDn());

        EntryMapping entryMapping = entry.getEntryMapping();

        SearchResult sr = EntryUtil.toSearchResult(entry);
        Attributes attributes = sr.getAttributes();

        //log.debug("Evaluating attributes read permission for "+bindDn);

        Set grants = new HashSet();
        Set denies = new HashSet();

        Collection attributeNames = new ArrayList();
        for (NamingEnumeration i=attributes.getAll(); i.hasMore(); ) {
            Attribute attribute = (Attribute)i.next();
            attributeNames.add(attribute.getID());
        }

        handler.getACLEngine().getReadableAttributes(bindDn, targetDn, entryMapping, attributeNames, grants, denies);

        //log.debug("Readable attributes: "+grants);
        //log.debug("Unreadable attributes: "+denies);

        Collection list = new ArrayList();
        for (NamingEnumeration i=attributes.getAll(); i.hasMore(); ) {
            Attribute attribute = (Attribute)i.next();
            //if (checkAttributeReadPermission(bindDn, targetDn, entryMapping, attribute.getName())) continue;
            if (grants.contains(attribute.getID())) continue;
            list.add(attribute);
        }

        for (Iterator i=list.iterator(); i.hasNext(); ) {
            Attribute attribute = (Attribute)i.next();
            attributes.remove(attribute.getID());
        }

        return sr;
    }
}
