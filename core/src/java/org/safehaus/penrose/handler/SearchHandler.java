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
import org.safehaus.penrose.engine.Engine;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.pipeline.PipelineAdapter;
import org.safehaus.penrose.pipeline.PipelineEvent;
import org.safehaus.penrose.util.EntryUtil;
import org.safehaus.penrose.util.LDAPUtil;
import org.safehaus.penrose.schema.SchemaManager;
import org.safehaus.penrose.schema.AttributeType;
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
     * @return return code
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
                    results.setReturnCode(LDAPException.OPERATIONS_ERROR);

                } finally {
                    results.close();
                }
            }
        });

        return LDAPException.SUCCESS;
    }

    public int searchInBackground(
            PenroseSession session,
            String baseDn,
            String filter,
            PenroseSearchControls sc,
            PenroseSearchResults results) throws Exception {

        Collection attributeNames = sc.getAttributes();

        int rc;
        try {
            String scope = LDAPUtil.getScope(sc.getScope());
            baseDn = normalizeDn(baseDn);

            attributeNames = normalizeAttributeNames(attributeNames);
            sc.setAttributes(attributeNames);

            log.warn("Search \""+baseDn +"\" with scope "+scope+" and filter \""+filter+"\"");

            log.debug("----------------------------------------------------------------------------------");
            log.debug("SEARCH:");
            if (session != null && session.getBindDn() != null) log.debug(" - Bind DN: " + session.getBindDn());
            log.debug(" - Base DN: "+baseDn);
            log.debug(" - Scope: "+scope);
            log.debug(" - Filter: "+filter);
            log.debug(" - Attribute Names: "+attributeNames);
            log.debug("");

            rc = performSearch(session, baseDn, filter, sc, results);

        } catch (LDAPException e) {
            rc = e.getResultCode();
            results.setReturnCode(rc);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            rc = LDAPException.OPERATIONS_ERROR;
            results.setReturnCode(rc);
        }

        if (rc == LDAPException.SUCCESS) {
            log.warn("Search operation succeded.");
        } else {
            log.warn("Search operation failed. RC="+rc);
        }

        return rc;
    }

    public String getNormalizedAttributeName(String attributeName) {

        SchemaManager schemaManager = handler.getSchemaManager();

        AttributeType attributeType = schemaManager.getAttributeType(attributeName);

        String newAttributeName = attributeName;
        if (attributeType != null) newAttributeName = attributeType.getName();

        //log.debug("Normalized attribute "+attributeName+" => "+newAttributeName);

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

            //log.debug("Normalized rdn "+rdn+" => "+newRdn);

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

    /**
     * @param results of Entries
     */
    public int performSearch(
            final PenroseSession session,
            final String baseDn,
            final String filter,
            final PenroseSearchControls sc,
            final PenroseSearchResults results) throws Exception {

        String normalizedBaseDn;
        try {
            normalizedBaseDn = LDAPDN.normalize(baseDn);
            if (normalizedBaseDn == null) normalizedBaseDn = "";

        } catch (IllegalArgumentException e) {
            results.setReturnCode(LDAPException.INVALID_DN_SYNTAX);
            return LDAPException.INVALID_DN_SYNTAX;
        }

        if ("".equals(baseDn) && sc.getScope() == LDAPConnection.SCOPE_BASE) { // finding root DSE
            return searchRootDSE(session, baseDn, filter, sc, results);
        }

        PartitionManager partitionManager = handler.getPartitionManager();
        Partition partition = null;
        EntryMapping entryMapping = null;

        for (Iterator i=partitionManager.getPartitions().iterator(); i.hasNext(); ) {
            Partition p = (Partition)i.next();
            entryMapping = p.findEntryMapping(normalizedBaseDn);

            if (entryMapping == null) continue;

            partition = p;
            break;
        }

        if (partition == null) {
            log.debug("Entry \""+normalizedBaseDn +"\" not found.");

            log.debug("Can't find base entry "+normalizedBaseDn);
            results.setReturnCode(LDAPException.NO_SUCH_OBJECT);
            return LDAPException.NO_SUCH_OBJECT;
        }

        log.debug("Found entry mapping \""+entryMapping.getDn()+"\".");

		Collection path = handler.getFindHandler().findPath(session, partition, normalizedBaseDn);

        if (path == null || path.isEmpty()) {
            log.debug("Entry \""+normalizedBaseDn +"\" not found.");

            log.debug("Can't find base entry "+normalizedBaseDn);
            results.setReturnCode(LDAPException.NO_SUCH_OBJECT);
            return LDAPException.NO_SUCH_OBJECT;
        }

        Entry baseEntry = (Entry)path.iterator().next();

        log.debug("Found base entry: " + baseEntry.getDn());

        AttributeValues parentSourceValues = handler.getEngine().getParentSourceValues(partition, path);

        int rc = handler.getACLEngine().checkSearch(session, baseEntry.getDn(), entryMapping);
        if (rc != LDAPException.SUCCESS) {
            log.debug("Checking search permission => FAILED");
            return rc;
        }

        final Filter f = FilterTool.parseFilter(filter);
        log.debug("Parsed filter: "+f+" ("+f.getClass().getName()+")");

        final PenroseSearchResults filterPipeline = new PenroseSearchResults();

        filterPipeline.addListener(new PipelineAdapter() {
            public void objectAdded(PipelineEvent event) {
                Entry child = (Entry)filterPipeline.next();

                try {
                    Entry e = checkEntry(session, child, f, sc);
                    if (e == null) return;

                    results.add(e);
                } catch (Exception e) {
                    // ignore
                }
            }

            public void pipelineClosed(PipelineEvent event) {
                results.setReturnCode(filterPipeline.getReturnCode());
            }

        });

        Engine engine = handler.getEngine();

        if (partition.isProxy(entryMapping)) {
            engine = handler.getEngine("PROXY");
        }

        engine.search(
                partition,
                path,
                parentSourceValues,
                entryMapping,
                baseDn,
                f,
                sc,
                filterPipeline
        );

        if (sc.getScope() == LDAPConnection.SCOPE_ONE || sc.getScope() == LDAPConnection.SCOPE_SUB) { // one level or subtree
            log.debug("Searching children of \""+entryMapping.getDn()+"\"");

            Collection children = partition.getChildren(entryMapping);

            for (Iterator i = children.iterator(); i.hasNext();) {
                EntryMapping childMapping = (EntryMapping) i.next();

                searchChildren(partition, path, parentSourceValues, childMapping, baseDn, f, sc, filterPipeline);
            }
        }

        filterPipeline.close();

        return LDAPException.SUCCESS;
	}

    public void searchChildren(
            Partition partition,
            Collection parentPath,
            AttributeValues parentSourceValues,
            EntryMapping entryMapping,
            String baseDn,
            Filter filter,
            PenroseSearchControls sc,
            final PenroseSearchResults results) throws Exception {

        log.info("Search child mapping \""+entryMapping.getDn()+"\":");

        final PenroseSearchResults sr = new PenroseSearchResults();

        sr.addListener(new PipelineAdapter() {
            public void objectAdded(PipelineEvent event) {
                Entry child = (Entry)event.getObject();
                results.add(child);
            }
        });

        //PenroseSearchControls newSc = new PenroseSearchControls();
        //newSc.setAttributes(sc.getAttributes());
        //newSc.setScope(sc.getScope());

        //String newBaseDn = baseDn;

        //if (sc.getScope() == LDAPConnection.SCOPE_ONE) {
            //newSc.setScope(LDAPConnection.SCOPE_BASE);
            //newBaseDn = entryMapping.getDn();
        //}

        Engine engine = handler.getEngine();

        if (partition.isProxy(entryMapping)) {
            engine = handler.getEngine("PROXY");
        }

        engine.expand(
                partition,
                parentPath,
                parentSourceValues,
                entryMapping,
                baseDn,
                filter,
                sc,
                sr
        );

        sr.close();

        //log.debug("Waiting for search results from \""+entryMapping.getDn()+"\".");

        int rc = sr.getReturnCode();
        log.debug("RC: "+rc);

        if (rc != LDAPException.SUCCESS) {
            results.setReturnCode(rc);
            return;
        }

        if (sc.getScope() != LDAPConnection.SCOPE_SUB) return;

        log.debug("Searching children of " + entryMapping.getDn());

        AttributeValues newParentSourceValues = handler.getEngine().shiftParentSourceValues(parentSourceValues);

        Interpreter interpreter = handler.getEngine().getInterpreterManager().newInstance();

        AttributeValues av = handler.getEngine().computeAttributeValues(entryMapping, interpreter);
        for (Iterator j=av.getNames().iterator(); j.hasNext(); ) {
            String name = (String)j.next();
            Collection values = av.get(name);

            newParentSourceValues.add("parent."+name, values);
        }

        interpreter.clear();

        for (Iterator j=newParentSourceValues.getNames().iterator(); j.hasNext(); ) {
            String name = (String)j.next();
            Collection values = newParentSourceValues.get(name);
            log.debug(" - "+name+": "+values);
        }

        Collection newParentPath = new ArrayList();
        newParentPath.add(null);
        newParentPath.addAll(parentPath);

        Collection children = partition.getChildren(entryMapping);

        for (Iterator i = children.iterator(); i.hasNext();) {
            EntryMapping childMapping = (EntryMapping) i.next();

            searchChildren(partition, newParentPath, newParentSourceValues, childMapping, baseDn, filter, sc, results);
        }
    }

    public int searchRootDSE(
            PenroseSession session,
            String baseDn,
            String filter,
            PenroseSearchControls sc,
            PenroseSearchResults results) throws Exception {

        log.debug("Creating default Root DSE");

        Entry entry = new Entry("", null);
        AttributeValues attributeValues = entry.getAttributeValues();
        attributeValues.set("objectClass", "top");
        attributeValues.add("objectClass", "extensibleObject");
        attributeValues.set("vendorName", Penrose.VENDOR_NAME);
        attributeValues.set("vendorVersion", Penrose.PRODUCT_NAME+" "+Penrose.PRODUCT_VERSION);

        for (Iterator i=handler.getPartitionManager().getPartitions().iterator(); i.hasNext(); ) {
            Partition p = (Partition)i.next();
            for (Iterator j=p.getRootEntryMappings().iterator(); j.hasNext(); ) {
                EntryMapping e = (EntryMapping)j.next();
                if ("".equals(e.getDn())) continue;
                attributeValues.add("namingContexts", e.getDn());
            }
        }

        Collection attributeNames = sc.getAttributes();
        if (!attributeNames.isEmpty() && !attributeNames.contains("*")) {
            attributeValues.retain(attributeNames);
        }

        results.add(entry);

        return LDAPException.SUCCESS;
    }

    public Entry checkEntry(
            PenroseSession session,
            Entry entry,
            Filter filter,
            PenroseSearchControls sc
    ) throws Exception {

        if (!handler.getFilterTool().isValid(entry, filter)) return null;

        int rc = handler.getACLEngine().checkSearch(session, entry.getDn(), entry.getEntryMapping());
        if (rc != LDAPException.SUCCESS) return null;

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
            Entry entry
    ) throws Exception {

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
