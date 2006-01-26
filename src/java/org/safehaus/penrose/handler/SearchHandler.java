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
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.pipeline.PipelineAdapter;
import org.safehaus.penrose.pipeline.PipelineEvent;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.engine.Engine;
import org.safehaus.penrose.event.SearchEvent;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.filter.SimpleFilter;
import org.safehaus.penrose.filter.FilterTool;
import org.safehaus.penrose.mapping.*;
import org.apache.log4j.Logger;
import org.ietf.ldap.*;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class SearchHandler {

    Logger log = Logger.getLogger(getClass());

    private SessionHandler sessionHandler;

    public SearchHandler(SessionHandler sessionHandler) throws Exception {
        this.sessionHandler = sessionHandler;
    }

    /**
	 * Find an entry given a dn.
	 *
	 * @param dn
	 * @return path from the entry to the root entry
	 */
    public Entry find(
            PenroseSession session,
            String dn) throws Exception {

        List path = findPath(session, dn);
        if (path == null) return null;
        if (path.size() == 0) return null;

        //Map map = (Map)path.get(0);
        //return (Entry)map.get("entry");
        return (Entry)path.get(0);
    }

    /**
     *
     * @param session
     * @param dn
     * @return path (List of Entries).
     * @throws Exception
     */
    public List findPath(
            PenroseSession session,
            String dn) throws Exception {

        if (dn == null) return null;

        String parentDn = Entry.getParentDn(dn);
        Row rdn = Entry.getRdn(dn);

        log.debug("Find entry: ["+rdn+"] ["+parentDn+"]");

        List path = findPath(session, parentDn);
        Entry parent;

        if (path == null) {
            path = new ArrayList();
            parent = null;
        } else {
            parent = (Entry)path.iterator().next();
            //Map map = (Map)path.iterator().next();
            //parent = (Entry)map.get("entry");
        }

        log.debug("Found parent: "+(parent == null ? null : parent.getDn()));

        Partition partition = sessionHandler.getPartitionManager().getPartitionByDn(dn);
        if (partition == null) {
            //log.error("Missing config for "+dn);
            return null;
        }

        // search the entry directly
        EntryMapping entryMapping = partition.getEntryMapping(dn);

        if (entryMapping != null) {
            log.debug("Found static entry: " + dn);

            Entry entry = find(path, entryMapping);
/*
            //AttributeValues values = entryDefinition.getAttributeValues(handlerContext.newInterpreter());
            //Entry entry = new Entry(dn, entryDefinition, values);

            log.debug("Entry:");
            log.debug(" - sourceValues: "+entry.getSourceValues());
            log.debug(" - attributeValues: "+entry.getAttributeValues());
            log.debug("\n"+entry);
*/
            log.debug("Adding "+entry.getDn()+" into path");
            //Map map = new HashMap();
            //map.put("dn", entry.getDn());
            //map.put("entry", entry);
            //map.put("entryDefinition", entry.getEntryMapping());
            //path.add(0, map);
            path.add(0, entry);

            return path;
        }

        log.debug("Searching dynamic entry: "+dn);

        if (parent == null) {
            log.error("Missing parent: "+parentDn);
            return null;
        }

        EntryMapping parentMapping = parent.getEntryMapping();

		Collection children = partition.getChildren(parentMapping);

        Filter filter = null;
        for (Iterator iterator=rdn.getNames().iterator(); iterator.hasNext(); ) {
            String name = (String)iterator.next();
            String value = (String)rdn.get(name);

            SimpleFilter sf = new SimpleFilter(name, "=", value);
            filter = FilterTool.appendAndFilter(filter, sf);
        }

        log.debug("Searching children with filter "+filter);

		for (Iterator iterator = children.iterator(); iterator.hasNext(); ) {
			EntryMapping childMapping = (EntryMapping) iterator.next();

            Row childRdn = Entry.getRdn(childMapping.getRdn());
            log.debug("Finding entry in "+childMapping.getDn());

            if (!rdn.getNames().equals(childRdn.getNames())) continue;

            Engine engine = sessionHandler.getEngine();
            AttributeValues parentSourceValues = new AttributeValues();
            engine.getParentSourceValues(path, childMapping, parentSourceValues);

            PenroseSearchResults sr = search(
                    path,
                    parentSourceValues,
                    childMapping,
                    true,
                    filter,
                    new ArrayList()
            );

            while (sr.hasNext()) {
                Entry child = (Entry)sr.next();
                if (sessionHandler.getFilterTool().isValid(child, filter)) {

                    if (sr.getReturnCode() != LDAPException.SUCCESS) return null;

                    log.debug("Adding "+child.getDn()+" into path");
                    //Map map = new HashMap();
                    //map.put("dn", child.getDn());
                    //map.put("entry", child);
                    //map.put("entryDefinition", child.getEntryMapping());
                    //path.add(0, map);
                    path.add(0, child);
                    return path;
                }
            }
		}

		return null;
	}

    public Entry find(
            Collection path,
            EntryMapping entryMapping
            ) throws Exception {

        if (log.isDebugEnabled()) {
            log.debug(org.safehaus.penrose.util.Formatter.displaySeparator(80));
            log.debug(org.safehaus.penrose.util.Formatter.displayLine("FIND", 80));
            log.debug(org.safehaus.penrose.util.Formatter.displayLine("Entry: "+entryMapping.getDn(), 80));
            log.debug(org.safehaus.penrose.util.Formatter.displayLine("Parents:", 80));

            for (Iterator i=path.iterator(); i.hasNext(); ) {
                Entry entry = (Entry)i.next();
                String dn = entry.getDn();
                //Map map = (Map)i.next();
                //String dn = (String)map.get("dn");
                log.debug(org.safehaus.penrose.util.Formatter.displayLine(" - "+dn, 80));
            }

            log.debug(org.safehaus.penrose.util.Formatter.displaySeparator(80));
        }

        AttributeValues parentSourceValues = new AttributeValues();

        PenroseSearchResults results = search(
                path,
                parentSourceValues,
                entryMapping,
                true,
                null,
                null
        );

        if (results.size() == 0) return null;
        if (results.getReturnCode() != LDAPException.SUCCESS) return null;

        Entry entry = (Entry)results.next();

        if (log.isDebugEnabled()) {
            log.debug(org.safehaus.penrose.util.Formatter.displaySeparator(80));
            log.debug(org.safehaus.penrose.util.Formatter.displayLine("FIND RESULT", 80));
            log.debug(org.safehaus.penrose.util.Formatter.displayLine("dn: "+entry.getDn(), 80));
            log.debug(org.safehaus.penrose.util.Formatter.displaySeparator(80));
        }

        return entry;
    }

    /**
     * @param session
     * @param base
     * @param scope
     * @param deref
     * @param filter
     * @param attributeNames
     * @param results
     * @return Entry
     * @throws Exception
     */
    public int search(
            PenroseSession session,
            String base,
            int scope,
            int deref,
            String filter,
            Collection attributeNames,
            PenroseSearchResults results) throws Exception {

        String s = null;
        switch (scope) {
        case LDAPConnection.SCOPE_BASE:
            s = "base";
            break;
        case LDAPConnection.SCOPE_ONE:
            s = "one level";
            break;
        case LDAPConnection.SCOPE_SUB:
            s = "subtree";
            break;
        }

        String d = null;
        switch (deref) {
        case LDAPSearchConstraints.DEREF_NEVER:
            d = "never";
            break;
        case LDAPSearchConstraints.DEREF_SEARCHING:
            d = "searching";
            break;
        case LDAPSearchConstraints.DEREF_FINDING:
            d = "finding";
            break;
        case LDAPSearchConstraints.DEREF_ALWAYS:
            d = "always";
            break;
        }

        log.debug("----------------------------------------------------------------------------------");
        log.info("SEARCH:");
        if (session != null && session.getBindDn() != null) log.info(" - Bind DN: " + session.getBindDn());
        log.info(" - Base DN: " + base);
        log.info(" - Scope: " + s);
        log.info(" - Filter: "+filter);
        log.debug(" - Alias Dereferencing: " + d);
        log.debug(" - Attribute Names: " + attributeNames);
        log.info("");

        SearchEvent beforeSearchEvent = new SearchEvent(this, SearchEvent.BEFORE_SEARCH, session, base);
        sessionHandler.postEvent(base, beforeSearchEvent);

        int rc;

        try {
            rc = performSearch(session, base, scope, deref, filter, attributeNames, results);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            rc = LDAPException.OPERATIONS_ERROR;
            results.setReturnCode(rc);

        } finally {
            results.close();
        }

        SearchEvent afterSearchEvent = new SearchEvent(this, SearchEvent.AFTER_SEARCH, session, base);
        afterSearchEvent.setReturnCode(rc);
        sessionHandler.postEvent(base, afterSearchEvent);

        return rc;
    }

    /**
     * @param session
     * @param base
     * @param scope
     * @param deref
     * @param filter
     * @param attributeNames
     * @param results
     * @return Entry
     * @throws Exception
     */
    public int performSearch(
            PenroseSession session,
            String base,
            int scope,
            int deref,
            String filter,
            Collection attributeNames,
            PenroseSearchResults results) throws Exception {

        if ("".equals(base) && scope == LDAPConnection.SCOPE_BASE) { // finding root DSE
            Entry entry = new Entry("", null);
            AttributeValues attributeValues = entry.getAttributeValues();
            attributeValues.set("objectClass", "top");
            attributeValues.add("objectClass", "extensibleObject");
            attributeValues.set("vendorName", Penrose.VENDOR_NAME);
            attributeValues.set("vendorVersion", Penrose.PRODUCT_NAME+" "+Penrose.PRODUCT_VERSION);

            for (Iterator i=sessionHandler.getPartitionManager().getPartitions().iterator(); i.hasNext(); ) {
                Partition partition = (Partition)i.next();
                for (Iterator j=partition.getRootEntryMappings().iterator(); j.hasNext(); ) {
                    EntryMapping entryMapping = (EntryMapping)j.next();
                    attributeValues.add("namingContexts", entryMapping.getDn());
                }
            }

            results.add(entry);
/*
            LDAPAttributeSet set = new LDAPAttributeSet();
            set.add(new LDAPAttribute("objectClass", new String[] { "top", "extensibleObject" }));
            set.add(new LDAPAttribute("vendorName", new String[] { Penrose.VENDOR_NAME }));
            set.add(new LDAPAttribute("vendorVersion", new String[] { Penrose.PRODUCT_NAME+" "+Penrose.PRODUCT_VERSION }));

            LDAPAttribute namingContexts = new LDAPAttribute("namingContexts");
            for (Iterator i=sessionHandler.getPartitionManager().getPartitions().iterator(); i.hasNext(); ) {
                Partition partition = (Partition)i.next();
                for (Iterator j=partition.getRootEntryMappings().iterator(); j.hasNext(); ) {
                    EntryMapping entryMapping = (EntryMapping)j.next();
                    namingContexts.addValue(entryMapping.getDn());
                }
            }
            set.add(namingContexts);

            LDAPEntry ldapEntry = new LDAPEntry("", set);
            Entry.filterAttributes(ldapEntry, normalizedAttributeNames);
            results.add(ldapEntry);
*/
            //results.setReturnCode(LDAPException.SUCCESS);
            return LDAPException.SUCCESS;
        }

        String nbase;
        try {
            nbase = LDAPDN.normalize(base);
        } catch (IllegalArgumentException e) {
            results.setReturnCode(LDAPException.INVALID_DN_SYNTAX);
            return LDAPException.INVALID_DN_SYNTAX;
        }

		Filter f = FilterTool.parseFilter(filter);
		log.debug("Parsed filter: "+f+" ("+f.getClass().getName()+")");

		List path = findPath(session, nbase);

		if (path == null) {
			log.debug("Can't find base entry " + nbase);
			results.setReturnCode(LDAPException.NO_SUCH_OBJECT);
			return LDAPException.NO_SUCH_OBJECT;
		}

        //Map map = (Map)path.iterator().next();
        //Entry baseEntry = (Entry)map.get("entry");
        Entry baseEntry = (Entry)path.iterator().next();

        log.debug("Found base entry: " + baseEntry.getDn());
        EntryMapping entryMapping = baseEntry.getEntryMapping();

        Engine engine = sessionHandler.getEngine();
        AttributeValues parentSourceValues = new AttributeValues();
        engine.getParentSourceValues(path, entryMapping, parentSourceValues);

        int rc = sessionHandler.getACLEngine().checkSearch(session, baseEntry);
        if (rc != LDAPException.SUCCESS) return rc;

		if (scope == LDAPConnection.SCOPE_BASE || scope == LDAPConnection.SCOPE_SUB) { // base or subtree
			if (sessionHandler.getFilterTool().isValid(baseEntry, f)) {

                rc = sessionHandler.getACLEngine().checkRead(session, baseEntry);
                if (rc == LDAPException.SUCCESS) {
                    results.add(baseEntry);
                }
			}
		}

		if (scope == LDAPConnection.SCOPE_ONE || scope == LDAPConnection.SCOPE_SUB) { // one level or subtree
            searchChildren(session, path, entryMapping, parentSourceValues, scope, f, attributeNames, results, true);
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
            PenroseSearchResults results,
            boolean first) throws Exception {

        Partition partition = sessionHandler.getPartitionManager().getPartition(entryMapping);
        Collection children = partition.getChildren(entryMapping);

        for (Iterator i = children.iterator(); i.hasNext();) {
            EntryMapping childMapping = (EntryMapping) i.next();

            if (sessionHandler.getFilterTool().isValid(childMapping, filter)) {

                PenroseSearchResults sr = search(
                        path,
                        parentSourceValues,
                        childMapping,
                        false,
                        filter,
                        attributeNames
                );

                while (sr.hasNext()) {
                    Entry child = (Entry)sr.next();

                    int rc = sessionHandler.getACLEngine().checkSearch(session, child);
                    if (rc != LDAPException.SUCCESS) continue;

                    if (!sessionHandler.getFilterTool().isValid(child, filter)) continue;

                    rc = sessionHandler.getACLEngine().checkRead(session, child);
                    if (rc != LDAPException.SUCCESS) continue;

                    results.add(child);
                }

                int rc = sr.getReturnCode();

                if (rc != LDAPException.SUCCESS) {
                    log.debug("RC: "+rc);
                    results.setReturnCode(rc);
                    continue;
                }
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

                Engine engine = sessionHandler.getEngine();
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

                //Map map = new HashMap();
                //map.put("dn", childMapping.getDn());
                //map.put("entry", null);
                //map.put("entryDefinition", childMapping);

                Collection newPath = new ArrayList();
                newPath.add(null);
                newPath.addAll(path);

                searchChildren(session, newPath, childMapping, newParentSourceValues, scope, filter, attributeNames, results, false);
            }
        }
    }

    public PenroseSearchResults search(
            final Collection path,
            final AttributeValues parentSourceValues,
            final EntryMapping entryMapping,
            boolean single,
            final Filter filter,
            Collection attributeNames) throws Exception {

        if (log.isDebugEnabled()) {
            log.debug(org.safehaus.penrose.util.Formatter.displaySeparator(80));
            log.debug(org.safehaus.penrose.util.Formatter.displayLine("SEARCH", 80));
            log.debug(org.safehaus.penrose.util.Formatter.displayLine("Entry: "+entryMapping.getDn(), 80));
            log.debug(org.safehaus.penrose.util.Formatter.displayLine("Filter: "+filter, 80));
            log.debug(org.safehaus.penrose.util.Formatter.displayLine("Parents:", 80));

            if (parentSourceValues != null) {
                for (Iterator i = parentSourceValues.getNames().iterator(); i.hasNext(); ) {
                    String name = (String)i.next();
                    Collection values = parentSourceValues.get(name);
                    log.debug(org.safehaus.penrose.util.Formatter.displayLine(" - "+name+": "+values, 80));
                }
            }

            log.debug(org.safehaus.penrose.util.Formatter.displaySeparator(80));
        }

        final PenroseSearchResults results = new PenroseSearchResults();
        final PenroseSearchResults dns = new PenroseSearchResults();
        final PenroseSearchResults entriesToLoad = new PenroseSearchResults();
        final PenroseSearchResults loadedEntries = new PenroseSearchResults();
        final PenroseSearchResults newEntries = new PenroseSearchResults();

        Collection attributeDefinitions = entryMapping.getAttributeMappings(attributeNames);

        // check if client only requests the dn to be returned
        final boolean dnOnly = attributeNames != null && attributeNames.contains("dn")
                && attributeDefinitions.isEmpty()
                && "(objectclass=*)".equals(filter.toString().toLowerCase());

        final Interpreter interpreter = sessionHandler.getEngine().getInterpreterFactory().newInstance();

        dns.addListener(new PipelineAdapter() {
            public void objectAdded(PipelineEvent event) {
                try {
                    Map map = (Map)event.getObject();
                    String dn = (String)map.get("dn");

                    Entry entry = (Entry)sessionHandler.getEngine().getEntryCache().get(dn);
                    log.debug("Entry cache for "+dn+": "+(entry == null ? "not found" : "found"));

                    if (entry == null) {

                        if (dnOnly) {
                            AttributeValues sv = (AttributeValues)map.get("sourceValues");
                            AttributeValues attributeValues = sessionHandler.getEngine().computeAttributeValues(entryMapping, sv, interpreter);
                            entry = new Entry(dn, entryMapping, sv, attributeValues);

                            results.add(entry);

                        } else {
                            entriesToLoad.add(map);
                        }

                    } else {
                        results.add(entry);
                    }

                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            }

            public void pipelineClosed(PipelineEvent event) {
                int rc = dns.getReturnCode();
                //log.debug("RC: "+rc);

                if (dnOnly) {
                    results.setReturnCode(rc);
                    results.close();
                } else {
                    entriesToLoad.setReturnCode(rc);
                    entriesToLoad.close();
                }
            }
        });

        Entry parent = null;
        if (path != null && path.size() > 0) {
            parent = (Entry)path.iterator().next();
        }

        log.debug("Parent: "+(parent == null ? null : parent.getDn()));

        Collection list = null;
        if (parent != null) {
            String parentDn = parent.getDn();
            list = sessionHandler.getEngine().getEntryCache().search(entryMapping, parentDn, filter);
        }

        if (list == null || list.size() == 0) {

            log.debug("Filter cache for "+filter+" not found.");

            dns.addListener(new PipelineAdapter() {
                public void objectAdded(PipelineEvent event) {
                    try {
                        Map map = (Map)event.getObject();
                        String dn = (String)map.get("dn");

                        log.debug("Adding "+dn+" into filter cache for "+filter+" in "+entryMapping.getDn());

                        sessionHandler.getEngine().getEntryCache().add(entryMapping, filter, dn);

                    } catch (Exception e) {
                        log.error(e.getMessage(), e);
                    }
                }
            });

            sessionHandler.getEngine().search(parent, parentSourceValues, entryMapping, filter, dns);

        } else {
            log.debug("Filter cache for "+filter+" found.");
            for (Iterator i=list.iterator(); i.hasNext(); ) {
                String dn = (String)i.next();
                log.debug(" - "+dn);

                Map map = new HashMap();
                map.put("dn", dn);
                map.put("sourceValues", new AttributeValues());
                dns.add(map);
            }

            dns.close();
        }

        if (dnOnly) return results;

        sessionHandler.getEngine().load(entryMapping, entriesToLoad, loadedEntries);

        newEntries.addListener(new PipelineAdapter() {
            public void objectAdded(PipelineEvent event) {
                try {
                    Entry entry = (Entry)event.getObject();
                    sessionHandler.getEngine().getEntryCache().put(entry);
                    results.add(entry);

                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            }

            public void pipelineClosed(PipelineEvent event) {
                int rc = newEntries.getReturnCode();
                //log.debug("RC: "+rc);

                results.setReturnCode(rc);
                results.close();
            }
        });

        sessionHandler.getEngine().merge(entryMapping, loadedEntries, newEntries);

        return results;
    }

}
