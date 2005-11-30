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
import org.safehaus.penrose.session.PenroseSession;
import org.safehaus.penrose.session.PenroseSearchResults;
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

    private Handler handler;

    public SearchHandler(Handler handler) throws Exception {
        this.handler = handler;
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

        Map map = (Map)path.get(0);
        return (Entry)map.get("entry");
    }

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
            Map map = (Map)path.iterator().next();
            parent = (Entry)map.get("entry");
        }

        log.debug("Found parent: "+(parent == null ? null : parent.getDn()));

        Partition partition = handler.getPartitionManager().getPartitionByDn(dn);
        if (partition == null) {
            //log.error("Missing config for "+dn);
            return null;
        }

        // search the entry directly
        EntryMapping entryMapping = partition.getEntryMapping(dn);

        if (entryMapping != null) {
            log.debug("Found static entry: " + dn);

            Entry entry = handler.getEngine().find(path, entryMapping);
/*
            //AttributeValues values = entryDefinition.getAttributeValues(handlerContext.newInterpreter());
            //Entry entry = new Entry(dn, entryDefinition, values);

            log.debug("Entry:");
            log.debug(" - sourceValues: "+entry.getSourceValues());
            log.debug(" - attributeValues: "+entry.getAttributeValues());
            log.debug("\n"+entry);
*/
            log.debug("Adding "+entry.getDn()+" into path");
            Map map = new HashMap();
            map.put("dn", entry.getDn());
            map.put("entry", entry);
            map.put("entryDefinition", entry.getEntryMapping());
            path.add(0, map);

            return path;
        }

        log.debug("Searching dynamic entry: "+dn);

        if (parent == null) {
            log.error("Missing parent: "+parentDn);
            return null;
        }

        EntryMapping parentMapping = parent.getEntryMapping();

		Collection children = partition.getChildren(parentMapping);
        if (children == null) {
            log.debug("Entry "+parentDn+" has no children.");
            return null;
        }

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

            Engine engine = handler.getEngine();
            AttributeValues parentSourceValues = new AttributeValues();
            engine.getParentSourceValues(path, childMapping, parentSourceValues);

            PenroseSearchResults sr = handler.getEngine().search(
                    path,
                    parentSourceValues,
                    childMapping,
                    filter,
                    new ArrayList()
            );

            while (sr.hasNext()) {
                Entry child = (Entry)sr.next();
                if (handler.getFilterTool().isValid(child, filter)) {
                    log.debug("Adding "+child.getDn()+" into path");
                    Map map = new HashMap();
                    map.put("dn", child.getDn());
                    map.put("entry", child);
                    map.put("entryDefinition", child.getEntryMapping());
                    path.add(0, map);
                    return path;
                }
            }
		}

		return null;
	}

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
        handler.postEvent(base, beforeSearchEvent);

        int rc;

        try {
            rc = performSearch(session, base, scope, deref, filter, attributeNames, results);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            rc = LDAPException.OPERATIONS_ERROR;

        } finally {
            results.close();
        }

        SearchEvent afterSearchEvent = new SearchEvent(this, SearchEvent.AFTER_SEARCH, session, base);
        afterSearchEvent.setReturnCode(rc);
        handler.postEvent(base, afterSearchEvent);

        return rc;
    }

    public int performSearch(
            PenroseSession session,
            String base,
            int scope,
            int deref,
            String filter,
            Collection attributeNames,
            PenroseSearchResults results) throws Exception {

        Collection normalizedAttributeNames = null;
        if (attributeNames != null) {
            normalizedAttributeNames = new HashSet();
            for (Iterator i=attributeNames.iterator(); i.hasNext(); ) {
                String attributeName = (String)i.next();
                normalizedAttributeNames.add(attributeName.toLowerCase());
            }
        }

        if ("".equals(base) && scope == LDAPConnection.SCOPE_BASE) { // finding root DSE
            LDAPAttributeSet set = new LDAPAttributeSet();
            set.add(new LDAPAttribute("objectClass", new String[] { "top", "extensibleObject" }));
            set.add(new LDAPAttribute("vendorName", new String[] { Penrose.VENDOR_NAME }));
            set.add(new LDAPAttribute("vendorVersion", new String[] { Penrose.PRODUCT_NAME }));

            LDAPAttribute namingContexts = new LDAPAttribute("namingContexts");
            for (Iterator i=handler.getPartitionManager().getPartitions().iterator(); i.hasNext(); ) {
                Partition partition = (Partition)i.next();
                for (Iterator j=partition.getRootEntryMappings().iterator(); j.hasNext(); ) {
                    EntryMapping entry = (EntryMapping)j.next();
                    namingContexts.addValue(entry.getDn());
                }
            }
            set.add(namingContexts);

            LDAPEntry ldapEntry = new LDAPEntry("", set);
            Entry.filterAttributes(ldapEntry, normalizedAttributeNames);
            results.add(ldapEntry);

            results.setReturnCode(LDAPException.SUCCESS);
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
			log.debug("Can't find " + nbase);
			results.setReturnCode(LDAPException.NO_SUCH_OBJECT);
			return LDAPException.NO_SUCH_OBJECT;
		}

        Map map = (Map)path.iterator().next();
        //String baseDn = (String)map.get("dn");
        Entry baseEntry = (Entry)map.get("entry");
        log.debug("Found base entry: " + baseEntry.getDn());
        EntryMapping entryMapping = baseEntry.getEntryMapping();

        Engine engine = handler.getEngine();
        AttributeValues parentSourceValues = new AttributeValues();
        engine.getParentSourceValues(path, entryMapping, parentSourceValues);

        int rc = handler.getACLEngine().checkSearch(session, baseEntry);
        if (rc != LDAPException.SUCCESS) return rc;

		if (scope == LDAPConnection.SCOPE_BASE || scope == LDAPConnection.SCOPE_SUB) { // base or subtree
			if (handler.getFilterTool().isValid(baseEntry, f)) {

                rc = handler.getACLEngine().checkRead(session, baseEntry);
                if (rc == LDAPException.SUCCESS) {
                    LDAPEntry ldapEntry = baseEntry.toLDAPEntry();
                    Entry.filterAttributes(ldapEntry, normalizedAttributeNames);
                    results.add(ldapEntry);
                }
			}
		}

		if (scope == LDAPConnection.SCOPE_ONE || scope == LDAPConnection.SCOPE_SUB) { // one level or subtree
            searchChildren(session, path, entryMapping, parentSourceValues, scope, f, normalizedAttributeNames, results, true);
		}

		results.setReturnCode(LDAPException.SUCCESS);
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

        Partition partition = handler.getPartitionManager().getPartition(entryMapping);
        Collection children = partition.getChildren(entryMapping);
        if (children == null) {
            return;
        }

        for (Iterator i = children.iterator(); i.hasNext();) {
            EntryMapping childMapping = (EntryMapping) i.next();

            if (handler.getFilterTool().isValid(childMapping, filter)) {

                PenroseSearchResults sr = handler.getEngine().search(
                        path,
                        parentSourceValues,
                        childMapping,
                        filter,
                        attributeNames
                );

                while (sr.hasNext()) {
                    Entry child = (Entry)sr.next();

                    int rc = handler.getACLEngine().checkSearch(session, child);
                    if (rc != LDAPException.SUCCESS) continue;

                    if (!handler.getFilterTool().isValid(child, filter)) continue;

                    rc = handler.getACLEngine().checkRead(session, child);
                    if (rc != LDAPException.SUCCESS) continue;

                    //newParents.add(child);

                    LDAPEntry en = child.toLDAPEntry();
                    Entry.filterAttributes(en, attributeNames);
                    results.add(en);
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

                Map map = new HashMap();
                map.put("dn", childMapping.getDn());
                map.put("entry", null);
                map.put("entryDefinition", childMapping);

                Collection newPath = new ArrayList();
                newPath.add(map);
                newPath.addAll(path);

                searchChildren(session, newPath, childMapping, newParentSourceValues, scope, filter, attributeNames, results, false);
            }
        }
    }

}
