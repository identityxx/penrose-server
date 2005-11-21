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

import org.safehaus.penrose.SearchResults;
import org.safehaus.penrose.PenroseConnection;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.engine.Engine;
import org.safehaus.penrose.event.SearchEvent;
import org.safehaus.penrose.config.Config;
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
    private HandlerContext handlerContext;

    public SearchHandler(Handler handler) throws Exception {
        this.handler = handler;
        this.handlerContext = handler.getHandlerContext();
    }

    public HandlerContext getHandlerContext() {
        return handlerContext;
    }

    public void setHandlerContext(HandlerContext handlerContext) {
        this.handlerContext = handlerContext;
    }

    /**
	 * Find an entry given a dn.
	 *
	 * @param dn
	 * @return path from the entry to the root entry
	 */
    public Entry find(
            PenroseConnection connection,
            String dn) throws Exception {

        List path = findPath(connection, dn);
        if (path == null) return null;
        if (path.size() == 0) return null;

        Map map = (Map)path.get(0);
        return (Entry)map.get("entry");
    }

    public List findPath(
            PenroseConnection connection,
            String dn) throws Exception {

        if (dn == null) return null;

        String parentDn = Entry.getParentDn(dn);
        Row rdn = Entry.getRdn(dn);

        List path = findPath(connection, parentDn);
        Entry parent;

        if (path == null) {
            path = new ArrayList();
            parent = null;
        } else {
            Map map = (Map)path.iterator().next();
            parent = (Entry)map.get("entry");
        }

		log.debug("Find entry: ["+rdn+"] ["+parentDn+"]");

        Config config = getHandlerContext().getConfig(dn);
        if (config == null) return null;

        // search the entry directly
        EntryDefinition entryDefinition = config.getEntryDefinition(dn);

        if (entryDefinition != null) {
            log.debug("Found static entry: " + dn);

            Entry entry = handler.getEngine().find(path, entryDefinition);
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
            map.put("entryDefinition", entry.getEntryDefinition());
            path.add(0, map);

            return path;
        }

        if (parent == null) return null;

        EntryDefinition parentDefinition = parent.getEntryDefinition();

		//log.debug("Found parent entry: " + parentDn);
		Collection children = config.getChildren(parentDefinition);
        if (children == null) return null;

        Filter filter = null;
        for (Iterator iterator=rdn.getNames().iterator(); iterator.hasNext(); ) {
            String name = (String)iterator.next();
            String value = (String)rdn.get(name);

            SimpleFilter sf = new SimpleFilter(name, "=", value);
            filter = FilterTool.appendAndFilter(filter, sf);
        }

        // Find in each dynamic children
		for (Iterator iterator = children.iterator(); iterator.hasNext(); ) {
			EntryDefinition childDefinition = (EntryDefinition) iterator.next();

            Row childRdn = Entry.getRdn(childDefinition.getRdn());
            log.debug("Finding entry in "+childDefinition.getDn()+" with "+filter);

            if (!rdn.getNames().equals(childRdn.getNames())) continue;

            Engine engine = handler.getEngine();
            AttributeValues parentSourceValues = new AttributeValues();
            String prefix = engine.getParentSourceValues(path, childDefinition, parentSourceValues);

            SearchResults sr = handler.getEngine().search(
                    path,
                    parentSourceValues,
                    childDefinition,
                    filter,
                    new ArrayList()
            );

            while (sr.hasNext()) {
                Entry child = (Entry)sr.next();
                if (handlerContext.getFilterTool().isValid(child, filter)) {
                    log.debug("Adding "+child.getDn()+" into path");
                    Map map = new HashMap();
                    map.put("dn", child.getDn());
                    map.put("entry", child);
                    map.put("entryDefinition", child.getEntryDefinition());
                    path.add(0, map);
                    return path;
                }
            }
		}

		return null;
	}

    public int search(
            PenroseConnection connection,
            String base,
            int scope,
            int deref,
            String filter,
            Collection attributeNames,
            SearchResults results) throws Exception {

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
        if (connection != null && connection.getBindDn() != null) log.info(" - Bind DN: " + connection.getBindDn());
        log.info(" - Base DN: " + base);
        log.info(" - Scope: " + s);
        log.info(" - Filter: "+filter);
        log.debug(" - Alias Dereferencing: " + d);
        log.debug(" - Attribute Names: " + attributeNames);
        log.info("");

        SearchEvent beforeSearchEvent = new SearchEvent(this, SearchEvent.BEFORE_SEARCH, connection, base);
        handler.postEvent(base, beforeSearchEvent);

        int rc;

        try {
            rc = performSearch(connection, base, scope, deref, filter, attributeNames, results);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            rc = LDAPException.OPERATIONS_ERROR;

        } finally {
            results.close();
        }

        SearchEvent afterSearchEvent = new SearchEvent(this, SearchEvent.AFTER_SEARCH, connection, base);
        afterSearchEvent.setReturnCode(rc);
        handler.postEvent(base, afterSearchEvent);

        return rc;
    }

    public int performSearch(
            PenroseConnection connection,
            String base,
            int scope,
            int deref,
            String filter,
            Collection attributeNames,
            SearchResults results) throws Exception {

		String nbase;
		try {
			nbase = LDAPDN.normalize(base);
		} catch (IllegalArgumentException e) {
			results.setReturnCode(LDAPException.INVALID_DN_SYNTAX);
			return LDAPException.INVALID_DN_SYNTAX;
		}

        Collection normalizedAttributeNames = new HashSet();
        for (Iterator i=attributeNames.iterator(); i.hasNext(); ) {
            String attributeName = (String)i.next();
            normalizedAttributeNames.add(attributeName.toLowerCase());
        }

        if ("".equals(base) && scope == LDAPConnection.SCOPE_BASE) { // finding root DSE
            LDAPAttributeSet set = new LDAPAttributeSet();
            set.add(new LDAPAttribute("objectClass", new String[] { "top", "extensibleObject" }));
            set.add(new LDAPAttribute("vendorName", new String[] { "Identyx Corporation" }));
            set.add(new LDAPAttribute("vendorVersion", new String[] { "Penrose Virtual Directory Server 0.9.8" }));

            LDAPAttribute namingContexts = new LDAPAttribute("namingContexts");
            for (Iterator i=handlerContext.getConfigs().iterator(); i.hasNext(); ) {
                Config config = (Config)i.next();
                for (Iterator j=config.getRootEntryDefinitions().iterator(); j.hasNext(); ) {
                    EntryDefinition entry = (EntryDefinition)j.next();
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

		Filter f = FilterTool.parseFilter(filter);
		log.debug("Parsed filter: "+f+" ("+f.getClass().getName()+")");

		List path = findPath(connection, nbase);

		if (path == null) {
			log.debug("Can't find " + nbase);
			results.setReturnCode(LDAPException.NO_SUCH_OBJECT);
			return LDAPException.NO_SUCH_OBJECT;
		}

        Map map = (Map)path.iterator().next();
        String baseDn = (String)map.get("dn");
        Entry baseEntry = (Entry)map.get("entry");
        log.debug("Found base entry: " + baseEntry.getDn());
        EntryDefinition entryDefinition = baseEntry.getEntryDefinition();

        Engine engine = handler.getEngine();
        AttributeValues parentSourceValues = new AttributeValues();
        String prefix = engine.getParentSourceValues(path, entryDefinition, parentSourceValues);

        int rc = handlerContext.getACLEngine().checkSearch(connection, baseEntry);
        if (rc != LDAPException.SUCCESS) return rc;

		if (scope == LDAPConnection.SCOPE_BASE || scope == LDAPConnection.SCOPE_SUB) { // base or subtree
			if (handlerContext.getFilterTool().isValid(baseEntry, f)) {

                rc = handlerContext.getACLEngine().checkRead(connection, baseEntry);
                if (rc == LDAPException.SUCCESS) {
                    LDAPEntry ldapEntry = baseEntry.toLDAPEntry();
                    Entry.filterAttributes(ldapEntry, normalizedAttributeNames);
                    results.add(ldapEntry);
                }
			}
		}

		if (scope == LDAPConnection.SCOPE_ONE || scope == LDAPConnection.SCOPE_SUB) { // one level or subtree
            searchChildren(connection, path, entryDefinition, parentSourceValues, scope, f, normalizedAttributeNames, results, true);
		}

		results.setReturnCode(LDAPException.SUCCESS);
		return LDAPException.SUCCESS;
	}

    public void searchChildren(
            PenroseConnection connection,
            Collection path,
            EntryDefinition entryDefinition,
            AttributeValues parentSourceValues,
            int scope,
            Filter filter,
            Collection attributeNames,
            SearchResults results,
            boolean first) throws Exception {

        Config config = handlerContext.getConfig(entryDefinition.getDn());
        Collection children = config.getChildren(entryDefinition);
        if (children == null) {
            return;
        }

        for (Iterator i = children.iterator(); i.hasNext();) {
            EntryDefinition childDefinition = (EntryDefinition) i.next();

            if (handlerContext.getFilterTool().isValid(childDefinition, filter)) {

                SearchResults sr = handler.getEngine().search(
                        path,
                        parentSourceValues,
                        childDefinition,
                        filter,
                        attributeNames
                );

                while (sr.hasNext()) {
                    Entry child = (Entry)sr.next();

                    int rc = handlerContext.getACLEngine().checkSearch(connection, child);
                    if (rc != LDAPException.SUCCESS) continue;

                    if (!handlerContext.getFilterTool().isValid(child, filter)) continue;

                    rc = handlerContext.getACLEngine().checkRead(connection, child);
                    if (rc != LDAPException.SUCCESS) continue;

                    //newParents.add(child);

                    LDAPEntry en = child.toLDAPEntry();
                    Entry.filterAttributes(en, attributeNames);
                    results.add(en);
                }
            }

            if (scope == LDAPConnection.SCOPE_SUB) {
                log.debug("Searching children of " + childDefinition.getDn());

                AttributeValues newParentSourceValues = new AttributeValues();
                for (Iterator j=parentSourceValues.getNames().iterator(); j.hasNext(); ) {
                    String name = (String)j.next();
                    Collection values = parentSourceValues.get(name);

                    if (name.startsWith("parent.")) name = "parent."+name;

                    newParentSourceValues.add(name, values);
                }

                Engine engine = handler.getEngine();
                Interpreter interpreter = engine.getInterpreterFactory().newInstance();

                AttributeValues av = engine.computeAttributeValues(childDefinition, interpreter);
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
                map.put("dn", childDefinition.getDn());
                map.put("entry", null);
                map.put("entryDefinition", childDefinition);

                Collection newPath = new ArrayList();
                newPath.add(map);
                newPath.addAll(path);

                searchChildren(connection, newPath, childDefinition, newParentSourceValues, scope, filter, attributeNames, results, false);
            }
        }
    }

}
