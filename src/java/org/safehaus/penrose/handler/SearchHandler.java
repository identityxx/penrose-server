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
import org.safehaus.penrose.event.SearchEvent;
import org.safehaus.penrose.config.Config;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.filter.SimpleFilter;
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

    /**
	 * Find an real entry given a dn. If not found it will search for a virtual
	 * entry from all possible mappings.
	 *
	 * @param dn
	 * @return virtual entry
	 * @throws Exception
	 */
	public Entry find(
            PenroseConnection connection,
            String dn) throws Exception {

		log.debug("Find entry: " + dn);

        Config config = getHandlerContext().getConfig(dn);
        if (config == null) return null;

        // search the entry directly
        EntryDefinition entryDefinition = config.getEntryDefinition(dn);

        if (entryDefinition != null) {
            log.debug("Found static entry: " + dn);

            AttributeValues values = entryDefinition.getAttributeValues(handlerContext.newInterpreter());
            return new Entry(dn, entryDefinition, values);
        }

		int i = dn.indexOf(",");
		String rdn;
		String parentDn;

		if (i < 0) {
			rdn = dn;
			parentDn = null;
		} else {
			rdn = dn.substring(0, i);
			parentDn = dn.substring(i + 1);
		}

        // find the parent entry
        Entry parent = null;

        try {
            parent = find(connection, parentDn);
        } catch (Exception e) {
            log.debug("Exception: "+e.getMessage(), e);
            // ignore
        }

		if (parent == null) {
            log.debug("Parent not found: " + dn);

            throw new LDAPException("Can't find " + dn + ".",
                    LDAPException.NO_SUCH_OBJECT, "Can't find " + dn + ".");
		}

        EntryDefinition parentDefinition = parent.getEntryDefinition();

		//log.debug("Found parent entry: " + parentDn);
		Collection children = config.getChildren(parentDefinition);

        if (children == null) {
            log.debug("Children not found: " + dn);

            throw new LDAPException("Can't find " + dn + ".",
                    LDAPException.NO_SUCH_OBJECT, "Can't find " + dn + ".");
        }

        int j = rdn.indexOf("=");
        String rdnAttribute = rdn.substring(0, j);
        String rdnValue = rdn.substring(j + 1);

        Collection parents = new ArrayList();
        parents.add(parent);
        Filter filter = new SimpleFilter(rdnAttribute, "=", rdnValue);

        // Find in each dynamic children
		for (Iterator iterator = children.iterator(); iterator.hasNext(); ) {
			EntryDefinition childDefinition = (EntryDefinition) iterator.next();

            String childRdn = childDefinition.getRdn();
            log.debug("Finding entry in "+childDefinition.getDn()+" with "+filter);

            int k = childRdn.indexOf("=");
            String childRdnAttribute = childRdn.substring(0, k);
            String childRdnValue = childRdn.substring(k+1);

            // the rdn attribute types must match
            if (!rdnAttribute.equals(childRdnAttribute)) continue;

            SearchResults sr = handlerContext.getEngine().search(
                    connection,
                    parents,
                    childDefinition,
                    filter,
                    new ArrayList()
            );

            while (sr.hasNext()) {
                Entry child = (Entry)sr.next();
                if (handlerContext.getFilterTool().isValidEntry(child, filter)) return child;
            }
/*
			if (childDefinition.isDynamic()) {

                log.debug("Found entry definition: " + childDefinition.getDn());

                try {
                    Entry entry = find(connection, parent, childDefinition, rdn);
                    if (entry == null) continue;

                    log.debug("Found virtual entry: " + entry.getDn());

                    return entry;

                } catch (Exception e) {
                    e.printStackTrace(System.out);
                    // not found, continue to next entry definition
                }

            } else {
                if (!rdnValue.toLowerCase().equals(childRdnValue.toLowerCase())) continue;

                log.debug("Found static entry: " + childDefinition.getDn());

                AttributeValues values = childDefinition.getAttributeValues(handlerContext.newInterpreter());
                Entry entry = new Entry(dn, childDefinition, values);
                return entry;
            }
*/
		}

		throw new LDAPException("Can't find " + dn + ".",
				LDAPException.NO_SUCH_OBJECT, "Can't find virtual entry " + dn + ".");
	}

    public Entry find(
            PenroseConnection connection,
            Entry parent,
            EntryDefinition entryDefinition,
            String rdn)
            throws Exception {

        int i = rdn.indexOf("=");
        String attr = rdn.substring(0, i);
        String value = rdn.substring(i + 1);

        Row row = new Row();
        row.set(attr, value);

        Map rdns = new TreeMap();
        rdns.put(rdn+","+parent.getDn(), parent.getSourceValues());

        //Filter filter = handlerContext.getFilterTool().createFilter(rdns);

        //log.debug("--------------------------------------------------------------------------------------");
        //log.debug("Searching for entry "+entryDefinition.getDn()+" with filter "+filters);

        //Collection rdns = handlerContext.getEngine().search(parent, entryDefinition, filter);
        //log.debug("Searched rdns: "+rdns);

        Collection parents = new ArrayList();
        parents.add(parent);

        Collection parentSourceValues = new ArrayList();
        parentSourceValues.add(parent.getSourceValues());

        SearchResults results = new SearchResults();
        handlerContext.getEngine().load(entryDefinition, parentSourceValues, rdns, results);

        if (results.size() == 0) return null;

        // there should be only one entry
        Entry entry = (Entry)results.iterator().next();

        return entry;
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
        log.debug(" - Alias Dereferencing: " + d);
        log.info(" - Filter: " + filter);
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
            set.add(new LDAPAttribute("vendorVersion", new String[] { "Penrose Virtual Directory Server 0.9.7" }));

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

		Filter f = handlerContext.getFilterTool().parseFilter(filter);
		log.debug("Parsed filter: " + f);

		Entry baseEntry;
		try {
			baseEntry = find(connection, nbase);

        } catch (LDAPException e) {
            log.debug(e.getMessage());
            results.setReturnCode(e.getResultCode());
            return e.getResultCode();

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			results.setReturnCode(LDAPException.OPERATIONS_ERROR);
			return LDAPException.OPERATIONS_ERROR;
		}

		if (baseEntry == null) {
			log.debug("Can't find " + nbase);
			results.setReturnCode(LDAPException.NO_SUCH_OBJECT);
			return LDAPException.NO_SUCH_OBJECT;
		}

        int rc = handlerContext.getACLEngine().checkSearch(connection, baseEntry);
        if (rc != LDAPException.SUCCESS) return rc;

		//log.debug("Search base: " + baseEntry.getDn());

		if (scope == LDAPConnection.SCOPE_BASE || scope == LDAPConnection.SCOPE_SUB) { // base or subtree
			if (handlerContext.getFilterTool().isValidEntry(baseEntry, f)) {

                rc = handlerContext.getACLEngine().checkRead(connection, baseEntry);
                if (rc == LDAPException.SUCCESS) {
                    LDAPEntry ldapEntry = baseEntry.toLDAPEntry();
                    Entry.filterAttributes(ldapEntry, normalizedAttributeNames);
                    results.add(ldapEntry);
                }
			}
		}

		if (scope == LDAPConnection.SCOPE_ONE || scope == LDAPConnection.SCOPE_SUB) { // one level or subtree
			//log.debug("Searching children of " + baseEntry.getDn());
			//searchChildren(connection, baseEntry, scope, f, normalizedAttributeNames, results);
            Collection parents = new ArrayList();
            parents.add(baseEntry);
            searchChildren(connection, parents, baseEntry.getEntryDefinition(), scope, f, normalizedAttributeNames, results, true);
		}

		results.setReturnCode(LDAPException.SUCCESS);
		return LDAPException.SUCCESS;
	}

    public HandlerContext getHandlerContext() {
        return handlerContext;
    }

    public void setHandlerContext(HandlerContext handlerContext) {
        this.handlerContext = handlerContext;
    }

    public void searchChildren(
            PenroseConnection connection,
            Collection parents,
            EntryDefinition entryDefinition,
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
            //log.debug("Checking children: " + childDefinition.getDn());

            Collection newParents = new ArrayList();

            if (handlerContext.getFilterTool().isValidEntry(childDefinition, filter)) {

                SearchResults sr = handlerContext.getEngine().search(
                        connection,
                        parents,
                        childDefinition,
                        filter,
                        attributeNames
                );

                while (sr.hasNext()) {
                    Entry child = (Entry)sr.next();

                    int rc = handlerContext.getACLEngine().checkSearch(connection, child);
                    if (rc != LDAPException.SUCCESS) continue;

                    if (!handlerContext.getFilterTool().isValidEntry(child, filter)) continue;

                    rc = handlerContext.getACLEngine().checkRead(connection, child);
                    if (rc != LDAPException.SUCCESS) continue;

                    newParents.add(child);

                    LDAPEntry en = child.toLDAPEntry();
                    Entry.filterAttributes(en, attributeNames);
                    results.add(en);
                }
            }

            if (scope == LDAPConnection.SCOPE_SUB) {
                searchChildren(connection, newParents, childDefinition, scope, filter, attributeNames, results, false);
            }
        }
    }

}
