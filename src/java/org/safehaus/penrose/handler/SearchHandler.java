/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.handler;

import org.safehaus.penrose.SearchResults;
import org.safehaus.penrose.PenroseConnection;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.config.Config;
import org.safehaus.penrose.cache.CacheConfig;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.mapping.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ietf.ldap.LDAPException;
import org.ietf.ldap.LDAPDN;
import org.ietf.ldap.LDAPConnection;
import org.ietf.ldap.LDAPEntry;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class SearchHandler {

    Logger log = LoggerFactory.getLogger(getClass());

    private HandlerContext handlerContext;

    public SearchHandler(Handler handler) throws Exception {
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

        Config config = getEngineContext().getConfig(dn);
        if (config == null) return null;

        // search the entry directly
        EntryDefinition entryDefinition = config.getEntryDefinition(dn);

        if (entryDefinition != null) {
            log.debug("Found static entry: " + dn);

            AttributeValues values = entryDefinition.getAttributeValues(handlerContext.newInterpreter());
            return new Entry(entryDefinition, values);
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
            // ignore
        }

		if (parent == null) {
            log.debug("Parent not found: " + dn);

            throw new LDAPException("Can't find " + dn + ".",
                    LDAPException.NO_SUCH_OBJECT, "Can't find " + dn + ".");
		}

        EntryDefinition parentDefinition = parent.getEntryDefinition();

		//log.debug("Found parent entry: " + parentDn);
		Collection children = parentDefinition.getChildren();

        int j = rdn.indexOf("=");
        String rdnAttribute = rdn.substring(0, j);
        String rdnValue = rdn.substring(j + 1);

        // Find in each dynamic children
		for (Iterator iterator = children.iterator(); iterator.hasNext(); ) {
			EntryDefinition childDefinition = (EntryDefinition) iterator.next();

            String childRdn = childDefinition.getRdn();
            //log.debug("Checking child: "+childRdn);

            int k = childRdn.indexOf("=");
            String childRdnAttribute = childRdn.substring(0, k);
            String childRdnValue = childRdn.substring(k+1);

            // the rdn attribute types must match
            if (!rdnAttribute.equals(childRdnAttribute)) continue;

			if (childDefinition.isDynamic()) {

                log.debug("Found entry definition: " + childDefinition.getDn());

                try {
                    Entry entry = find(connection, parent, childDefinition, rdn);
                    if (entry == null) continue;

                    entry.setParent(parent);
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
                Entry entry = new Entry(childDefinition, values);
                entry.setParent(parent);
                return entry;
            }
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

        Calendar calendar = Calendar.getInstance();

        int i = rdn.indexOf("=");
        String attr = rdn.substring(0, i);
        String value = rdn.substring(i + 1);

        Row pk = new Row();
        pk.set(attr, value);

        List rdns = new ArrayList();
        rdns.add(pk);

        SearchResults results = handlerContext.getEngine().load(parent, entryDefinition, rdns, calendar);

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

		Filter f = handlerContext.getFilterTool().parseFilter(filter);
		log.debug("Parsed filter: " + f);

        log.debug("----------------------------------------------------------------------------------");
		Entry baseEntry;
		try {
			baseEntry = find(connection, nbase);

        } catch (LDAPException e) {
            log.debug(e.getMessage());
            results.setReturnCode(e.getResultCode());
            return e.getResultCode();

		} catch (Exception e) {
			log.debug(e.getMessage(), e);
			results.setReturnCode(LDAPException.OPERATIONS_ERROR);
			return LDAPException.OPERATIONS_ERROR;
		}

		if (baseEntry == null) {
			log.debug("Can't find " + nbase);
			results.setReturnCode(LDAPException.NO_SUCH_OBJECT);
			return LDAPException.NO_SUCH_OBJECT;
		}

		log.debug("Search base: " + baseEntry.getDn());

		if (scope == LDAPConnection.SCOPE_BASE || scope == LDAPConnection.SCOPE_SUB) { // base or subtree
			if (handlerContext.getFilterTool().isValidEntry(baseEntry, f)) {
                LDAPEntry entry = baseEntry.toLDAPEntry();
				results.add(Entry.filterAttributes(entry, normalizedAttributeNames));
			}
		}

        log.debug("----------------------------------------------------------------------------------");
		if (scope == LDAPConnection.SCOPE_ONE || scope == LDAPConnection.SCOPE_SUB) { // one level or subtree
			log.debug("Searching children of " + baseEntry.getDn());
			searchChildren(connection, baseEntry, scope, f, normalizedAttributeNames, results);
		}
		/*
		 * log.debug("-------------------------------------------------------------------------------");
		 * log.debug("CHECKING FILTER: "+filter);
		 *
		 * for (Iterator i=entries.iterator(); i.hasNext(); ) { Entry r =
		 * (Entry)i.next();
		 *
		 * if (penrose.filterTool.isValidEntry(r, f)) {
		 * results.put(penrose.entryTool.toLDAPEntry(connection, r,
		 * attributeNames)); log.debug(" - "+r.getDn()+" ok"); }
		 * else { log.debug(" - "+r.getDn()+" failed"); } }
		 *
		 * log.debug("-------------------------------------------------------------------------------");
		 */
		results.setReturnCode(LDAPException.SUCCESS);
		return LDAPException.SUCCESS;
	}

    public SearchResults search(
            PenroseConnection connection,
            Entry parent,
            EntryDefinition entryDefinition,
            Filter filter
            ) throws Exception {

        log.debug("--------------------------------------------------------------------------------------");
        log.debug("Searching for entry "+entryDefinition.getDn()+" with filter "+filter);

        Calendar calendar = Calendar.getInstance();

        Collection rdns = search(parent, entryDefinition, filter, calendar);
        log.debug("Searched rdns: "+rdns);

        return handlerContext.getEngine().load(parent, entryDefinition, rdns, calendar);
    }

    public HandlerContext getEngineContext() {
        return handlerContext;
    }

    public void setEngineContext(HandlerContext handlerContext) {
        this.handlerContext = handlerContext;
    }

    /**
	 * Find children given a entry. It could return real entries and/or
	 * virtual entries.
	 *
	 * @param entry
	 * @param scope
	 * @throws Exception
	 */
	public void searchChildren(
            PenroseConnection connection,
			Entry entry,
            int scope,
            Filter filter,
			Collection attributeNames,
            SearchResults results) throws Exception {

		EntryDefinition entryDefinition = entry.getEntryDefinition();
		Collection children = entryDefinition.getChildren();
		log.debug("Total children: " + children.size());

		for (Iterator i = children.iterator(); i.hasNext();) {
			EntryDefinition childDefinition = (EntryDefinition) i.next();
			if (childDefinition.isDynamic()) continue;

			log.debug("Static children: " + childDefinition.getDn());

			AttributeValues values = childDefinition.getAttributeValues(handlerContext.newInterpreter());
			Entry child = new Entry(childDefinition, values);

			if (handlerContext.getFilterTool().isValidEntry(child, filter)) {
                LDAPEntry en = child.toLDAPEntry();
				results.add(Entry.filterAttributes(en, attributeNames));
			}

			if (scope == LDAPConnection.SCOPE_SUB) {
				searchChildren(connection, child, scope, filter, attributeNames, results);
			}
		}

		for (Iterator i = children.iterator(); i.hasNext();) {
			EntryDefinition childDefinition = (EntryDefinition) i.next();
			if (!childDefinition.isDynamic()) continue;

			log.debug("Virtual children: " + childDefinition.getDn());

			SearchResults results2 = search(connection, entry, childDefinition, filter);

            for (Iterator j=results2.iterator(); j.hasNext(); ) {
                Entry child = (Entry)j.next();

                if (handlerContext.getFilterTool().isValidEntry(child, filter)) {
                    LDAPEntry en = child.toLDAPEntry();
                    results.add(Entry.filterAttributes(en, attributeNames));
                }

                if (scope == LDAPConnection.SCOPE_SUB) {
                    searchChildren(connection, child, scope, filter, attributeNames, results);
                }
            }
		}
	}

    public Collection search(
            Entry parent,
            EntryDefinition entryDefinition,
            Filter filter,
            Calendar calendar
            ) throws Exception {

        String str = handlerContext.getCache().getParameter(CacheConfig.CACHE_EXPIRATION);
        int cacheExpiration = str == null ? 0 : Integer.parseInt(str);
        log.debug("Filter Cache Expiration: "+cacheExpiration);
        if (cacheExpiration < 0) cacheExpiration = Integer.MAX_VALUE;

        Calendar c = (Calendar) calendar.clone();
        c.add(Calendar.MINUTE, -cacheExpiration);

        String key = entryDefinition.getDn()+","+parent.getDn() + ":" + filter;
        Collection rdns = handlerContext.getCache().getFilterCache().get(key);
        if (rdns != null) {
            log.debug("Filter Cache found: "+rdns);
            return rdns;
        }

        log.debug("Filter Cache not found.");

        Source primarySource = getEngineContext().getPrimarySource(entryDefinition);
        String primarySourceName = primarySource.getName();

        log.debug("--------------------------------------------------------------------------------------");

        Collection keys = handlerContext.getEngine().search(parent, entryDefinition, filter);

        rdns = new TreeSet();

        for (Iterator j=keys.iterator(); j.hasNext(); ) {
            Row row = (Row)j.next();

            Interpreter interpreter = getEngineContext().newInterpreter();
            for (Iterator k=row.getNames().iterator(); k.hasNext(); ) {
                String name = (String)k.next();
                Object value = row.get(name);
                interpreter.set(primarySourceName+"."+name, value);
            }

            Collection rdnAttributes = entryDefinition.getRdnAttributes();

            Row rdn = new Row();
            boolean valid = true;

            for (Iterator k=rdnAttributes.iterator(); k.hasNext(); ) {
                AttributeDefinition attr = (AttributeDefinition)k.next();
                String name = attr.getName();
                String expression = attr.getExpression();
                Object value = interpreter.eval(expression);

                if (value == null) {
                    valid = false;
                    break;
                }

                rdn.set(name, value);
            }

            if (!valid) continue;
            rdns.add(rdn);
        }

        handlerContext.getCache().getFilterCache().put(key, rdns);

        return rdns;
    }

    /**
     * Convert rdns into primary keys
     */

}
