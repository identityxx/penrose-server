/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.engine;

import org.safehaus.penrose.SearchResults;
import org.safehaus.penrose.PenroseConnection;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.config.Config;
import org.safehaus.penrose.cache.Cache;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.mapping.Entry;
import org.safehaus.penrose.mapping.EntryDefinition;
import org.safehaus.penrose.mapping.AttributeValues;
import org.apache.log4j.Logger;
import org.ietf.ldap.LDAPException;
import org.ietf.ldap.LDAPDN;
import org.ietf.ldap.LDAPConnection;
import org.ietf.ldap.LDAPEntry;

import java.util.Collection;
import java.util.Iterator;
import java.util.HashSet;

/**
 * @author Endi S. Dewata
 */
public abstract class SearchHandler {

    public Logger log = Logger.getLogger(Penrose.SEARCH_LOGGER);

    private Engine engine;
    private Cache cache;
    private EngineContext engineContext;

    public void init(Engine engine) throws Exception {
        this.engine = engine;
        this.cache = engine.getCache();
		this.engineContext = engine.getEngineContext();

        init();
	}

    public void init() throws Exception {
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

            AttributeValues values = entryDefinition.getAttributeValues(engineContext.newInterpreter());
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

                AttributeValues values = childDefinition.getAttributeValues(engineContext.newInterpreter());
                Entry entry = new Entry(childDefinition, values);
                entry.setParent(parent);
                return entry;
            }
		}

		throw new LDAPException("Can't find " + dn + ".",
				LDAPException.NO_SUCH_OBJECT, "Can't find virtual entry " + dn + ".");
	}

    public abstract Entry find(
            PenroseConnection connection,
            Entry parent,
            EntryDefinition entryDefinition,
            String rdn)
            throws Exception;

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

		Filter f = engineContext.getFilterTool().parseFilter(filter);
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
			if (engineContext.getFilterTool().isValidEntry(baseEntry, f)) {
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

    public abstract SearchResults search(
            PenroseConnection connection,
            Entry parent,
            EntryDefinition entryDefinition,
            Filter filter)
            throws Exception;

    public EngineContext getEngineContext() {
        return engineContext;
    }

    public void setEngineContext(EngineContext engineContext) {
        this.engineContext = engineContext;
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

			AttributeValues values = childDefinition.getAttributeValues(engineContext.newInterpreter());
			Entry child = new Entry(childDefinition, values);

			if (engineContext.getFilterTool().isValidEntry(child, filter)) {
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

                if (engineContext.getFilterTool().isValidEntry(child, filter)) {
                    LDAPEntry en = child.toLDAPEntry();
                    results.add(Entry.filterAttributes(en, attributeNames));
                }

                if (scope == LDAPConnection.SCOPE_SUB) {
                    searchChildren(connection, child, scope, filter, attributeNames, results);
                }
            }
		}
	}

    public Engine getEngine() {
        return engine;
    }

    public void setEngine(Engine engine) {
        this.engine = engine;
    }

    public Cache getCache() {
        return cache;
    }

    public void setCache(Cache cache) {
        this.cache = cache;
    }
}
