/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.engine;


import java.util.*;

import org.ietf.ldap.LDAPException;
import org.ietf.ldap.LDAPDN;
import org.ietf.ldap.LDAPEntry;
import org.safehaus.penrose.config.*;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.filter.OrFilter;
import org.safehaus.penrose.filter.SimpleFilter;
import org.safehaus.penrose.filter.AndFilter;
import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.SearchResults;
import org.safehaus.penrose.PenroseConnection;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.thread.MRSWLock;
import org.safehaus.penrose.cache.CacheConfig;
import org.apache.log4j.Logger;

/**
 * @author Endi S. Dewata
 */
public class DefaultSearchHandler implements SearchHandler {

    public Logger log = Logger.getLogger(Penrose.SEARCH_LOGGER);

    public DefaultEngine engine;
	public EngineContext engineContext;
    public Config config;

	public void init(Engine engine, EngineContext engineContext) throws Exception {
        this.engine = ((DefaultEngine)engine);
		this.engineContext = engineContext;
        config = engineContext.getConfig();
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

		Filter f = engineContext.getFilterTool().parseFilter(filter);
		log.debug("Parsed filter: " + f);

		Entry baseEntry;
		try {
			baseEntry = getVirtualEntry(connection, nbase, attributeNames);
            
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

		if (scope == 0 || scope == 2) { // base or subtree
			if (engineContext.getFilterTool().isValidEntry(baseEntry, f)) {
                LDAPEntry entry = baseEntry.toLDAPEntry();
				results.add(Entry.filterAttributes(entry, attributeNames));
			}
		}

		if (scope == 1 || scope == 2) { // one level or subtree
			log.debug("Searching children of " + baseEntry.getDn());
			searchChildren(connection, baseEntry, scope, f, attributeNames, results);
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

	/**
	 * Find an real entry given a dn. If not found it will search for a virtual
	 * entry from all possible mappings.
	 * 
	 * @param dn
	 * @return virtual entry
	 * @throws Exception
	 */
	public Entry getVirtualEntry(
            PenroseConnection connection,
            String dn,
			Collection attributeNames) throws Exception {

		log.debug("----------------------------------------------------------------------------------");
		log.debug("Searching entry: " + dn);

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

		String filter = "(" + rdn + ")";

		Filter f = engineContext.getFilterTool().parseFilter(filter);
		log.debug("Parsed filter: " + f);


		EntryDefinition entry = config.getEntryDefinition(dn);

		if (entry != null) {
			log.debug("Found static entry: " + dn);

			AttributeValues values = entry.getAttributeValues(engineContext.newInterpreter());
			return new Entry(entry, values);
		}

		EntryDefinition parentEntry = config.getEntryDefinition(parentDn);
		if (parentEntry == null) {
			log.debug("Parent not found: " + dn);

			throw new LDAPException("Can't find " + dn + ".",
					LDAPException.NO_SUCH_OBJECT, "Can't find virtual entries "
							+ dn + ".");
		}

        int j = rdn.indexOf("=");
        String rdnAttribute = rdn.substring(0, j);
        String rdnValue = rdn.substring(j + 1);

		log.debug("Found parent entry: " + parentDn);
		Collection children = parentEntry.getChildren();

        // Find in each dynamic children
		for (Iterator iterator = children.iterator(); iterator.hasNext(); ) {
			EntryDefinition child = (EntryDefinition) iterator.next();

            String childRdn = child.getRdn();
            log.debug("Checking child: "+childRdn);

            int k = childRdn.indexOf("=");

            String childRdnAttribute = childRdn.substring(0, k);
            String childRdnValue = childRdn.substring(k+1);

            if (!rdnAttribute.equals(childRdnAttribute)) continue;

			if (child.isDynamic()) {

                // the rdn attribute types must match

                log.debug("Found dynamic entry: " + child.getDn());

                try {
                    Entry sr = searchVirtualEntry(connection, child, rdn, f, attributeNames);
                    return sr;

                } catch (Exception e) {
                    e.printStackTrace(System.out);
                    // not found, continue to next mappingEntry
                }

            } else {
                if (!rdnValue.toLowerCase().equals(childRdnValue.toLowerCase())) continue;

                log.debug("Found static entry: " + child.getDn());

                AttributeValues values = child.getAttributeValues(engineContext.newInterpreter());
                return new Entry(child, values);
            }
		}

		throw new LDAPException("Can't find " + dn + ".",
				LDAPException.NO_SUCH_OBJECT, "Can't find virtual entries " + dn + ".");
	}

	/**
	 * Find entries given a parent entry. It could return real entries and/or
	 * virtual entries.
	 * 
	 * @param baseEntry
	 * @param scope
	 * @throws Exception
	 */
	public void searchChildren(
            PenroseConnection connection,
			Entry baseEntry,
            int scope,
            Filter filter,
			Collection attributeNames,
            SearchResults results) throws Exception {

		EntryDefinition parentEntry = baseEntry.getEntryDefinition();
		Collection children = parentEntry.getChildren();
		log.debug("Total children: " + children.size());

		for (Iterator i = children.iterator(); i.hasNext();) {
			EntryDefinition entryDefinition = (EntryDefinition) i.next();
			if (entryDefinition.isDynamic()) continue;

			log.debug("Real children: " + entryDefinition.getDn());

			AttributeValues values = entryDefinition.getAttributeValues(engineContext.newInterpreter());
			Entry sr2 = new Entry(entryDefinition, values);

			if (engineContext.getFilterTool().isValidEntry(sr2, filter)) {
                LDAPEntry en2 = sr2.toLDAPEntry();
				results.add(Entry.filterAttributes(en2, attributeNames));
			}

			if (scope == 2) {
				searchChildren(connection, sr2, scope, filter, attributeNames, results);
			}
		}

		for (Iterator i = children.iterator(); i.hasNext();) {
			EntryDefinition entryDefinition = (EntryDefinition) i.next();
			if (!entryDefinition.isDynamic()) continue;

			log.debug("Virtual children: " + entryDefinition.getDn());

			SearchResults results2 = search(entryDefinition, filter, attributeNames);

            for (Iterator j=results2.iterator(); j.hasNext(); ) {
                Entry sr = (Entry)j.next();
                LDAPEntry en = sr.toLDAPEntry();
                results.add(Entry.filterAttributes(en, attributeNames));
            }
		}
	}

	/**
	 * Find an virtual entry given an rdn and a mapping entry.
	 * 
	 * @param rdn
	 * @param entryDefinition
	 * @return
	 * @throws Exception
	 */
	public Entry searchVirtualEntry(
            PenroseConnection connection,
			EntryDefinition entryDefinition,
            String rdn,
            Filter filter,
            Collection attributeNames)
			throws Exception {

        // find entry using rdn as filter
		SearchResults results = searchObject(entryDefinition, filter, attributeNames);

        if (results.size() == 0) return null;

        // there should be only one entry
        Entry entry = (Entry)results.iterator().next();

        log.debug("Checking " + entry.getDn() + ": " + entry.getAttributeValues());

		Map rdnValues = Entry.parseRdn(rdn);
        AttributeValues values = entry.getAttributeValues();

        // check all values in rdn
        for (Iterator j = rdnValues.keySet().iterator(); j.hasNext();) {
            String rdnName = (String) j.next();

            String rdnValue = ((String)rdnValues.get(rdnName)).toLowerCase();
            Collection v = values.get(rdnName);

            // convert to lower cases
            List v2 = new ArrayList();
            for (Iterator k=v.iterator(); k.hasNext(); ) {
                String value = (String)k.next();
                v2.add(value.toLowerCase());
            }

            log.debug("Checking "+rdnName+"="+rdnValue+" with "+v2);
            if (!v2.contains(rdnValue)) {
                return null;
            }
        }

		return entry;
	}

    public SearchResults search(
            EntryDefinition entry,
            Filter filter,
            Collection attributeNames
            ) throws Exception {

        log.debug("--------------------------------------------------------------------------------------");
        log.debug("Searching for entry "+entry.getDn()+" with filter "+filter);

        SearchResults results = new SearchResults();

        try {
            Calendar calendar = Calendar.getInstance();

            // find the primary keys of the entries to be loaded
            Collection keys = getPrimaryKeys(entry, filter, calendar);
            log.debug("Keys: "+keys);

            // find the primary keys of entries that has been loaded
            String sqlFilter = engine.getCache().getCacheFilterTool().toSQLFilter(entry, filter);
            Collection pks = engine.getCache().searchPrimaryKeys(entry, sqlFilter);
            log.debug("Loaded Keys: "+pks);

            if (!keys.isEmpty() && pks.isEmpty()) { // never been loaded -> load everything at once

                log.debug("Loading all keys");
                Filter f = createFilter(keys);

                loadSources(entry, f, calendar);
                joinSources(entry, f, calendar);

            } else { // some has been loaded -> load each entry individually

                for (Iterator i=keys.iterator(); i.hasNext(); ) {
                    Map pk = (Map)i.next();
                    log.debug("Loading key: "+pk);

                    Filter f = createFilter(pk);
                    SearchResults sr = searchObject(entry, f, attributeNames);

                    for (Iterator j=sr.iterator(); j.hasNext(); ) {
                        results.add(j.next());
                    }
                }

            }

            return getEntries(entry, filter, attributeNames);

        } finally {
            results.close();
        }
    }

    public SearchResults searchObject(
            EntryDefinition entry,
            Filter filter,
            Collection attributeNames
            ) throws Exception {

        log.debug("--------------------------------------------------------------------------------------");
        log.debug("Searching entry "+entry.getDn()+" with filter "+filter);

        Calendar calendar = Calendar.getInstance();

        String s = engine.getCache().getParameter(CacheConfig.CACHE_EXPIRATION);
        int cacheExpiration = s == null ? 0 : Integer.parseInt(s);
        log.debug("Expiration: "+cacheExpiration);
        if (cacheExpiration < 0) cacheExpiration = Integer.MAX_VALUE;

        Calendar c = (Calendar) calendar.clone();
        c.add(Calendar.MINUTE, -cacheExpiration);

        String sqlFilter = engine.getCache().getCacheFilterTool().toSQLFilter(entry, filter);
        log.debug("Checking cache with sql filter: "+sqlFilter);

        Date modifyTime = engine.getCache().getModifyTime(entry, sqlFilter);

        boolean expired = modifyTime == null || modifyTime.before(c.getTime());

        log.debug("Comparing "+modifyTime+" with "+c.getTime()+" => "+(expired ? "expired" : "not expired"));

        if (expired) { // if cache expired => load this entry only
            loadSources(entry, filter, calendar);
            joinSources(entry, filter, calendar);
        }

        return getEntries(entry, filter, attributeNames);
    }

    public SearchResults getEntries(
            EntryDefinition entry,
            Filter filter,
            Collection attributeNames) throws Exception {

        log.debug("--------------------------------------------------------------------------------------");
        log.debug("Getting entries from cache for "+entry.getDn()+" with "+filter);

        MRSWLock lock = engine.getLock(entry.getDn());
        lock.getReadLock(Penrose.WAIT_TIMEOUT);

        SearchResults results = new SearchResults();

        try {

            String sqlFilter = engine.getCache().getCacheFilterTool().toSQLFilter(entry, filter);
            Collection pks = engine.getCache().searchPrimaryKeys(entry, sqlFilter);
            log.debug("Primary keys: " + pks);

            Collection translatedRows = engine.getCache().search(entry, pks);

            log.debug("Merging " + translatedRows.size() + " rows:");
            Map entries = engineContext.getTransformEngine().merge(entry, translatedRows);

            for (Iterator i = entries.values().iterator(); i.hasNext();) {
                AttributeValues values = (AttributeValues) i.next();

                Entry sr = new Entry(entry, values);

                if (engineContext.getFilterTool().isValidEntry(sr, filter)) {
                    log.debug(" - " + values+": ok");
                    results.add(sr);

                } else {
                    log.debug(" - " + values+": not ok");
                }
            }
        } finally {
            lock.releaseReadLock(Penrose.WAIT_TIMEOUT);
            results.close();
        }

        return results;
    }

    public Collection getPrimaryKeys(
            EntryDefinition entry,
            Filter filter,
            Calendar calendar
            ) throws Exception {

        log.debug("--------------------------------------------------------------------------------------");

        Collection rdnAttributes = entry.getRdnAttributes();

        // TODO need to handle multiple rdn attributes
        AttributeDefinition rdnAttribute = (AttributeDefinition)rdnAttributes.iterator().next();
        String exp = rdnAttribute.getExpression();

        // TODO need to handle complex expression
        int index = exp.indexOf(".");
        String primarySourceName = exp.substring(0, index);
        String primaryKey = exp.substring(index+1);
        log.debug("Primary source: "+primarySourceName);
        log.debug("Primary key: "+primaryKey);

        List list = new ArrayList();
        List nullList = new ArrayList();
        Map filters = new HashMap();

        Source primarySource = null;

        for (Iterator i = entry.getSources().iterator(); i.hasNext();) {
            Source source = (Source) i.next();

            String sourceName = source.getName();

            if (sourceName.equals(primarySourceName)) {
                primarySource = source;
            }

            Filter newFilter = engine.getCache().getCacheFilterTool().toSourceFilter(source, filter);
            filters.put(sourceName, newFilter);

            log.debug("Filter for " + sourceName+": "+newFilter);

            if (newFilter == null) {
                nullList.add(source);
            } else {
                list.add(source);
            }
        }

        Filter primaryFilter;

        Collection relationships = entry.getRelationships();
        Relationship relationship = null;
        if (relationships.size() > 0) {
            relationship = (Relationship)relationships.iterator().next();
        }

        // TODO convert this into a loop
        if (!list.isEmpty() && !nullList.isEmpty()) {

            Source source = (Source)list.iterator().next();
            String sourceName = source.getName();

            String lhs = relationship.getLhs();
            String rhs = relationship.getRhs();

            String keyName;
            String nextKeyName;

            if (lhs.startsWith(sourceName+".")) {
                keyName = lhs.substring(lhs.indexOf(".")+1);
                nextKeyName = rhs.substring(rhs.indexOf(".")+1);
            } else {
                keyName = rhs.substring(rhs.indexOf(".")+1);
                nextKeyName = lhs.substring(lhs.indexOf(".")+1);
            }

            Filter sourceFilter = (Filter)filters.get(sourceName);

            log.debug("Searching source "+sourceName+" for "+sourceFilter);
            SearchResults results = source.search(sourceFilter);

            Set keys = new HashSet();
            for (Iterator j=results.iterator(); j.hasNext(); ) {
                Row values = (Row)j.next();
                Object value = values.get(keyName);

                Map pk = new HashMap();
                pk.put(nextKeyName, value);

                keys.add(pk);
            }

            primaryFilter = createFilter(keys);

        } else {
            primaryFilter = engine.getCache().getCacheFilterTool().toSourceFilter(primarySource, filter);
        }

        log.debug("Searching source "+primarySourceName+" for "+primaryFilter);
        SearchResults results = primarySource.search(primaryFilter);

        Set keys = new HashSet();
        for (Iterator j=results.iterator(); j.hasNext(); ) {
            Row row = (Row)j.next();

            Interpreter interpreter = engineContext.newInterpreter();
            for (Iterator k=row.getNames().iterator(); k.hasNext(); ) {
                String name = (String)k.next();
                Object value = row.get(name);
                interpreter.set(primarySourceName+"."+name, value);
            }

            Map pk = new HashMap();
            for (Iterator k=rdnAttributes.iterator(); k.hasNext(); ) {
                AttributeDefinition attr = (AttributeDefinition)k.next();
                String name = attr.getName();
                String expression = attr.getExpression();
                String value = (String)interpreter.eval(expression);
                pk.put(name, value);
            }

            keys.add(pk);
        }

        return keys;
    }

    public Filter createFilter(Collection keys) {

        Filter filter = null;

        for (Iterator i=keys.iterator(); i.hasNext(); ) {
            Map pk = (Map)i.next();

            Filter f = createFilter(pk);

            if (filter == null) {
                filter = f;

            } else if (!(filter instanceof OrFilter)) {
                OrFilter of = new OrFilter();
                of.addFilterList(filter);
                of.addFilterList(f);
                filter = of;

            } else {
                OrFilter of = (OrFilter)filter;
                of.addFilterList(f);
            }
        }

        return filter;
    }

    public Filter createFilter(Map values) {

        Filter f = null;

        for (Iterator j=values.keySet().iterator(); j.hasNext(); ) {
            String name = (String)j.next();
            String value = (String)values.get(name);

            SimpleFilter sf = new SimpleFilter(name, "=", value);

            if (f == null) {
                f = sf;

            } else if (!(f instanceof AndFilter)) {
                AndFilter af = new AndFilter();
                af.addFilterList(f);
                af.addFilterList(sf);
                f = af;

            } else {
                AndFilter af = (AndFilter)f;
                af.addFilterList(sf);
            }
        }

        return f;
    }

    public void joinSources(EntryDefinition entry, Filter filter, Calendar calendar) throws Exception {
        String newFilter = engine.getCache().getCacheFilterTool().toSQLFilter(entry, filter);

        log.debug("--------------------------------------------------------------------------------------");
        log.debug("Joining sources ...");

        MRSWLock lock = engine.getLock(entry.getDn());
        lock.getWriteLock(Penrose.WAIT_TIMEOUT);

        try {

            Collection rows = engine.getCache().joinSources(entry);
            log.debug("Joined " + rows.size() + " rows.");

            engine.getCache().delete(entry, newFilter, calendar.getTime());

            for (Iterator i = rows.iterator(); i.hasNext();) {
                Row row = (Row)i.next();
                Map pk = new HashMap();
                Row translatedRow = new Row();

                boolean validPK = engineContext.getTransformEngine().translate(entry, row, pk, translatedRow);
                if (!validPK) continue;

                engine.getCache().insert(entry, translatedRow, calendar.getTime());
            }

            //r1.copy(r2, newFilter);

        } finally {
            lock.releaseWriteLock(Penrose.WAIT_TIMEOUT);
        }
    }

    /**
     * Load sources of entries matching the filter.
     *
     * @param entry
     * @param filter
     * @param calendar
     * @throws Exception
     */
    public void loadSources(
            EntryDefinition entry,
            Filter filter,
            Calendar calendar
            ) throws Exception {

        Collection relationships = entry.getRelationships();
        Relationship relationship = null;
        if (relationships.size() > 0) {
            relationship = (Relationship)relationships.iterator().next();
        }

        Set keyNames = new HashSet();

        Collection rdnAttributes = entry.getRdnAttributes();
        for (Iterator i=rdnAttributes.iterator(); i.hasNext(); ) {
            AttributeDefinition attribute = (AttributeDefinition)i.next();
            keyNames.add(new String[] { attribute.getName(), attribute.getName() });
        }

        Filter f = null;

        for (Iterator i = entry.getSources().iterator(); i.hasNext();) {
            Source source = (Source) i.next();

            String sourceName = source.getName();

            log.debug("--------------------------------------------------------------------------------------");
            log.debug("Loading source " + source.getName());

            if (relationship != null) {

                keyNames = new HashSet();

                String keyName = null;
                String nextKeyName = null;

                String lhs = relationship.getLhs();
                String rhs = relationship.getRhs();

                if (lhs.startsWith(sourceName+".")) {
                    keyName = lhs.substring(sourceName.length()+1);
                    int p = rhs.indexOf(".");
                    nextKeyName = rhs.substring(p+1);

                } else {
                    keyName = rhs.substring(sourceName.length()+1);
                    int p = lhs.indexOf(".");
                    nextKeyName = lhs.substring(p+1);
                }

                keyNames.add(new String[] { keyName, nextKeyName });
            }

            if (f == null) {
                f = engine.getCache().getCacheFilterTool().toSourceFilter(source, filter);
            }

            MRSWLock lock = engine.getLock(source);
            lock.getWriteLock(Penrose.WAIT_TIMEOUT);

            try {

                SearchResults results = engine.getCache().loadSource(entry, source, f, calendar.getTime());

                // update key values
                Set keys = new HashSet();
                for (Iterator j=results.iterator(); j.hasNext(); ) {
                    Row values = (Row)j.next();

                    Map key = new HashMap();
                    for (Iterator k=keyNames.iterator(); k.hasNext(); ) {
                        String[] names = (String[])k.next();
                        Object keyValue = values.get(names[0]);
                        key.put(names[1], keyValue);
                    }

                    keys.add(key);
                }

                f = createFilter(keys);

            } finally {
                lock.releaseWriteLock(Penrose.WAIT_TIMEOUT);
            }
       }

    }
}