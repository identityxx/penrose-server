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
import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.SearchResults;
import org.safehaus.penrose.PenroseConnection;
import org.safehaus.penrose.cache.CacheConfig;
import org.safehaus.penrose.cache.DefaultCache;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.thread.MRSWLock;
import org.apache.log4j.Logger;

/**
 * @author Endi S. Dewata
 */
public class DefaultSearchHandler implements SearchHandler {

    public Logger log = Logger.getLogger(Penrose.SEARCH_LOGGER);

    public DefaultEngine engine;
    public DefaultCache cache;
	public EngineContext engineContext;
    public Config config;

	public void init(Engine engine, EngineContext engineContext) throws Exception {
        this.engine = (DefaultEngine)engine;
        this.cache = (DefaultCache)engine.getCache();
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

        log.debug("----------------------------------------------------------------------------------");
		Entry baseEntry;
		try {
			baseEntry = findEntry(connection, nbase);
            
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

        log.debug("----------------------------------------------------------------------------------");
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
	public Entry findEntry(
            PenroseConnection connection,
            String dn) throws Exception {

		log.debug("Find entry: " + dn);

        // search the entry directly
        EntryDefinition entry = config.getEntryDefinition(dn);

        if (entry != null) {
            log.debug("Found static entry: " + dn);

            AttributeValues values = entry.getAttributeValues(engineContext.newInterpreter());
            return new Entry(entry, values);
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
        Entry parent = findEntry(connection, parentDn);

		if (parent == null) {
            log.debug("Parent not found: " + dn);

            throw new LDAPException("Can't find " + dn + ".",
                    LDAPException.NO_SUCH_OBJECT, "Can't find virtual entry "
                            + dn + ".");
		}

        EntryDefinition parentDefinition = parent.getEntryDefinition();

		//log.debug("Found parent entry: " + parentDn);
		Collection children = parentDefinition.getChildren();

        int j = rdn.indexOf("=");
        String rdnAttribute = rdn.substring(0, j);
        String rdnValue = rdn.substring(j + 1);

        // Find in each dynamic children
		for (Iterator iterator = children.iterator(); iterator.hasNext(); ) {
			entry = (EntryDefinition) iterator.next();

            String childRdn = entry.getRdn();
            //log.debug("Checking child: "+childRdn);

            int k = childRdn.indexOf("=");
            String childRdnAttribute = childRdn.substring(0, k);
            String childRdnValue = childRdn.substring(k+1);

            // the rdn attribute types must match
            if (!rdnAttribute.equals(childRdnAttribute)) continue;

			if (entry.isDynamic()) {

                log.debug("Found entry definition: " + entry.getDn());

                try {
                    Entry sr = searchVirtualEntry(connection, parent, entry, rdn);
                    if (sr == null) continue;

                    sr.setParent(parent);
                    log.debug("Found virtual entry: " + sr.getDn());

                    return sr;

                } catch (Exception e) {
                    e.printStackTrace(System.out);
                    // not found, continue to next entry definition
                }

            } else {
                if (!rdnValue.toLowerCase().equals(childRdnValue.toLowerCase())) continue;

                log.debug("Found static entry: " + entry.getDn());

                AttributeValues values = entry.getAttributeValues(engineContext.newInterpreter());
                Entry sr = new Entry(entry, values);
                sr.setParent(parent);
                return sr;
            }
		}

		throw new LDAPException("Can't find " + dn + ".",
				LDAPException.NO_SUCH_OBJECT, "Can't find virtual entry " + dn + ".");
	}

	/**
	 * Find entries given a parent entry. It could return real entries and/or
	 * virtual entries.
	 * 
	 * @param parent
	 * @param scope
	 * @throws Exception
	 */
	public void searchChildren(
            PenroseConnection connection,
			Entry parent,
            int scope,
            Filter filter,
			Collection attributeNames,
            SearchResults results) throws Exception {

		EntryDefinition parentEntry = parent.getEntryDefinition();
		Collection children = parentEntry.getChildren();
		log.debug("Total children: " + children.size());

		for (Iterator i = children.iterator(); i.hasNext();) {
			EntryDefinition entryDefinition = (EntryDefinition) i.next();
			if (entryDefinition.isDynamic()) continue;

			log.debug("Static children: " + entryDefinition.getDn());

			AttributeValues values = entryDefinition.getAttributeValues(engineContext.newInterpreter());
			Entry sr = new Entry(entryDefinition, values);

			if (engineContext.getFilterTool().isValidEntry(sr, filter)) {
                LDAPEntry en = sr.toLDAPEntry();
				results.add(Entry.filterAttributes(en, attributeNames));
			}

			if (scope == 2) {
				searchChildren(connection, sr, scope, filter, attributeNames, results);
			}
		}

		for (Iterator i = children.iterator(); i.hasNext();) {
			EntryDefinition entryDefinition = (EntryDefinition) i.next();
			if (!entryDefinition.isDynamic()) continue;

			log.debug("Virtual children: " + entryDefinition.getDn());

			SearchResults results2 = searchVirtualEntries(parent, entryDefinition, filter);

            for (Iterator j=results2.iterator(); j.hasNext(); ) {
                Entry sr = (Entry)j.next();

                if (engineContext.getFilterTool().isValidEntry(sr, filter)) {
                    LDAPEntry en = sr.toLDAPEntry();
                    results.add(Entry.filterAttributes(en, attributeNames));
                }
                
                if (scope == 2) {
                    searchChildren(connection, sr, scope, filter, attributeNames, results);
                }
            }
		}
	}

	/**
	 * Find a virtual entry given an rdn and a mapping entry.
	 * 
	 * @param rdn
	 * @param entryDefinition
	 * @return the entry
	 * @throws Exception
	 */
	public Entry searchVirtualEntry(
            PenroseConnection connection,
            Entry parent,
			EntryDefinition entryDefinition,
            String rdn)
			throws Exception {

        int i = rdn.indexOf("=");
        String attr = rdn.substring(0, i);
        String value = rdn.substring(i + 1);

        Row pk = new Row();
        pk.set(attr, value);

        List rdns = new ArrayList();
        rdns.add(pk);

        // find entry using rdn as primary key
        loadObject(entryDefinition, pk);
        SearchResults results = getEntries(parent, entryDefinition, rdns);

        if (results.size() == 0) return null;

        // there should be only one entry
        Entry entry = (Entry)results.iterator().next();
/*
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
                String val = (String)k.next();
                v2.add(val.toLowerCase());
            }

            log.debug("Checking "+rdnName+"="+rdnValue+" with "+v2);
            if (!v2.contains(rdnValue)) {
                return null;
            }
        }
*/
		return entry;
	}

    public SearchResults searchVirtualEntries(
            Entry parent,
            EntryDefinition entryDefinition,
            Filter filter
            ) throws Exception {

        log.debug("--------------------------------------------------------------------------------------");
        log.debug("Searching for entry "+entryDefinition.getDn()+" with filter "+filter);

        Calendar calendar = Calendar.getInstance();

        // find the primary keys of the entries to be loaded
        Collection rdns = getPrimaryKeys(parent, entryDefinition, filter, calendar);
        log.debug("Keys: "+rdns);

        // find the primary keys of entries that has been loaded
        Collection loadedPks = engine.getEntryCache().findPrimaryKeys(entryDefinition, rdns);
        log.debug("Loaded Keys: "+loadedPks);

        if (!rdns.isEmpty() && loadedPks.isEmpty()) { // never been loaded -> load everything at once

            Collection pks = rdnToPk(entryDefinition, rdns);

            loadSources(entryDefinition, pks, calendar);
            joinSources(entryDefinition, pks, calendar);

        } else { // some has been loaded -> load each entry individually

            for (Iterator i=rdns.iterator(); i.hasNext(); ) {
                Row pk = (Row)i.next();
                if (loadedPks.contains(pk)) continue;

                loadObject(entryDefinition, pk);
            }
        }

        return getEntries(parent, entryDefinition, rdns);
    }

    public void loadObject(
            EntryDefinition entryDefinition,
            Row rdn) throws Exception {

        log.debug("--------------------------------------------------------------------------------------");
        log.debug("Loading entry "+entryDefinition.getDn()+" with rdn "+rdn);

        Calendar calendar = Calendar.getInstance();

        String s = cache.getParameter(CacheConfig.CACHE_EXPIRATION);
        int cacheExpiration = s == null ? 0 : Integer.parseInt(s);
        log.debug("Expiration: "+cacheExpiration);
        if (cacheExpiration < 0) cacheExpiration = Integer.MAX_VALUE;

        Calendar c = (Calendar) calendar.clone();
        c.add(Calendar.MINUTE, -cacheExpiration);

        Date modifyTime = engine.getEntryCache().getModifyTime(entryDefinition, rdn);
        boolean expired = modifyTime == null || modifyTime.before(c.getTime());

        log.debug("Comparing "+modifyTime+" with "+c.getTime()+" => "+(expired ? "expired" : "not expired"));

        if (expired) { // if cache expired => load this entry only

            List rdns = new ArrayList();
            rdns.add(rdn);

            Collection pks = rdnToPk(entryDefinition, rdns);

            loadSources(entryDefinition, pks, calendar);
            joinSources(entryDefinition, pks, calendar);
        }
    }

    /**
     * Get entries from entry cache.
     *
     * @param parent
     * @param entryDefinition
     * @param keys
     * @return entries
     * @throws Exception
     */
    public SearchResults getEntries(
            Entry parent,
            EntryDefinition entryDefinition,
            Collection keys) throws Exception {

        log.debug("--------------------------------------------------------------------------------------");
        log.debug("Getting entries from cache with pks "+keys);

        MRSWLock lock = engine.getLock(entryDefinition.getDn());
        lock.getReadLock(Penrose.WAIT_TIMEOUT);

        SearchResults results = new SearchResults();

        try {
            Map entries = engine.getEntryCache().get(entryDefinition, keys);

            for (Iterator i = entries.values().iterator(); i.hasNext();) {
                Entry sr = (Entry) i.next();
                sr.setParent(parent);
                log.debug("Returning "+sr.getDn());
                results.add(sr);
            }

        } finally {
            lock.releaseReadLock(Penrose.WAIT_TIMEOUT);
            results.close();
        }

        return results;
    }

    public String getStartingSourceName(EntryDefinition entryDefinition) {

        Collection relationships = entryDefinition.getRelationships();
        for (Iterator i=relationships.iterator(); i.hasNext(); ) {
            Relationship relationship = (Relationship)i.next();

            String lhs = relationship.getLhs();
            int li = lhs.indexOf(".");
            String lsource = lhs.substring(0, li);
            Source ls = entryDefinition.getSource(lsource);
            if (ls == null) return lsource;

            String rhs = relationship.getRhs();
            int ri = rhs.indexOf(".");
            String rsource = rhs.substring(0, ri);
            Source rs = entryDefinition.getSource(lsource);
            if (rs == null) return rsource;

        }

        Source source = (Source)entryDefinition.getSources().iterator().next();
        return source.getName();
    }

    public Collection getPrimaryKeys(
            Entry parent,
            EntryDefinition entryDefinition,
            Filter filter,
            Calendar calendar
            ) throws Exception {

        Graph graph = config.getGraph(entryDefinition);
        Source primarySource = config.getPrimarySource(entryDefinition);

        log.debug("--------------------------------------------------------------------------------------");

        if (parent != null && parent.isDynamic()) {

            AttributeValues values = parent.getAttributeValues();
            Collection rows = engineContext.getTransformEngine().convert(values);

            Collection newRows = new HashSet();
            for (Iterator i=rows.iterator(); i.hasNext(); ) {
                Row row = (Row)i.next();

                Interpreter interpreter = engineContext.newInterpreter();
                interpreter.set(row);

                Row newRow = new Row();

                for (Iterator j=parent.getSources().iterator(); j.hasNext(); ) {
                    Source s = (Source)j.next();

                    for (Iterator k=s.getFields().iterator(); k.hasNext(); ) {
                        Field f = (Field)k.next();
                        String expression = f.getExpression();
                        Object v = interpreter.eval(expression);

                        //log.debug("Setting parent's value "+s.getName()+"."+f.getName()+": "+v);
                        newRow.set(f.getName(), v);
                    }
                }

                newRows.add(newRow);
            }

            String startingSourceName = getStartingSourceName(entryDefinition);
            Source startingSource = entryDefinition.getEffectiveSource(startingSourceName);

            PrimaryKeyGraphVisitor visitor = new PrimaryKeyGraphVisitor(engine, entryDefinition, newRows, primarySource);
            graph.traverse(visitor, startingSource);
            return visitor.getKeys();

        } else {

            String primarySourceName = primarySource.getName();
            log.debug("Primary source: "+primarySourceName);

            Filter f = cache.getCacheFilterTool().toSourceFilter(null, entryDefinition, primarySource, filter);

            log.debug("Searching source "+primarySourceName+" for "+f);
            SearchResults results = primarySource.search(f);

            Set keys = new HashSet();

            for (Iterator j=results.iterator(); j.hasNext(); ) {
                Row row = (Row)j.next();

                Interpreter interpreter = engineContext.newInterpreter();
                for (Iterator k=row.getNames().iterator(); k.hasNext(); ) {
                    String name = (String)k.next();
                    Object value = row.get(name);
                    interpreter.set(primarySourceName+"."+name, value);
                }

                Collection rdnAttributes = entryDefinition.getRdnAttributes();

                Row pk = new Row();
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

                    pk.set(name, value);
                }

                if (!valid) continue;
                keys.add(pk);
            }

            return keys;
        }
/*
        List list = new ArrayList();
        List nullList = new ArrayList();
        Map filters = new HashMap();

        for (Iterator i = entryDefinition.getSources().iterator(); i.hasNext();) {
            Source source = (Source) i.next();

            String sourceName = source.getName();

            Filter sqlFilter = engine.getSourceCache().getCacheFilterTool().toSourceFilter(initialRow, entryDefinition, source, filter);
            filters.put(sourceName, sqlFilter);

            log.debug("Filter for " + sourceName+": "+sqlFilter);

            if (sqlFilter == null) { // filter is not applicable to this source
                nullList.add(source);
            } else { // filter is applicable to this source
                list.add(source);
            }
        }

        log.debug("--------------------------------------------------------------------------------------");

        Collection rows = traverseGraph(entryDefinition, initialRow, sourceGraph, filter, primarySource);

        Set keys = new HashSet();

        if (rows != null) {
            log.debug("Found: ");
            for (Iterator j=rows.iterator(); j.hasNext(); ) {
                Row row = (Row)j.next();

                Interpreter interpreter = engineContext.newInterpreter();
                for (Iterator k=row.getNames().iterator(); k.hasNext(); ) {
                    String name = (String)k.next();
                    Object value = row.get(name);
                    interpreter.set(primarySourceName+"."+name, value);
                }

                Collection rdnAttributes = entryDefinition.getRdnAttributes();

                Row pk = new Row();
                for (Iterator k=rdnAttributes.iterator(); k.hasNext(); ) {
                    AttributeDefinition attr = (AttributeDefinition)k.next();
                    String name = attr.getName();
                    String expression = attr.getExpression();
                    Object value = interpreter.eval(expression);
                    pk.set(name, value);
                }

                log.debug(" - "+row+" => "+pk);

                keys.add(pk);
            }

        } else {
            Filter primaryFilter;

            Collection relationships = entryDefinition.getRelationships();
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

                Set pks = new HashSet();
                for (Iterator j=results.iterator(); j.hasNext(); ) {
                    Row values = (Row)j.next();
                    Object value = values.get(keyName);

                    Row pk = new Row();
                    pk.set(nextKeyName, value);

                    pks.add(pk);
                }

                primaryFilter = engine.getEngineContext().getFilterTool().createFilter(pks);

            } else {
                primaryFilter = engine.getSourceCache().getCacheFilterTool().toSourceFilter(initialRow, entryDefinition, primarySource, filter);
            }

            log.debug("Searching source "+primarySourceName+" for "+primaryFilter);
            SearchResults results = primarySource.search(primaryFilter);

            for (Iterator j=results.iterator(); j.hasNext(); ) {
                Row row = (Row)j.next();

                Interpreter interpreter = engineContext.newInterpreter();
                for (Iterator k=row.getNames().iterator(); k.hasNext(); ) {
                    String name = (String)k.next();
                    Object value = row.get(name);
                    interpreter.set(primarySourceName+"."+name, value);
                }

                Collection rdnAttributes = entryDefinition.getRdnAttributes();

                Row pk = new Row();
                for (Iterator k=rdnAttributes.iterator(); k.hasNext(); ) {
                    AttributeDefinition attr = (AttributeDefinition)k.next();
                    String name = attr.getName();
                    String expression = attr.getExpression();
                    Object value = interpreter.eval(expression);
                    pk.set(name, value);
                }

                keys.add(pk);
            }
        }

        return keys;
*/
    }

    /**
     * Convert rdns into primary keys
     */
    public Collection rdnToPk(EntryDefinition entryDefinition, Collection rdns) throws Exception {

        Source primarySource = config.getPrimarySource(entryDefinition);

        Collection pks = new HashSet();

        for (Iterator i=rdns.iterator(); i.hasNext(); ) {
            Row rdn = (Row)i.next();

            Interpreter interpreter = engineContext.newInterpreter();
            interpreter.set(rdn);

            Collection fields = primarySource.getPrimaryKeyFields();
            Row pk = new Row();

            for (Iterator j=fields.iterator(); j.hasNext(); ) {
                Field field = (Field)j.next();
                String name = field.getName();
                String expression = field.getExpression();

                Object value = interpreter.eval(expression);
                if (value == null) continue;

                pk.set(name, value);
            }

            pks.add(pk);
        }

        return pks;
    }

    /**
     * Load sources of entries matching the filter.
     *
     * @param entryDefinition
     * @param pks
     * @param calendar
     * @throws Exception
     */
    public void loadSources(
            EntryDefinition entryDefinition,
            Collection pks,
            Calendar calendar
            ) throws Exception {

        Graph graph = config.getGraph(entryDefinition);
        Source primarySource = config.getPrimarySource(entryDefinition);

        log.debug("--------------------------------------------------------------------------------------");
        log.debug("Loading all sources with pks " + pks);

        SourceLoaderGraphVisitor visitor = new SourceLoaderGraphVisitor(engine, entryDefinition, pks, calendar.getTime());
        graph.traverse(visitor, primarySource);
    }

    public void joinSources(
            EntryDefinition entryDefinition,
            Collection pks,
            Calendar calendar
            ) throws Exception {

        Graph graph = config.getGraph(entryDefinition);
        Source primarySource = config.getPrimarySource(entryDefinition);

        Filter filter = engine.getEngineContext().getFilterTool().createFilter(pks);

        // TODO need to add primarySource's name to the filter
        String sqlFilter = cache.getCacheFilterTool().toSQLFilter(entryDefinition, filter);

        log.debug("--------------------------------------------------------------------------------------");
        log.debug("Joining sources with pks "+pks);

        MRSWLock lock = engine.getLock(entryDefinition.getDn());
        lock.getWriteLock(Penrose.WAIT_TIMEOUT);

        try {

            // join rows from sources
            Collection rows = engine.getSourceCache().joinSources(entryDefinition, graph, primarySource, sqlFilter);
            log.debug("Joined " + rows.size() + " rows.");

            // merge rows into attribute values
            Map entries = new HashMap();
            for (Iterator i = rows.iterator(); i.hasNext();) {
                Row row = (Row)i.next();
                Map pk = new HashMap();
                Row translatedRow = new Row();

                boolean validPK = engineContext.getTransformEngine().translate(entryDefinition, row, pk, translatedRow);
                if (!validPK) continue;

                AttributeValues values = (AttributeValues)entries.get(pk);
                if (values == null) {
                    values = new AttributeValues();
                    entries.put(pk, values);
                }
                values.add(translatedRow);
            }
            log.debug("Merged " + entries.size() + " entries.");

            // update attribute values in entry cache
            for (Iterator i=entries.values().iterator(); i.hasNext(); ) {
                AttributeValues values = (AttributeValues)i.next();
                engine.getEntryCache().remove(entryDefinition, values, calendar.getTime());
                engine.getEntryCache().put(entryDefinition, values, calendar.getTime());
            }

        } finally {
            lock.releaseWriteLock(Penrose.WAIT_TIMEOUT);
        }
    }

}