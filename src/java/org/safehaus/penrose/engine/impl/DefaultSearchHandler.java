/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.engine.impl;


import java.util.*;

import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.SearchResults;
import org.safehaus.penrose.PenroseConnection;
import org.safehaus.penrose.engine.SearchHandler;
import org.safehaus.penrose.graph.Graph;
import org.safehaus.penrose.cache.CacheConfig;
import org.safehaus.penrose.cache.impl.DefaultCache;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.thread.MRSWLock;

/**
 * @author Endi S. Dewata
 */
public class DefaultSearchHandler extends SearchHandler {

    /**
	 * Find a virtual entry given an rdn and a mapping entry.
	 * 
	 * @param rdn
	 * @param entryDefinition
	 * @return the entry
	 * @throws Exception
	 */
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

        SearchResults results = load(parent, entryDefinition, rdns, calendar);

        if (results.size() == 0) return null;

        // there should be only one entry
        Entry entry = (Entry)results.iterator().next();

		return entry;
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

        return load(parent, entryDefinition, rdns, calendar);
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

    public Collection search(
            Entry parent,
            EntryDefinition entryDefinition,
            Filter filter,
            Calendar calendar
            ) throws Exception {

        String str = getCache().getParameter(CacheConfig.CACHE_EXPIRATION);
        int cacheExpiration = str == null ? 0 : Integer.parseInt(str);
        log.debug("Filter Cache Expiration: "+cacheExpiration);
        if (cacheExpiration < 0) cacheExpiration = Integer.MAX_VALUE;

        Calendar c = (Calendar) calendar.clone();
        c.add(Calendar.MINUTE, -cacheExpiration);

        String key = entryDefinition.getDn()+","+parent.getDn() + ":" + filter;
        Collection rdns = getCache().getFilterCache().get(key);
        if (rdns != null) {
            log.debug("Filter Cache found: "+rdns);
            return rdns;
        }

        log.debug("Filter Cache not found.");

        Graph graph = getConfig().getGraph(entryDefinition);
        Source primarySource = getConfig().getPrimarySource(entryDefinition);

        log.debug("--------------------------------------------------------------------------------------");

        rdns = new TreeSet();

        if (parent != null && parent.isDynamic()) {

            AttributeValues values = parent.getAttributeValues();
            Collection rows = getEngineContext().getTransformEngine().convert(values);

            Collection newRows = new HashSet();
            for (Iterator i=rows.iterator(); i.hasNext(); ) {
                Row row = (Row)i.next();

                Interpreter interpreter = getEngineContext().newInterpreter();
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

            PrimaryKeyGraphVisitor visitor = new PrimaryKeyGraphVisitor(getEngine(), entryDefinition, newRows, primarySource);
            graph.traverse(visitor, startingSource);
            rdns.addAll(visitor.getKeys());

        } else {

            String primarySourceName = primarySource.getName();
            log.debug("Primary source: "+primarySourceName);

            Filter f = ((DefaultCache)getCache()).getCacheFilterTool().toSourceFilter(null, entryDefinition, primarySource, filter);

            log.debug("Searching source "+primarySourceName+" for "+f);
            SearchResults results = primarySource.search(f, 100);

            for (Iterator j=results.iterator(); j.hasNext(); ) {
                Row row = (Row)j.next();

                Interpreter interpreter = getEngineContext().newInterpreter();
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
                rdns.add(pk);
            }

        }

        getCache().getFilterCache().put(key, rdns);

        return rdns;
    }

    /**
     * Convert rdns into primary keys
     */
    public Collection rdnToPk(EntryDefinition entryDefinition, Collection rdns) throws Exception {

        Source primarySource = getConfig().getPrimarySource(entryDefinition);

        Collection pks = new TreeSet();

        for (Iterator i=rdns.iterator(); i.hasNext(); ) {
            Row rdn = (Row)i.next();

            Interpreter interpreter = getEngineContext().newInterpreter();
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
     * @param rdns
     * @param calendar
     * @throws Exception
     */
    public SearchResults load(
            Entry parent,
            EntryDefinition entryDefinition,
            Collection rdns,
            Calendar calendar
            ) throws Exception {

        log.debug("--------------------------------------------------------------------------------------");
        log.debug("Loading entry "+entryDefinition.getDn()+" with rdns "+rdns);

        String s = getCache().getParameter(CacheConfig.CACHE_EXPIRATION);
        int cacheExpiration = s == null ? 0 : Integer.parseInt(s);
        log.debug("Expiration: "+cacheExpiration);
        if (cacheExpiration < 0) cacheExpiration = Integer.MAX_VALUE;

        Calendar c = (Calendar) calendar.clone();
        c.add(Calendar.MINUTE, -cacheExpiration);

        Collection loadedRdns = getEngine().getEntryCache().getRdns(entryDefinition, rdns, c.getTime());
        log.debug("Loaded rdns: "+loadedRdns);

        Collection rdnsToLoad = new TreeSet();
        rdnsToLoad.addAll(rdns);
        if (loadedRdns != null) rdnsToLoad.removeAll(loadedRdns);
        log.debug("Rdns to load: "+rdnsToLoad);

        if (rdnsToLoad.isEmpty()) return getEntries(parent, entryDefinition, rdns);

        Collection pks = rdnToPk(entryDefinition, rdnsToLoad);

        Graph graph = getConfig().getGraph(entryDefinition);
        Source primarySource = getConfig().getPrimarySource(entryDefinition);

        SourceLoaderGraphVisitor visitor = new SourceLoaderGraphVisitor(getEngine(), entryDefinition, pks, calendar.getTime());
        graph.traverse(visitor, primarySource);

        SearchResults results = new SearchResults();

        Collection entries = join(parent, entryDefinition, pks, calendar);
        log.debug("Loaded "+entries.size()+" new entries: "+rdnsToLoad);
        for (Iterator i=entries.iterator(); i.hasNext(); ) {
            Entry entry = (Entry)i.next();
            results.add(entry);
            getEngine().getEntryCache().put(entry, calendar.getTime());
        }

        SearchResults sr = getEntries(parent, entryDefinition, loadedRdns);
        log.debug("Returned "+sr.size()+" cached entries: "+loadedRdns);
        for (Iterator i=sr.iterator(); i.hasNext(); ) {
            Entry entry = (Entry)i.next();
            results.add(entry);
        }

        results.close();

        return results;
    }

    public Collection join(
            Entry parent,
            EntryDefinition entryDefinition,
            Collection pks,
            Calendar calendar
            ) throws Exception {

        log.debug("--------------------------------------------------------------------------------------");
        log.debug("Joining sources with pks "+pks);

        MRSWLock lock = ((DefaultEngine)getEngine()).getLock(entryDefinition.getDn());
        lock.getWriteLock(Penrose.WAIT_TIMEOUT);

        Collection results = new ArrayList();

        try {

            // join rows from sources
            Collection rows = getEngine().getSourceCache().join(entryDefinition, pks);
            log.debug("Joined " + rows.size() + " rows.");

            // merge rows into attribute values
            Map entries = new LinkedHashMap();
            for (Iterator i = rows.iterator(); i.hasNext();) {
                Row row = (Row)i.next();

                Map rdn = new HashMap();
                Row values = new Row();

                boolean validPK = getEngineContext().getTransformEngine().translate(entryDefinition, row, rdn, values);
                if (!validPK) continue;

                log.debug(" - "+values);

                AttributeValues attributeValues = (AttributeValues)entries.get(rdn);
                if (attributeValues == null) {
                    attributeValues = new AttributeValues();
                    entries.put(rdn, attributeValues);
                }
                attributeValues.add(values);
            }

            log.debug("Merged " + entries.size() + " entries.");

            for (Iterator i=entries.values().iterator(); i.hasNext(); ) {
                AttributeValues values = (AttributeValues)i.next();

                Entry entry = new Entry(entryDefinition, values);
                entry.setParent(parent);
                results.add(entry);
            }

        } finally {
            lock.releaseWriteLock(Penrose.WAIT_TIMEOUT);
        }

        return results;
    }

    /**
     * Get entries from entry cache.
     *
     * @param parent
     * @param entryDefinition
     * @param rdns
     * @return entries
     * @throws Exception
     */
    public SearchResults getEntries(
            Entry parent,
            EntryDefinition entryDefinition,
            Collection rdns) throws Exception {

        MRSWLock lock = ((DefaultEngine)getEngine()).getLock(entryDefinition.getDn());
        lock.getReadLock(Penrose.WAIT_TIMEOUT);

        SearchResults results = new SearchResults();

        try {
            Map entries = getEngine().getEntryCache().get(entryDefinition, rdns);

            for (Iterator i = entries.values().iterator(); i.hasNext();) {
                Entry entry = (Entry) i.next();
                entry.setParent(parent);
                results.add(entry);
            }

        } finally {
            lock.releaseReadLock(Penrose.WAIT_TIMEOUT);
            results.close();
        }

        return results;
    }
}