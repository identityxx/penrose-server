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

        // find entry using rdn as primary key
        loadObject(entryDefinition, pk, calendar);

        SearchResults results = getEntries(parent, entryDefinition, rdns);

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

        Collection rdns = getPrimaryKeys(parent, entryDefinition, filter, calendar);
        log.debug("Searched rdns: "+rdns);

        loadObjects(entryDefinition, rdns, calendar);

        return getEntries(parent, entryDefinition, rdns);
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

        //log.debug("--------------------------------------------------------------------------------------");
        //log.debug("Getting entries from cache with pks "+keys);

        MRSWLock lock = ((DefaultEngine)getEngine()).getLock(entryDefinition.getDn());
        lock.getReadLock(Penrose.WAIT_TIMEOUT);

        SearchResults results = new SearchResults();

        try {
            Map entries = getEngine().getEntryCache().get(entryDefinition, keys);

            for (Iterator i = entries.values().iterator(); i.hasNext();) {
                Entry sr = (Entry) i.next();
                sr.setParent(parent);
                //log.debug("Returning "+sr.getDn());
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

        Graph graph = getConfig().getGraph(entryDefinition);
        Source primarySource = getConfig().getPrimarySource(entryDefinition);

        log.debug("--------------------------------------------------------------------------------------");

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
            return visitor.getKeys();

        } else {

            String primarySourceName = primarySource.getName();
            log.debug("Primary source: "+primarySourceName);

            Filter f = ((DefaultCache)getCache()).getCacheFilterTool().toSourceFilter(null, entryDefinition, primarySource, filter);

            log.debug("Searching source "+primarySourceName+" for "+f);
            SearchResults results = primarySource.search(f, 100);

            Set keys = new HashSet();

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

        Source primarySource = getConfig().getPrimarySource(entryDefinition);

        Collection pks = new HashSet();

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

    public void loadObject(
            EntryDefinition entryDefinition,
            Row rdn,
            Calendar calendar)
            throws Exception {

        List rdns = new ArrayList();
        rdns.add(rdn);

        loadObjects(entryDefinition, rdns, calendar);
    }

    /**
     * Load sources of entries matching the filter.
     *
     * @param entryDefinition
     * @param rdns
     * @param calendar
     * @throws Exception
     */
    public void loadObjects(
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

        Collection rdnsToLoad = new HashSet();
        rdnsToLoad.addAll(rdns);
        rdnsToLoad.removeAll(loadedRdns);
        log.debug("Rdns to load: "+rdnsToLoad);

        if (rdnsToLoad.isEmpty()) return;

        Collection pks = rdnToPk(entryDefinition, rdnsToLoad);

        Graph graph = getConfig().getGraph(entryDefinition);
        Source primarySource = getConfig().getPrimarySource(entryDefinition);

        SourceLoaderGraphVisitor visitor = new SourceLoaderGraphVisitor(getEngine(), entryDefinition, pks, calendar.getTime());
        graph.traverse(visitor, primarySource);

        joinSources(entryDefinition, pks, calendar);
    }

    public void joinSources(
            EntryDefinition entryDefinition,
            Collection pks,
            Calendar calendar
            ) throws Exception {

        log.debug("--------------------------------------------------------------------------------------");
        log.debug("Joining sources with pks "+pks);

        Graph graph = getConfig().getGraph(entryDefinition);
        Source primarySource = getConfig().getPrimarySource(entryDefinition);

        Filter filter = getEngine().getEngineContext().getFilterTool().createFilter(pks);
        String sqlFilter = ((DefaultCache)getCache()).getCacheFilterTool().toSQLFilter(entryDefinition, filter);

        MRSWLock lock = ((DefaultEngine)getEngine()).getLock(entryDefinition.getDn());
        lock.getWriteLock(Penrose.WAIT_TIMEOUT);

        try {

            // join rows from sources
            Collection rows = getEngine().getSourceCache().joinSources(entryDefinition, graph, primarySource, sqlFilter);
            log.debug("Joined " + rows.size() + " rows.");

            // merge rows into attribute values
            Map entries = new HashMap();
            for (Iterator i = rows.iterator(); i.hasNext();) {
                Row row = (Row)i.next();
                Map pk = new HashMap();
                Row translatedRow = new Row();

                boolean validPK = getEngineContext().getTransformEngine().translate(entryDefinition, row, pk, translatedRow);
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
                getEngine().getEntryCache().remove(entryDefinition, values);
                getEngine().getEntryCache().put(entryDefinition, values, calendar.getTime());
            }

        } finally {
            lock.releaseWriteLock(Penrose.WAIT_TIMEOUT);
        }
    }
}