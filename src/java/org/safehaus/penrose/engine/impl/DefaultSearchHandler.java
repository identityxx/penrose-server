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
import org.safehaus.penrose.cache.CacheConfig;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.thread.MRSWLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Endi S. Dewata
 */
public class DefaultSearchHandler extends SearchHandler {

    Logger log = LoggerFactory.getLogger(getClass());

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

        Source primarySource = getEngineContext().getPrimarySource(entryDefinition);
        String primarySourceName = primarySource.getName();

        log.debug("--------------------------------------------------------------------------------------");

        Collection keys = getEngineContext().getSyncService().search(parent, entryDefinition, filter);

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

        getCache().getFilterCache().put(key, rdns);

        return rdns;
    }

    /**
     * Convert rdns into primary keys
     */
    public Collection rdnToPk(EntryDefinition entryDefinition, Collection rdns) throws Exception {

        Source source = getEngineContext().getPrimarySource(entryDefinition);

        Collection pks = new TreeSet();

        for (Iterator i=rdns.iterator(); i.hasNext(); ) {
            Row rdn = (Row)i.next();

            Interpreter interpreter = getEngineContext().newInterpreter();
            interpreter.set(rdn);

            Collection fields = source.getPrimaryKeyFields();
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

        MRSWLock lock = getEngine().getLock(entryDefinition.getDn());
        lock.getWriteLock(Penrose.WAIT_TIMEOUT);

        SearchResults results = new SearchResults();

        try {
            String s = getCache().getParameter(CacheConfig.CACHE_EXPIRATION);
            int cacheExpiration = s == null ? 0 : Integer.parseInt(s);
            log.debug("Expiration: "+cacheExpiration);
            if (cacheExpiration < 0) cacheExpiration = Integer.MAX_VALUE;

            Calendar c = (Calendar) calendar.clone();
            c.add(Calendar.MINUTE, -cacheExpiration);

            Collection rdnsToLoad = new TreeSet();

            for (Iterator i=rdns.iterator(); i.hasNext(); ) {
                Row rdn = (Row)i.next();

                String dn = rdn.toString()+","+parent.getDn();

                Entry entry = getEngine().getCache().getEntryCache().get(dn);
                if (entry == null) {
                    rdnsToLoad.add(rdn);
                } else {
                    entry.setParent(parent);
                    entry.setEntryDefinition(entryDefinition);
                    results.add(entry);
                }
            }

            log.debug("Rdns to load: "+rdnsToLoad);

            if (!rdnsToLoad.isEmpty()) {

                Collection pks = rdnToPk(entryDefinition, rdnsToLoad);
                Collection rows = getEngineContext().getSyncService().load(parent, entryDefinition, pks);
                Collection entries = getEngine().merge(parent, entryDefinition, rows);

                for (Iterator i=entries.iterator(); i.hasNext(); ) {
                    Entry entry = (Entry)i.next();
                    entry.setParent(parent);
                    results.add(entry);
                    getEngine().getCache().getEntryCache().put(entry);
                }
            }

        } finally {
            lock.releaseWriteLock(Penrose.WAIT_TIMEOUT);
            results.close();
        }

        return results;
    }
}