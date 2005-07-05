/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.cache;

import org.safehaus.penrose.mapping.EntryDefinition;
import org.safehaus.penrose.mapping.Row;
import org.safehaus.penrose.mapping.Entry;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.config.Config;
import org.apache.log4j.Logger;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class EntryCache {

    public Logger log = Logger.getLogger(Penrose.CACHE_LOGGER);

    private Cache cache;
    private CacheContext cacheContext;
    private Config config;

    private Map entries = new HashMap();

    public void init(Cache cache) throws Exception {
        this.cache = cache;
        this.cacheContext = cache.getCacheContext();
        this.config = cacheContext.getConfig();

        init();
    }

    public void init() throws Exception {
    }

    public Map getMap(String dn) {
        Map map = (Map)entries.get(dn);
        if (map == null) {
            map = new TreeMap();
            entries.put(dn, map);
        }
        return map;
    }

    public Collection getRdns(
            EntryDefinition entryDefinition,
            Collection rdns,
            Date date)
            throws Exception {

        log.debug("Getting rdns: "+rdns);

        Map map = getMap(entryDefinition.getDn());

        Collection allRdns = map.keySet();

        Collection loadedRdns = new HashSet();
        loadedRdns.addAll(allRdns);
        loadedRdns.retainAll(rdns);

        return loadedRdns;
    }

    public Entry get(EntryDefinition entryDefinition, Row rdn) throws Exception {

        log.debug("Getting entry: "+rdn);

        Map map = getMap(entryDefinition.getDn());

        return (Entry)map.get(rdn);
    }

    public Map get(EntryDefinition entryDefinition, Collection rdns) throws Exception {
        Map results = new HashMap();
        if (rdns == null) return results;

        for (Iterator i=rdns.iterator(); i.hasNext(); ) {
            Row rdn = (Row)i.next();

            Entry entry = get(entryDefinition, rdn);
            if (entry == null) continue;

            results.put(rdn, entry);
        }

        return results;
    }

    public void put(Entry entry, Date date) throws Exception {

        Row rdn = entry.getRdn();
        log.debug("Storing entry: "+rdn);

        EntryDefinition entryDefinition = entry.getEntryDefinition();

        Map map = getMap(entryDefinition.getDn());
        map.put(rdn, entry);
    }

    public void remove(Entry entry) throws Exception {

        Row rdn = entry.getRdn();
        log.debug("Removing entry: "+rdn);

        EntryDefinition entryDefinition = entry.getEntryDefinition();

        Map map = getMap(entryDefinition.getDn());
        map.remove(rdn);
    }

    public Cache getCache() {
        return cache;
    }

    public void setCache(Cache cache) {
        this.cache = cache;
    }

    public CacheContext getCacheContext() {
        return cacheContext;
    }

    public void setCacheContext(CacheContext cacheContext) {
        this.cacheContext = cacheContext;
    }

    public Config getConfig() {
        return config;
    }

    public void setConfig(Config config) {
        this.config = config;
    }

    public Map getEntries() {
        return entries;
    }

    public void setEntries(Map entries) {
        this.entries = entries;
    }
}
