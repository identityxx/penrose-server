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

    private int size;

    private Map entries = new LinkedHashMap();

    public void init(Cache cache) throws Exception {
        this.cache = cache;
        this.cacheContext = cache.getCacheContext();
        this.config = cacheContext.getConfig();

        String s = cache.getParameter("size");
        size = s == null ? 50 : Integer.parseInt(s);

        init();
    }

    public void init() throws Exception {
    }

    public Entry get(String dn) throws Exception {

        log.debug("Getting entry cache ("+entries.size()+"): "+dn);

        Entry entry = (Entry)entries.remove(dn);
        entries.put(dn, entry);

        return entry;
    }

    public void put(Entry entry) throws Exception {

        String dn = entry.getDn();

        while (entries.size() >= size) {
            log.debug("Trimming entry cache ("+entries.size()+").");
            Row key = (Row)entries.keySet().iterator().next();
            entries.remove(key);
        }

        log.debug("Storing entry cache ("+entries.size()+"): "+dn);
        entries.put(dn, entry);
    }

    public void remove(Entry entry) throws Exception {

        String dn = entry.getDn();

        log.debug("Removing entry cache ("+entries.size()+"): "+dn);
        entries.remove(dn);
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

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }
}
