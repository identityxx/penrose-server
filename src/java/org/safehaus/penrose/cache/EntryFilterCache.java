/**
 * Copyright (c) 2000-2005, Identyx Corporation.
 * All rights reserved.
 */
package org.safehaus.penrose.cache;

import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.mapping.EntryDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class EntryFilterCache {

    Logger log = LoggerFactory.getLogger(getClass());

    public Map dataMap = new TreeMap();
    public Map expirationMap = new LinkedHashMap();

    public EntryDefinition entryDefinition;
    public Cache cache;
    public CacheContext cacheContext;

    private int size;
    private int expiration; // minutes

    public EntryFilterCache(Cache cache, EntryDefinition entryDefinition) {
        this.cache = cache;
        this.cacheContext = cache.getCacheContext();
        this.entryDefinition = entryDefinition;

        String s = entryDefinition.getParameter(EntryDefinition.FILTER_CACHE_SIZE);
        size = s == null ? EntryDefinition.DEFAULT_FILTER_CACHE_SIZE : Integer.parseInt(s);

        s = entryDefinition.getParameter(EntryDefinition.FILTER_CACHE_EXPIRATION);
        expiration = s == null ? EntryDefinition.DEFAULT_FILTER_CACHE_EXPIRATION : Integer.parseInt(s);
    }

    public void init() throws Exception {
    }

    public Collection get(Filter filter) throws Exception {

        String key = filter == null ? "" : filter.toString();
        log.debug("Getting entry filter cache ("+dataMap.size()+"): "+key);

        Collection rdns = (Collection)dataMap.get(key);
        Date date = (Date)expirationMap.get(key);

        if (date == null || date.getTime() <= System.currentTimeMillis()) {
            dataMap.remove(key);
            expirationMap.remove(key);
            return null;
        }

        return rdns;
    }

    public void put(Filter filter, Collection rdns) throws Exception {

        String key = filter == null ? "" : filter.toString();

        Object object = (Collection)dataMap.remove(key);

        while (object == null && dataMap.size() >= size) {
            log.debug("Trimming entry filter cache ("+dataMap.size()+").");
            Object k = expirationMap.keySet().iterator().next();
            dataMap.remove(k);
            expirationMap.remove(k);
        }

        log.debug("Storing entry filter cache ("+dataMap.size()+"): "+key);
        dataMap.put(key, rdns);
        expirationMap.put(key, new Date(System.currentTimeMillis() + expiration * 60 * 1000));
    }

    public void invalidate() throws Exception {
        dataMap.clear();
        expirationMap.clear();
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public int getExpiration() {
        return expiration;
    }

    public void setExpiration(int expiration) {
        this.expiration = expiration;
    }
}
