/**
 * Copyright (c) 2000-2005, Identyx Corporation.
 * All rights reserved.
 */
package org.safehaus.penrose.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.mapping.SourceDefinition;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class SourceFilterCache {

    Logger log = LoggerFactory.getLogger(getClass());

    public Map dataMap = new TreeMap();
    public Map expirationMap = new LinkedHashMap();

    public SourceDefinition sourceDefinition;
    public Cache cache;
    public CacheContext cacheContext;

    private int size;
    private int expiration; // minutes

    public SourceFilterCache(Cache cache, SourceDefinition sourceDefinition) {
        this.cache = cache;
        this.cacheContext = cache.getCacheContext();
        this.sourceDefinition = sourceDefinition;

        String s = sourceDefinition.getParameter(SourceDefinition.FILTER_CACHE_SIZE);
        size = s == null ? SourceDefinition.DEFAULT_FILTER_CACHE_SIZE : Integer.parseInt(s);

        s = sourceDefinition.getParameter(SourceDefinition.FILTER_CACHE_EXPIRATION);
        expiration = s == null ? SourceDefinition.DEFAULT_FILTER_CACHE_EXPIRATION : Integer.parseInt(s);
    }

    public Collection get(Filter filter) throws Exception {

        String key = filter == null ? "" : filter.toString();
        log.debug("Getting source filter cache ("+dataMap.size()+"): "+key);

        Collection pks = (Collection)dataMap.get(key);
        Date date = (Date)expirationMap.get(key);

        if (date == null || date.getTime() <= System.currentTimeMillis()) {
            dataMap.remove(key);
            expirationMap.remove(key);
            return null;
        }

        return pks;
    }

    public void put(Filter filter, Collection pks) throws Exception {

        String key = filter == null ? "" : filter.toString();

        Object object = dataMap.get(key);

        while (object == null && dataMap.size() >= size) {
            log.debug("Trimming source filter cache ("+dataMap.size()+").");
            Object k = dataMap.keySet().iterator().next();
            dataMap.remove(k);
            expirationMap.remove(k);
        }

        log.debug("Storing source filter cache ("+dataMap.size()+"): "+key);
        dataMap.put(key, pks);
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
}
