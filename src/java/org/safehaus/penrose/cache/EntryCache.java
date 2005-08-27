/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.cache;

import org.safehaus.penrose.mapping.Entry;
import org.safehaus.penrose.mapping.Source;
import org.safehaus.penrose.mapping.EntryDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class EntryCache {

    Logger log = LoggerFactory.getLogger(getClass());

    private Cache cache;
    private CacheContext cacheContext;

    private int size;

    private Map entries = new LinkedHashMap();

    public void init(Cache cache) throws Exception {
        this.cache = cache;
        this.cacheContext = cache.getCacheContext();

        String s = cache.getParameter("size");
        size = s == null ? 50 : Integer.parseInt(s);

        init();
    }

    public void init() throws Exception {
    }

    public Map getMap(EntryDefinition entryDefinition) {
        Map map = (Map)entries.get(entryDefinition.getDn());
        if (map == null) {
            map = new TreeMap();
            entries.put(entryDefinition.getDn(), map);
        }
        return map;
    }

    public Entry get(String dn, EntryDefinition entryDefinition) throws Exception {

        Map map = getMap(entryDefinition);
        String ndn = cacheContext.getSchema().normalize(dn);

        log.debug("Getting entry cache ("+map.size()+"): "+ndn);

        Entry entry = (Entry)map.remove(ndn);
        map.put(ndn, entry);

        return entry;
    }

    public void put(Entry entry) throws Exception {

        Map map = getMap(entry.getEntryDefinition());
        String dn = entry.getDn();
        String ndn = cacheContext.getSchema().normalize(dn);

        while (map.size() >= size) {
            log.debug("Trimming entry cache ("+map.size()+").");
            Object key = map.keySet().iterator().next();
            map.remove(key);
        }

        log.debug("Storing entry cache ("+map.size()+"): "+ndn);
        map.put(ndn, entry);
    }

    public void remove(Entry entry) throws Exception {

        Map map = getMap(entry.getEntryDefinition());
        String dn = entry.getDn();
        String ndn = cacheContext.getSchema().normalize(dn);

        log.debug("Removing entry cache ("+map.size()+"): "+ndn);
        map.remove(ndn);
    }

    public void invalidate(EntryDefinition entryDefinition) throws Exception {
        Map map = getMap(entryDefinition);
        map.clear();
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

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }
}
