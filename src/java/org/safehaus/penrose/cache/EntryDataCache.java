/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.cache;

import org.safehaus.penrose.mapping.Entry;
import org.safehaus.penrose.mapping.EntryDefinition;
import org.safehaus.penrose.mapping.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class EntryDataCache {

    Logger log = LoggerFactory.getLogger(getClass());

    private Cache cache;
    private CacheContext cacheContext;

    private Map data = new TreeMap();
    private Map expirations = new TreeMap();

    private int size;
    private int expiration; // minutes

    public void init(Cache cache) throws Exception {
        this.cache = cache;
        this.cacheContext = cache.getCacheContext();

        String s = cache.getParameter("size");
        size = s == null ? 50 : Integer.parseInt(s);

        s = cache.getParameter("expiration");
        expiration = s == null ? 5 : Integer.parseInt(s);

        init();
    }

    public void init() throws Exception {
    }

    public Map getDataMap(String dn) {
        Map map = (Map)data.get(dn);
        if (map == null) {
            map = new TreeMap();
            data.put(dn, map);
        }
        return map;
    }

    public Map getExpirationMap(String dn) {
        Map map = (Map)expirations.get(dn);
        if (map == null) {
            map = new LinkedHashMap();
            expirations.put(dn, map);
        }
        return map;
    }

    public Entry get(String dn, Row rdn) throws Exception {

        Map dataMap = getDataMap(dn);
        Map expirationMap = getExpirationMap(dn);

        Row key = cacheContext.getSchema().normalize(rdn);

        log.debug("Getting entry cache ("+dataMap.size()+"): "+key);

        Entry entry = (Entry)dataMap.get(key);
        Date date = (Date)expirationMap.get(key);

        if (date == null || date.getTime() <= System.currentTimeMillis()) {
            dataMap.remove(key);
            expirationMap.remove(key);
            return null;
        }

        return entry;
    }

    public void put(String dn, Row rdn, Entry entry) throws Exception {

        Map dataMap = getDataMap(dn);
        Map expirationMap = getExpirationMap(dn);

        Row key = cacheContext.getSchema().normalize(rdn);

        Object object = dataMap.get(key);

        while (object == null && dataMap.size() >= size) {
            log.debug("Trimming entry cache ("+dataMap.size()+").");
            Object k = expirationMap.keySet().iterator().next();
            dataMap.remove(k);
            expirationMap.remove(k);
        }

        log.debug("Storing entry cache ("+dataMap.size()+"): "+key);
        dataMap.put(key, entry);
        expirationMap.put(key, new Date(System.currentTimeMillis() + expiration * 60 * 1000));
    }

    public void remove(String dn, Row rdn) throws Exception {

        Map dataMap = getDataMap(dn);
        Map expirationMap = getExpirationMap(dn);

        Row key = cacheContext.getSchema().normalize(rdn);

        log.debug("Removing entry cache ("+dataMap.size()+"): "+key);
        dataMap.remove(key);
        expirationMap.remove(key);
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

    public int getExpiration() {
        return expiration;
    }

    public void setExpiration(int expiration) {
        this.expiration = expiration;
    }
}
