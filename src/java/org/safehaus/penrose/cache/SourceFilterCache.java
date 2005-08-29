/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.filter.Filter;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class SourceFilterCache {

    Logger log = LoggerFactory.getLogger(getClass());

    public Map data = new TreeMap();
    public Map expirations = new TreeMap();

    public Cache cache;

    private int size;
    private int expiration; // minutes

    public void init(Cache cache) throws Exception {
        this.cache = cache;

        String s = cache.getParameter("size");
        size = s == null ? 100 : Integer.parseInt(s);

        s = cache.getParameter("expiration");
        expiration = s == null ? 5 : Integer.parseInt(s);

        init();
    }

    public void init() throws Exception {
    }

    public Map getDataMap(String sourceName) {
        Map map = (Map)data.get(sourceName);
        if (map == null) {
            map = new TreeMap();
            data.put(sourceName, map);
        }
        return map;
    }

    public Map getExpirationMap(String sourceName) {
        Map map = (Map)expirations.get(sourceName);
        if (map == null) {
            map = new LinkedHashMap();
            expirations.put(sourceName, map);
        }
        return map;
    }

    public Collection get(String sourceName, Filter filter) throws Exception {

        Map dataMap = getDataMap(sourceName);
        Map expirationMap = getExpirationMap(sourceName);

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

    public void put(String sourceName, Filter filter, Collection pks) throws Exception {

        Map dataMap = getDataMap(sourceName);
        Map expirationMap = getExpirationMap(sourceName);

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

    public void remove(String sourceName) throws Exception {
        Map dataMap = getDataMap(sourceName);
        Map expirationMap = getExpirationMap(sourceName);

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
