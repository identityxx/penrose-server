/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.cache;

import org.safehaus.penrose.filter.Filter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class EntryFilterCache {

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

    public Collection get(String dn, Filter filter) throws Exception {

        Map dataMap = getDataMap(dn);
        Map expirationMap = getExpirationMap(dn);

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

    public void put(String dn, Filter filter, Collection rdns) throws Exception {

        Map dataMap = getDataMap(dn);
        Map expirationMap = getExpirationMap(dn);

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

    public void remove(String dn) throws Exception {
        Map dataMap = getDataMap(dn);
        Map expirationMap = getExpirationMap(dn);

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
