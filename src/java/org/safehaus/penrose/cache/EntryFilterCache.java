/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.cache;

import org.safehaus.penrose.mapping.EntryDefinition;
import org.safehaus.penrose.filter.Filter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Collection;
import java.util.TreeMap;

/**
 * @author Endi S. Dewata
 */
public class EntryFilterCache {

    Logger log = LoggerFactory.getLogger(getClass());

    public Map maps = new TreeMap();

    public Cache cache;

    private int size;

    public void init(Cache cache) throws Exception {
        this.cache = cache;

        String s = cache.getParameter("size");
        size = s == null ? 50 : Integer.parseInt(s);

        init();
    }

    public void init() throws Exception {
    }

    public Map getMap(Object key) {
        Map map = (Map)maps.get(key);
        if (map == null) {
            map = new TreeMap();
            maps.put(key, map);
        }
        return map;
    }

    public Collection get(Object key, Filter filter) throws Exception {
        Map map = getMap(key);

        log.debug("Getting entry filter cache ("+map.size()+"): "+filter);

        Collection rdns = (Collection)map.remove(filter == null ? "" : filter.toString());
        if (rdns != null) map.put(filter == null ? "" : filter.toString(), rdns);

        return rdns;
    }

    public void put(Object key, Filter filter, Collection rdns) throws Exception {

        Map map = getMap(key);

        if (map.size() >= size) {
            log.debug("Trimming entry filter cache ("+map.size()+").");
            Object o = map.keySet().iterator().next();
            map.remove(o);
        }

        log.debug("Storing entry filter cache ("+map.size()+"): "+filter);
        map.put(filter == null ? "" : filter.toString(), rdns);
    }

    public void invalidate(Object key) throws Exception {
        Map map = getMap(key);
        map.clear();
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }
}
