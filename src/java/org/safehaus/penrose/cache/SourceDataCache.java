/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.cache;

import org.safehaus.penrose.mapping.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class SourceDataCache {

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

    public void refresh() throws Exception {
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

    public Map get(
            String sourceName,
            Collection filters)
            throws Exception {

        Map results = new TreeMap();

        Map dataMap = getDataMap(sourceName);
        Map expirationMap = getExpirationMap(sourceName);

        for (Iterator i=dataMap.keySet().iterator(); i.hasNext(); ) {
            Row pk = (Row)i.next();

            AttributeValues attributeValues = (AttributeValues)dataMap.get(pk);
            Date date = (Date)expirationMap.get(pk);

            if (date == null || date.getTime() <= System.currentTimeMillis()) {
                continue;
            }

            for (Iterator j=filters.iterator(); j.hasNext(); ) {
                Row filter = (Row)j.next();

                boolean found = cacheContext.getSchema().partialMatch(attributeValues, filter);

                if (found) {
                    results.put(pk, attributeValues);
                }
            }

        }

        return results;
    }

    public void put(String sourceName, Row pk, AttributeValues values) throws Exception {

        Map dataMap = getDataMap(sourceName);
        Map expirationMap = getExpirationMap(sourceName);

        Row key = cacheContext.getSchema().normalize(pk);

        Object object = dataMap.get(key);

        while (object == null && dataMap.size() >= size) {
            log.debug("Trimming source cache ("+dataMap.size()+").");
            Object k = expirationMap.keySet().iterator().next();
            dataMap.remove(k);
            expirationMap.remove(k);
        }

        log.debug("Storing source cache ("+dataMap.size()+"): "+key);
        dataMap.put(key, values);
        expirationMap.put(key, new Date(System.currentTimeMillis() + expiration * 60 * 1000));
    }

    public void remove(String sourceName, Row pk) throws Exception {

        Map dataMap = getDataMap(sourceName);
        Map expirationMap = getExpirationMap(sourceName);

        Row key = cacheContext.getSchema().normalize(pk);

        log.debug("Removing source cache ("+dataMap.size()+"): "+key);
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
