/**
 * Copyright (c) 2000-2005, Identyx Corporation.
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

    private Map dataMap = new TreeMap();
    private Map expirationMap = new TreeMap();

    private int size;
    private int expiration; // minutes

    public void init(Cache cache) throws Exception {
        this.cache = cache;
        this.cacheContext = cache.getCacheContext();

        String s = cache.getParameter(Cache.SIZE);
        size = s == null ? 100 : Integer.parseInt(s);

        s = cache.getParameter(Cache.EXPIRATION);
        expiration = s == null ? 5 : Integer.parseInt(s);

        init();
    }

    public void init() throws Exception {
    }

    public Map get(Collection filters) throws Exception {

        Map results = new TreeMap();

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

    public void put(Row pk, AttributeValues values) throws Exception {

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

    public void remove(Row pk) throws Exception {

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
