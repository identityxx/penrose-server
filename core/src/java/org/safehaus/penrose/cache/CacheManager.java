package org.safehaus.penrose.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.ldap.SearchResponse;

import java.util.LinkedHashMap;
import java.util.Date;

/**
 * @author Endi Sukma Dewata
 */
public class CacheManager {

    public Logger log = LoggerFactory.getLogger(getClass());

    private int querySize  = 10;
    private int resultSize = 100;
    private int expiration = 5; // minutes

    private LinkedHashMap<CacheKey,Cache> caches = new LinkedHashMap<CacheKey,Cache>();

    public CacheManager() {
    }

    public int getQuerySize() {
        return querySize;
    }

    public void setQuerySize(int querySize) {
        this.querySize = querySize;
    }

    public Cache create(CacheKey key) {

        Date creationDate = new Date();
        Date expirationDate = expiration == 0 ? null : new Date(creationDate.getTime() + expiration * 60 * 1000);

        Cache cache = new Cache();
        cache.setKey(key);
        cache.setCreationDate(creationDate);
        cache.setExpirationDate(expirationDate);
        
        return cache;
    }

    public void add(Cache cache) {

        boolean debug = log.isDebugEnabled();
        CacheKey key = cache.getKey();
        if (debug) log.debug("Adding cache key "+key.getEntryId()+".");

        SearchResponse response = cache.getResponse();
        long totalCount = response.getTotalCount();

        if (resultSize > 0 && totalCount > resultSize) {
            if (debug) log.debug("Result size ("+totalCount+") is too big.");
            return;
        }

        caches.put(key, cache);
        purge();
    }

    public synchronized Cache get(CacheKey key) {
        boolean debug = log.isDebugEnabled();
        if (debug) log.debug("Getting cache key "+key.getEntryId()+".");
        Cache cache = caches.get(key);
        if (cache == null) return null;

        Date expirationDate = cache.getExpirationDate();
        if (expirationDate != null && expirationDate.getTime() <= System.currentTimeMillis()) return null;

        caches.put(key, cache);

        purge();

        return cache;
    }

    public synchronized void purge() {
        boolean debug = log.isDebugEnabled();
        if (querySize == 0) return;

        int counter = caches.size() - querySize;
        for (int i=0; i<counter; i++) {
            CacheKey key = caches.keySet().iterator().next();
            if (debug) log.debug("Removing cache key "+key.getEntryId()+".");
            caches.remove(key);
        }
    }

    public synchronized void clear() {
        caches.clear();
    }

    public int getExpiration() {
        return expiration;
    }

    public void setExpiration(int expiration) {
        this.expiration = expiration;
    }

    public int getResultSize() {
        return resultSize;
    }

    public void setResultSize(int resultSize) {
        this.resultSize = resultSize;
    }
}
