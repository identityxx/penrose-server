package org.safehaus.penrose.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;

/**
 * @author Endi Sukma Dewata
 */
public class CacheManager {

    public Logger log = LoggerFactory.getLogger(getClass());

    private int size       = 10;
    private int expiration = 5; // minutes

    private LinkedHashMap<CacheKey,Cache> caches = new LinkedHashMap<CacheKey,Cache>();

    public CacheManager() {
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public Cache create(CacheKey key) {
        Cache cache = new Cache();
        cache.setExpiration(expiration);

        put(key, cache);

        return cache;
    }

    public synchronized void put(CacheKey key, Cache cache) {
        log.debug("Adding cache key "+key.getEntryId());
        caches.put(key, cache);
        purge();
    }

    public synchronized Cache get(CacheKey key) {
        log.debug("Getting cache key "+key.getEntryId());
        Cache cache = caches.get(key);
        if (cache == null) return null;
        if (cache.isExpired()) return null;

        caches.put(key, cache);

        purge();

        return cache;
    }

    public synchronized void purge() {
        if (size == 0) return;

        int counter = caches.size() - size;
        for (int i=0; i<counter; i++) {
            CacheKey key = caches.keySet().iterator().next();
            log.debug("Removing cache key "+key.getEntryId());
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
}
