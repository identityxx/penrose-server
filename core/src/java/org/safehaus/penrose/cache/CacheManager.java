package org.safehaus.penrose.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;

/**
 * @author Endi Sukma Dewata
 */
public class CacheManager {

    public Logger log = LoggerFactory.getLogger(getClass());

    private int size;
    private int expiration;

    private LinkedHashMap<CacheKey,Cache> caches = new LinkedHashMap<CacheKey,Cache>();

    public CacheManager() {
        this(10);
    }

    public CacheManager(int size) {
        this.size = size;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public synchronized Cache create() {
        Cache cache = new Cache();
        cache.setSize(size);
        cache.setExpiration(expiration);

        return cache;
    }

    public synchronized void put(CacheKey key, Cache value) {
        //log.debug("Adding cache key "+key);
        caches.put(key, value);
        prune();
    }

    public synchronized Cache get(CacheKey key) {
        //log.debug("Getting cache key "+key);
        Cache cache = caches.remove(key);
        if (cache == null) return null;
        if (cache.isExpired()) return null;

        caches.put(key, cache);
        prune();
        return cache;
    }

    public synchronized void prune() {
        if (size == 0) return;
        while (caches.size() > size && caches.size() > 0) {
            CacheKey key = caches.keySet().iterator().next();
            //log.debug("Removing cache key "+key);
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
