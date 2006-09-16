package org.safehaus.penrose.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;

/**
 * @author Endi S. Dewata
 */
public class LRUCache {

    Logger log = LoggerFactory.getLogger(getClass());

    private int size;
    private LinkedHashMap map = new LinkedHashMap();

    public LRUCache() {
        this(10);
    }

    public LRUCache(int size) {
        this.size = size;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public synchronized void put(Object key, Object value) {
        //log.debug("Adding cache key "+key);
        map.put(key, value);
        prune();
    }

    public synchronized Object get(Object key) {
        //log.debug("Getting cache key "+key);
        Object value = map.remove(key);
        if (value == null) return value;

        map.put(key, value);
        prune();
        return value;
    }

    public synchronized void prune() {
        while (map.size() > size && map.size() > 0) {
            Object key = map.keySet().iterator().next();
            //log.debug("Removing cache key "+key);
            map.remove(key);
        }
    }
}
