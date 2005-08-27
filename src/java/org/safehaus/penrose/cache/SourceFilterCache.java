/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.cache;

import java.util.Map;
import java.util.Collection;
import java.util.TreeMap;

/**
 * @author Endi S. Dewata
 */
public class SourceFilterCache {

    public Map map = new TreeMap();

    public Cache cache;

    public void init(Cache cache) throws Exception {
        this.cache = cache;

        init();
    }

    public void init() throws Exception {
    }

    public Collection get(Object key) throws Exception {
        return (Collection)map.get(key);
    }

    public void put(Object key, Collection rdns) throws Exception {
        map.put(key, rdns);

        if (map.size() > 20) {
            key = map.keySet().iterator().next();
            map.remove(key);
        }
    }

    public void invalidate() throws Exception {
        map.clear();
    }
}
