/**
 * Copyright 2009 Red Hat, Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */
package org.safehaus.penrose.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;

/**
 * @author Endi S. Dewata
 */
public class LRUCache {

    public Logger log = LoggerFactory.getLogger(getClass());

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
