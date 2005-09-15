/**
 * Copyright (c) 2000-2005, Identyx Corporation.
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

import org.safehaus.penrose.mapping.Entry;
import org.safehaus.penrose.mapping.Row;
import org.safehaus.penrose.mapping.EntryDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class EntryDataCache {

    Logger log = LoggerFactory.getLogger(getClass());

    private EntryDefinition entryDefinition;
    private Cache cache;
    private CacheContext cacheContext;

    private Map dataMap = new TreeMap();
    private Map expirationMap = new LinkedHashMap();

    private int size;
    private int expiration; // minutes

    public EntryDataCache(Cache cache, EntryDefinition entryDefinition) {
        this.cache = cache;
        this.cacheContext = cache.getCacheContext();
        this.entryDefinition = entryDefinition;

        String s = entryDefinition.getParameter(EntryDefinition.DATA_CACHE_SIZE);
        size = s == null ? EntryDefinition.DEFAULT_DATA_CACHE_SIZE : Integer.parseInt(s);

        s = entryDefinition.getParameter(EntryDefinition.DATA_CACHE_EXPIRATION);
        expiration = s == null ? EntryDefinition.DEFAULT_DATA_CACHE_EXPIRATION : Integer.parseInt(s);
    }

    public void init() throws Exception {
    }

    public Entry get(Row rdn) throws Exception {

        Row key = cacheContext.getSchema().normalize(rdn);

        log.debug("Getting entry cache ("+dataMap.size()+"): "+key);

        Entry entry = (Entry)dataMap.get(key);
        Date date = (Date)expirationMap.get(key);

        if (date == null || date.getTime() <= System.currentTimeMillis()) {
            dataMap.remove(key);
            expirationMap.remove(key);
            return null;
        }

        return entry;
    }

    public void put(Row rdn, Entry entry) throws Exception {

        Row key = cacheContext.getSchema().normalize(rdn);

        Object object = dataMap.get(key);

        while (object == null && dataMap.size() >= size) {
            log.debug("Trimming entry cache ("+dataMap.size()+").");
            Object k = expirationMap.keySet().iterator().next();
            dataMap.remove(k);
            expirationMap.remove(k);
        }

        log.debug("Storing entry cache ("+dataMap.size()+"): "+key);
        dataMap.put(key, entry);
        expirationMap.put(key, new Date(System.currentTimeMillis() + expiration * 60 * 1000));
    }

    public void remove(Row rdn) throws Exception {

        Row key = cacheContext.getSchema().normalize(rdn);

        log.debug("Removing entry cache ("+dataMap.size()+"): "+key);
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
