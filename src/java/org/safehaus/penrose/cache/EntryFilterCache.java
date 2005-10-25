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

import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.mapping.EntryDefinition;
import org.apache.log4j.Logger;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class EntryFilterCache {

    Logger log = Logger.getLogger(getClass());

    public Map dataMap = new TreeMap();
    public Map expirationMap = new LinkedHashMap();

    private CacheConfig cacheConfig;
    private CacheContext cacheContext;

    private String parentDn;
    private EntryDefinition entryDefinition;

    private int size;
    private int expiration; // minutes

    public void init(CacheConfig cacheConfig, CacheContext cacheContext) throws Exception {
        this.cacheConfig = cacheConfig;
        this.cacheContext = cacheContext;

        String s = cacheConfig.getParameter(CacheConfig.CACHE_SIZE);
        size = s == null ? CacheConfig.DEFAULT_CACHE_SIZE : Integer.parseInt(s);

        s = cacheConfig.getParameter(CacheConfig.CACHE_EXPIRATION);
        expiration = s == null ? CacheConfig.DEFAULT_CACHE_EXPIRATION : Integer.parseInt(s);

        init();
    }

    public void init() throws Exception {
        String s = entryDefinition.getParameter(EntryDefinition.FILTER_CACHE_SIZE);
        if (s != null) size = Integer.parseInt(s);

        s = entryDefinition.getParameter(EntryDefinition.FILTER_CACHE_EXPIRATION);
        if (s != null) expiration = Integer.parseInt(s);
    }

    public Collection get(Filter filter) throws Exception {

        String key = filter == null ? "" : filter.toString();
        //log.debug("Getting entry filter cache ("+dataMap.size()+"): "+key);

        Collection rdns = (Collection)dataMap.get(key);
        Date date = (Date)expirationMap.get(key);

        if (date == null || date.getTime() <= System.currentTimeMillis()) {
            dataMap.remove(key);
            expirationMap.remove(key);
            return null;
        }

        return rdns;
    }

    public void put(Filter filter, Collection rdns) throws Exception {
        if (size == 0) return;

        String key = filter == null ? "" : filter.toString();

        Object object = (Collection)dataMap.remove(key);

        while (object == null && dataMap.size() >= size) {
            //log.debug("Trimming entry filter cache ("+dataMap.size()+").");
            Object k = expirationMap.keySet().iterator().next();
            dataMap.remove(k);
            expirationMap.remove(k);
        }

        //log.debug("Storing entry filter cache ("+dataMap.size()+"): "+key);
        dataMap.put(key, rdns);
        expirationMap.put(key, new Date(System.currentTimeMillis() + expiration * 60 * 1000));
    }

    public void invalidate() throws Exception {
        dataMap.clear();
        expirationMap.clear();
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

    public CacheConfig getCacheConfig() {
        return cacheConfig;
    }

    public void setCacheConfig(CacheConfig cacheConfig) {
        this.cacheConfig = cacheConfig;
    }

    public CacheContext getCacheContext() {
        return cacheContext;
    }

    public void setCacheContext(CacheContext cacheContext) {
        this.cacheContext = cacheContext;
    }

    public String getParentDn() {
        return parentDn;
    }

    public void setParentDn(String parentDn) {
        this.parentDn = parentDn;
    }

    public EntryDefinition getEntryDefinition() {
        return entryDefinition;
    }

    public void setEntryDefinition(EntryDefinition entryDefinition) {
        this.entryDefinition = entryDefinition;
    }
}
