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

import org.apache.log4j.Logger;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.mapping.SourceDefinition;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class ConnectorQueryCache {

    Logger log = Logger.getLogger(getClass());

    public Map dataMap = new TreeMap();
    public Map expirationMap = new LinkedHashMap();

    private CacheConfig cacheConfig;
    private SourceDefinition sourceDefinition;

    private int size;
    private int expiration; // minutes

    public void setSourceDefinition(SourceDefinition sourceDefinition) {
        this.sourceDefinition = sourceDefinition;
    }

    public void init(CacheConfig cacheConfig) throws Exception {
        this.cacheConfig = cacheConfig;

        String s = cacheConfig.getParameter(CacheConfig.CACHE_SIZE);
        size = s == null ? CacheConfig.DEFAULT_CACHE_SIZE : Integer.parseInt(s);

        s = cacheConfig.getParameter(CacheConfig.CACHE_EXPIRATION);
        expiration = s == null ? CacheConfig.DEFAULT_CACHE_EXPIRATION : Integer.parseInt(s);

        init();
    }

    public void init() throws Exception {
        String s = sourceDefinition.getParameter(SourceDefinition.QUERY_CACHE_SIZE);
        if (s != null) size = Integer.parseInt(s);

        s = sourceDefinition.getParameter(SourceDefinition.QUERY_CACHE_EXPIRATION);
        if (s != null) expiration = Integer.parseInt(s);
    }

    public Collection get(Filter filter) throws Exception {

        String key = filter == null ? "" : filter.toString();

        Collection pks = (Collection)dataMap.get(key);
        Date date = (Date)expirationMap.get(key);

        if (date == null || date.getTime() <= System.currentTimeMillis()) {
            dataMap.remove(key);
            expirationMap.remove(key);
            pks = null;
        }

        //log.debug("Getting source filter cache: ["+key+"] => "+pks);

        return pks;
    }

    public void put(Filter filter, Collection pks) throws Exception {
        if (size == 0) return;
        
        String key = filter == null ? "" : filter.toString();

        Object object = dataMap.get(key);

        while (object == null && dataMap.size() >= size) {
            //log.debug("Trimming source filter cache ("+dataMap.size()+").");
            Object k = dataMap.keySet().iterator().next();
            dataMap.remove(k);
            expirationMap.remove(k);
        }

        //log.debug("Storing source filter cache: ["+key+"] => "+pks);
        dataMap.put(key, pks);
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

    public CacheConfig getCacheConfig() {
        return cacheConfig;
    }

    public void setCacheConfig(CacheConfig cacheConfig) {
        this.cacheConfig = cacheConfig;
    }

    public SourceDefinition getSourceDefinition() {
        return sourceDefinition;
    }
}
