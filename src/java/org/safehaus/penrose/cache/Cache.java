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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.mapping.*;

import java.util.Collection;
import java.util.TreeMap;
import java.util.Map;
import java.lang.reflect.Constructor;

/**
 * @author Endi S. Dewata
 */
public class Cache {

    Logger log = LoggerFactory.getLogger(getClass());

    private CacheConfig cacheConfig;
    private CacheContext cacheContext;

    private Map entryFilterCaches = new TreeMap();
    private Map entryDataCaches = new TreeMap();

    private Map sourceFilterCaches = new TreeMap();
    private Map sourceDataCaches = new TreeMap();

    protected CacheFilterTool cacheFilterTool;

    public CacheConfig getCacheConfig() {
        return cacheConfig;
    }

    public void setCacheConfig(CacheConfig cacheConfig) {
        this.cacheConfig = cacheConfig;
    }

    public Collection getParameterNames() {
        return cacheConfig.getParameterNames();
    }

    public String getParameter(String name) {
        return cacheConfig.getParameter(name);
    }

    public void init(CacheConfig cacheConfig, CacheContext cacheContext) throws Exception {
        this.cacheConfig = cacheConfig;
        this.cacheContext = cacheContext;

        cacheFilterTool = new CacheFilterTool(getCacheContext());
    }

    public EntryFilterCache getEntryFilterCache(Entry parent, EntryDefinition entry) throws Exception {
        String key = entry.getRdn()+","+parent.getDn();
        EntryFilterCache cache = (EntryFilterCache)entryFilterCaches.get(key);
        if (cache == null) {
            cache = new EntryFilterCache(this, entry);
            entryFilterCaches.put(key, cache);
        }
        return cache;
    }

    public EntryDataCache getEntryDataCache(Entry parent, EntryDefinition entry) throws Exception {
        String key = entry.getRdn()+","+parent.getDn();
        EntryDataCache cache = (EntryDataCache)entryDataCaches.get(key);
        if (cache == null) {
            cache = new EntryDataCache(this, entry);
            entryDataCaches.put(key, cache);
        }
        return cache;
    }

    public SourceDataCache getSourceDataCache(ConnectionConfig connectionConfig, SourceDefinition sourceDefinition) throws Exception {
        String key = connectionConfig.getConnectionName()+"."+sourceDefinition.getName();
        SourceDataCache cache = (SourceDataCache)sourceDataCaches.get(key);
        if (cache == null) {
            String sourceDataCache = getParameter("sourceDataCache");
            sourceDataCache = sourceDataCache == null ? InMemorySourceDataCache.class.getName() : sourceDataCache;
            Class clazz = Class.forName(sourceDataCache);
            Constructor constructor = clazz.getConstructor(new Class[] { Cache.class, SourceDefinition.class });
            cache = (SourceDataCache)constructor.newInstance(new Object[] { this, sourceDefinition });
            cache.init();
            sourceDataCaches.put(key, cache);
        }
        return cache;
    }

    public SourceFilterCache getSourceFilterCache(ConnectionConfig connectionConfig, SourceDefinition sourceDefinition) throws Exception {
        String key = connectionConfig.getConnectionName()+"."+sourceDefinition.getName();
        SourceFilterCache cache = (SourceFilterCache)sourceFilterCaches.get(key);
        if (cache == null) {
            cache = new SourceFilterCache(this, sourceDefinition);
            sourceFilterCaches.put(key, cache);
        }
        return cache;
    }

    public CacheContext getCacheContext() {
        return cacheContext;
    }

    public void setCacheContext(CacheContext cacheContext) {
        this.cacheContext = cacheContext;
    }

    public CacheFilterTool getCacheFilterTool() {
        return cacheFilterTool;
    }

    public void setCacheFilterTool(CacheFilterTool cacheFilterTool) {
        this.cacheFilterTool = cacheFilterTool;
    }
}
