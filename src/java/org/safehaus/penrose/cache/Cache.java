/**
 * Copyright (c) 2000-2005, Identyx Corporation.
 * All rights reserved.
 */
package org.safehaus.penrose.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.mapping.*;

import java.util.Collection;
import java.util.TreeMap;
import java.util.Map;

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
            cache = new SourceDataCache(this, sourceDefinition);
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
