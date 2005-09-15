/**
 * Copyright (c) 2000-2005, Identyx Corporation.
 * All rights reserved.
 */
package org.safehaus.penrose.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.mapping.Source;
import org.safehaus.penrose.mapping.EntryDefinition;
import org.safehaus.penrose.mapping.Entry;

import java.util.Collection;
import java.util.TreeMap;
import java.util.Map;

/**
 * @author Endi S. Dewata
 */
public class Cache {

    public final static String SIZE       = "size";
    public final static String EXPIRATION = "expiration";

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

        init();
    }

    public void init() throws Exception {
    }

    public EntryFilterCache getEntryFilterCache(Entry parent, EntryDefinition entry) throws Exception {
        String key = entry.getRdn()+","+parent.getDn();
        EntryFilterCache cache = (EntryFilterCache)entryFilterCaches.get(key);
        if (cache == null) {
            cache = new EntryFilterCache();
            cache.init(this);
            entryFilterCaches.put(key, cache);
        }
        return cache;
    }

    public EntryDataCache getEntryDataCache(Entry parent, EntryDefinition entry) throws Exception {
        String key = entry.getRdn()+","+parent.getDn();
        EntryDataCache cache = (EntryDataCache)entryDataCaches.get(key);
        if (cache == null) {
            cache = new EntryDataCache();
            cache.init(this);
            entryDataCaches.put(key, cache);
        }
        return cache;
    }

    public SourceDataCache getSourceDataCache(Source source) throws Exception {
        String key = source.getConnectionName()+"."+source.getSourceName();
        SourceDataCache cache = (SourceDataCache)sourceDataCaches.get(key);
        if (cache == null) {
            cache = new SourceDataCache();
            cache.init(this);
            sourceDataCaches.put(key, cache);
        }
        return cache;
    }

    public SourceFilterCache getSourceFilterCache(Source source) throws Exception {
        String key = source.getConnectionName()+"."+source.getSourceName();
        SourceFilterCache cache = (SourceFilterCache)sourceFilterCaches.get(key);
        if (cache == null) {
            cache = new SourceFilterCache();
            cache.init(this);
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
