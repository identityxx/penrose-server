/**
 * Copyright (c) 2000-2005, Identyx Corporation.
 * All rights reserved.
 */
package org.safehaus.penrose.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public class Cache {

    Logger log = LoggerFactory.getLogger(getClass());

    private CacheConfig cacheConfig;
    private CacheContext cacheContext;

    private EntryFilterCache entryFilterCache;
    private EntryDataCache entryDataCache;
    private SourceDataCache sourceDataCache;
    private SourceFilterCache sourceFilterCache;

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

        createFilterCache();
        createEntryCache();
        createSourceCache();
        createSourceFilterCache();

        init();

        entryFilterCache.init(this);
        entryDataCache.init(this);
        sourceDataCache.init(this);
        sourceFilterCache.init(this);
    }

    public void createFilterCache() throws Exception {
        entryFilterCache = new EntryFilterCache();
    }

    public void createEntryCache() throws Exception {
        entryDataCache = new EntryDataCache();
    }

    public void createSourceCache() throws Exception {
        sourceDataCache = new SourceDataCache();
    }

    public void createSourceFilterCache() throws Exception {
        sourceFilterCache = new SourceFilterCache();
    }

    public void init() throws Exception {
    }

    public EntryDataCache getEntryDataCache() {
        return entryDataCache;
    }

    public void setEntryDataCache(EntryDataCache entryDataCache) {
        this.entryDataCache = entryDataCache;
    }

    public SourceDataCache getSourceDataCache() {
        return sourceDataCache;
    }

    public void setSourceDataCache(SourceDataCache sourceDataCache) {
        this.sourceDataCache = sourceDataCache;
    }

    public CacheContext getCacheContext() {
        return cacheContext;
    }

    public void setCacheContext(CacheContext cacheContext) {
        this.cacheContext = cacheContext;
    }

    public EntryFilterCache getEntryFilterCache() {
        return entryFilterCache;
    }

    public void setEntryFilterCache(EntryFilterCache entryFilterCache) {
        this.entryFilterCache = entryFilterCache;
    }

    public CacheFilterTool getCacheFilterTool() {
        return cacheFilterTool;
    }

    public void setCacheFilterTool(CacheFilterTool cacheFilterTool) {
        this.cacheFilterTool = cacheFilterTool;
    }

    public SourceFilterCache getSourceFilterCache() {
        return sourceFilterCache;
    }

    public void setSourceFilterCache(SourceFilterCache sourceFilterCache) {
        this.sourceFilterCache = sourceFilterCache;
    }
}
