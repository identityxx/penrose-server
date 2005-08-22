/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.cache;

import org.safehaus.penrose.Penrose;
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

    private FilterCache filterCache;
    private EntryCache entryCache;
    private SourceCache sourceCache;
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

        init();

        filterCache.init(this);
        entryCache.init(this);
        sourceCache.init(this);
    }

    public void createFilterCache() throws Exception {
        filterCache = new FilterCache();
    }

    public void createEntryCache() throws Exception {
        entryCache = new EntryCache();
    }

    public void createSourceCache() throws Exception {
        sourceCache = new SourceCache();
    }

    public void init() throws Exception {
    }

    public EntryCache getEntryCache() {
        return entryCache;
    }

    public void setEntryCache(EntryCache entryCache) {
        this.entryCache = entryCache;
    }

    public SourceCache getSourceCache() {
        return sourceCache;
    }

    public void setSourceCache(SourceCache sourceCache) {
        this.sourceCache = sourceCache;
    }

    public CacheContext getCacheContext() {
        return cacheContext;
    }

    public void setCacheContext(CacheContext cacheContext) {
        this.cacheContext = cacheContext;
    }

    public FilterCache getFilterCache() {
        return filterCache;
    }

    public void setFilterCache(FilterCache filterCache) {
        this.filterCache = filterCache;
    }

    public CacheFilterTool getCacheFilterTool() {
        return cacheFilterTool;
    }

    public void setCacheFilterTool(CacheFilterTool cacheFilterTool) {
        this.cacheFilterTool = cacheFilterTool;
    }
}
