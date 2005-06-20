/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.cache;

import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.config.Config;
import org.safehaus.penrose.event.CacheEvent;
import org.safehaus.penrose.event.CacheListener;
import org.safehaus.penrose.SearchResults;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.engine.Graph;
import org.safehaus.penrose.filter.Filter;
import org.apache.log4j.Logger;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public abstract class SourceCache {

    public Logger log = Logger.getLogger(Penrose.CACHE_LOGGER);

    private SourceCacheConfig sourceCacheConfig;
    public SourceCacheContext sourceCacheContext;
    public Config config;

    public SourceCacheFilterTool sourceCacheFilterTool;

    public void init(SourceCacheConfig sourceCacheConfig, SourceCacheContext sourceCacheContext) throws Exception {
        this.sourceCacheConfig = sourceCacheConfig;
        this.sourceCacheContext = sourceCacheContext;
        this.config = this.sourceCacheContext.getConfig();

        sourceCacheFilterTool = new SourceCacheFilterTool(sourceCacheContext);

        init();
    }

    public abstract void init() throws Exception;
    public abstract void refresh() throws Exception;

    public abstract Date getExpiration(Source source) throws Exception;
    public abstract void setExpiration(Source source, Date date) throws Exception;

    public abstract void insert(Source source, AttributeValues values, Date date) throws Exception;
    public abstract void delete(Source source, AttributeValues values, Date date) throws Exception;

    public abstract Date getModifyTime(Source source, String filter) throws Exception;
    public abstract Date getModifyTime(Source source) throws Exception;

    public abstract Collection joinSources(EntryDefinition entry, Graph graph, Source primarySource, String sqlFilter) throws Exception;

    public void postCacheEvent(SourceDefinition sourceConfig, CacheEvent event)
            throws Exception {
        List listeners = sourceConfig.getListeners();

        for (Iterator i = listeners.iterator(); i.hasNext();) {
            Object listener = i.next();
            if (!(listener instanceof CacheListener))
                continue;

            CacheListener cacheListener = (CacheListener) listener;
            switch (event.getType()) {
            case CacheEvent.BEFORE_LOAD_ENTRIES:
                cacheListener.beforeLoadEntries(event);
                break;

            case CacheEvent.AFTER_LOAD_ENTRIES:
                cacheListener.afterLoadEntries(event);
                break;
            }
        }
    }

    public abstract SearchResults loadSource(
            EntryDefinition entry,
            Source source,
            Filter sqlFilter,
            Date date)
            throws Exception;

    public abstract SourceCacheFilterTool getCacheFilterTool();

    public abstract void setCacheFilterTool(SourceCacheFilterTool sourceCacheFilterTool);

    public SourceCacheConfig getCacheConfig() {
        return sourceCacheConfig;
    }

    public void setCacheConfig(SourceCacheConfig sourceCacheConfig) {
        this.sourceCacheConfig = sourceCacheConfig;
    }

    public Collection getParameterNames() {
        return sourceCacheConfig.getParameterNames();
    }

    public String getParameter(String name) {
        return sourceCacheConfig.getParameter(name);
    }
}
