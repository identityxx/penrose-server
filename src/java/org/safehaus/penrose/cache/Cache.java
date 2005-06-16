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
public abstract class Cache {

    public Logger log = Logger.getLogger(Penrose.CACHE_LOGGER);

    private CacheConfig cacheConfig;
    public CacheContext cacheContext;
    public Config config;

    public CacheFilterTool cacheFilterTool;

    public void init(CacheConfig cacheConfig, CacheContext cacheContext) throws Exception {
        this.cacheConfig = cacheConfig;
        this.cacheContext = cacheContext;
        this.config = this.cacheContext.getConfig();

        cacheFilterTool = new CacheFilterTool(cacheContext);

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

    public abstract Collection search(EntryDefinition entry, Collection primaryKeys) throws Exception;

    public abstract void insert(EntryDefinition entry, AttributeValues values, Date date) throws Exception;
    public abstract void insert(EntryDefinition entry, Row row, Date date) throws Exception;
    public abstract void delete(EntryDefinition entry, AttributeValues values, Date date) throws Exception;
    public abstract void delete(EntryDefinition entry, String filter, Date date) throws Exception;

    public abstract Date getModifyTime(EntryDefinition entry, String filter) throws Exception;
    public abstract Date getModifyTime(EntryDefinition entry) throws Exception;

    public abstract void setModifyTime(EntryDefinition entry, Date date) throws Exception;

    public abstract Collection joinSources(EntryDefinition entry, Graph graph, Map sourceGraph, Source primarySource, String sqlFilter) throws Exception;

    public abstract Collection searchPrimaryKeys(
            EntryDefinition entry,
            Filter filter)
            throws Exception;

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

    public abstract CacheFilterTool getCacheFilterTool();

    public abstract void setCacheFilterTool(CacheFilterTool cacheFilterTool);

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
}
