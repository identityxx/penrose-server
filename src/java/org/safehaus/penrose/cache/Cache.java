/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.cache;

import org.safehaus.penrose.mapping.Source;
import org.safehaus.penrose.mapping.EntryDefinition;
import org.safehaus.penrose.mapping.SourceDefinition;
import org.safehaus.penrose.mapping.Row;
import org.safehaus.penrose.config.Config;
import org.safehaus.penrose.event.CacheEvent;
import org.safehaus.penrose.SearchResults;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.filter.Filter;
import org.apache.log4j.Logger;

import java.util.Date;
import java.util.Collection;
import java.util.Map;
import java.sql.ResultSet;

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

    public abstract void insert(Source source, Row row, Date date) throws Exception;

    public abstract void update(Source source, Row oldRow, Row newRow, Date date) throws Exception;

    public abstract void delete(Source source, Row row, Date date) throws Exception;

    public abstract Collection search(EntryDefinition entry, Collection primaryKeys) throws Exception;

    public abstract void insert(EntryDefinition entry, Row row, Date date) throws Exception;

    public abstract void delete(EntryDefinition entry, Row row, Date date) throws Exception;

    public abstract void delete(EntryDefinition entry, String filter, Date date) throws Exception;

    public abstract Date getModifyTime(Source source, String filter) throws Exception;

    public abstract Date getModifyTime(Source source) throws Exception;

    public abstract Date getModifyTime(EntryDefinition entry, String filter) throws Exception;

    public abstract Date getModifyTime(EntryDefinition entry) throws Exception;

    public abstract void setModifyTime(EntryDefinition entry, Date date) throws Exception;

    public abstract String getFieldNames(Collection sources);

    public abstract String getTableNames(EntryDefinition entry, boolean temporary);

    public abstract Row getRow(EntryDefinition entry, ResultSet rs) throws Exception;

    public abstract Collection joinSources(EntryDefinition entry) throws Exception;

    public abstract String getPkAttributeNames(EntryDefinition entry);

    public abstract Collection searchPrimaryKeys(
            EntryDefinition entry,
            String filter)
            throws Exception;

    public abstract Map getPk(EntryDefinition entry, ResultSet rs) throws Exception;

    public abstract void postCacheEvent(SourceDefinition sourceConfig, CacheEvent event)
            throws Exception;

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
