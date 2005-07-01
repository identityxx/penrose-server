/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.cache;

import org.safehaus.penrose.mapping.EntryDefinition;
import org.safehaus.penrose.mapping.AttributeValues;
import org.safehaus.penrose.mapping.Row;
import org.safehaus.penrose.mapping.Entry;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.config.Config;
import org.apache.log4j.Logger;

import java.util.Collection;
import java.util.Date;
import java.util.Map;

/**
 * @author Endi S. Dewata
 */
public abstract class EntryCache {

    public Logger log = Logger.getLogger(Penrose.CACHE_LOGGER);

    private Cache cache;
    private CacheContext cacheContext;
    private Config config;

    public void init(Cache cache) throws Exception {
        this.cache = cache;
        this.cacheContext = cache.getCacheContext();
        this.config = cacheContext.getConfig();

        init();
    }

    public abstract void init() throws Exception;

    public abstract Collection getRdns(
            EntryDefinition entry,
            Collection pks,
            Date date)
            throws Exception;

    public abstract Entry get(EntryDefinition entry, Row pk) throws Exception;
    public abstract Map get(EntryDefinition entry, Collection pks) throws Exception;

    public abstract void put(Entry entry, Date date) throws Exception;
    public abstract void remove(Entry entry) throws Exception;

    public Cache getCache() {
        return cache;
    }

    public void setCache(Cache cache) {
        this.cache = cache;
    }

    public CacheContext getCacheContext() {
        return cacheContext;
    }

    public void setCacheContext(CacheContext cacheContext) {
        this.cacheContext = cacheContext;
    }

    public Config getConfig() {
        return config;
    }

    public void setConfig(Config config) {
        this.config = config;
    }
}
