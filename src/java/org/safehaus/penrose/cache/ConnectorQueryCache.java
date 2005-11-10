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
public abstract class ConnectorQueryCache {

    Logger log = Logger.getLogger(getClass());

    private CacheConfig cacheConfig;
    private SourceDefinition sourceDefinition;

    private int size;
    private int expiration; // minutes

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

    public abstract Collection get(Filter filter) throws Exception;

    public abstract void put(Filter filter, Collection pks) throws Exception;

    public abstract void invalidate() throws Exception;

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

    public void setSourceDefinition(SourceDefinition sourceDefinition) {
        this.sourceDefinition = sourceDefinition;
    }

    public int getExpiration() {
        return expiration;
    }

    public void setExpiration(int expiration) {
        this.expiration = expiration;
    }
}
