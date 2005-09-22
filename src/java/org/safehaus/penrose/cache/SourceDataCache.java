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

import org.safehaus.penrose.mapping.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public abstract class SourceDataCache {

    Logger log = LoggerFactory.getLogger(getClass());

    SourceDefinition sourceDefinition;
    Cache cache;
    CacheContext cacheContext;

    int size;
    int expiration; // minutes

    public SourceDataCache(Cache cache, SourceDefinition sourceDefinition) {
        this.cache = cache;
        this.cacheContext = cache.getCacheContext();
        this.sourceDefinition = sourceDefinition;

        String s = sourceDefinition.getParameter(SourceDefinition.DATA_CACHE_SIZE);
        size = s == null ? SourceDefinition.DEFAULT_DATA_CACHE_SIZE : Integer.parseInt(s);

        s = sourceDefinition.getParameter(SourceDefinition.DATA_CACHE_EXPIRATION);
        expiration = s == null ? SourceDefinition.DEFAULT_DATA_CACHE_EXPIRATION : Integer.parseInt(s);
    }

    public void init() throws Exception {
    }

    public abstract Map get(Collection filters) throws Exception;

    public abstract void put(Row pk, AttributeValues values) throws Exception;

    public abstract void remove(Row pk) throws Exception;

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

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public int getExpiration() {
        return expiration;
    }

    public void setExpiration(int expiration) {
        this.expiration = expiration;
    }

    public SourceDefinition getSourceDefinition() {
        return sourceDefinition;
    }

    public void setSourceDefinition(SourceDefinition sourceDefinition) {
        this.sourceDefinition = sourceDefinition;
    }
}
