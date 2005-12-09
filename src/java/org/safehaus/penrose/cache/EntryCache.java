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

import org.safehaus.penrose.mapping.EntryMapping;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.engine.Engine;
import org.apache.log4j.Logger;

import java.util.Collection;
import java.util.Map;

/**
 * @author Endi S. Dewata
 */
public abstract class EntryCache {

    Logger log = Logger.getLogger(getClass());

    String parentDn;
    EntryMapping entryMapping;

    CacheConfig cacheConfig;
    Engine engine;

    int size;
    int expiration; // minutes

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

    public void init(CacheConfig cacheConfig) throws Exception {
        this.cacheConfig = cacheConfig;

        String s = cacheConfig.getParameter(CacheConfig.CACHE_SIZE);
        size = s == null ? CacheConfig.DEFAULT_CACHE_SIZE : Integer.parseInt(s);

        s = cacheConfig.getParameter(CacheConfig.CACHE_EXPIRATION);
        expiration = s == null ? CacheConfig.DEFAULT_CACHE_EXPIRATION : Integer.parseInt(s);

        init();
    }

    public void init() throws Exception {

        String s = entryMapping.getParameter(EntryMapping.DATA_CACHE_SIZE);
        if (s != null) size = Integer.parseInt(s);

        s = entryMapping.getParameter(EntryMapping.DATA_CACHE_EXPIRATION);
        if (s != null) expiration = Integer.parseInt(s);
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

    public EntryMapping getEntryMapping() {
        return entryMapping;
    }

    public void setEntryMapping(EntryMapping entryMapping) {
        this.entryMapping = entryMapping;
    }

    public String getParentDn() {
        return parentDn;
    }

    public void setParentDn(String parentDn) {
        this.parentDn = parentDn;
    }

    public Collection search(Filter filter) throws Exception {
        return null;
    }

    public void put(Filter filter, Collection rdns) throws Exception {
    }

    public void invalidate() throws Exception {
    }

    public void create() throws Exception {
    }

    public void load() throws Exception {
    }

    public void clean() throws Exception {
    }

    public void drop() throws Exception {
    }

    public Engine getEngine() {
        return engine;
    }

    public void setEngine(Engine engine) {
        this.engine = engine;
    }

    public abstract Object get(Object key) throws Exception;

    public abstract Map getExpired() throws Exception;

    public abstract void put(Object key, Object object) throws Exception;

    public abstract void remove(Object key) throws Exception;
}
