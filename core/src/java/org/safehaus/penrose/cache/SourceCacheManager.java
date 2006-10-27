/**
 * Copyright (c) 2000-2006, Identyx Corporation.
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

import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.source.SourceConfig;
import org.safehaus.penrose.connection.ConnectionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

/**
 * @author Endi S. Dewata
 */
public class SourceCacheManager {

    Logger log = LoggerFactory.getLogger(getClass());

    CacheConfig cacheConfig;

    ConnectionManager connectionManager;

    private Map caches = new TreeMap();

    public SourceCache createCacheStorage(Partition partition, SourceConfig sourceConfig) throws Exception {

        String cacheClass = cacheConfig.getCacheClass() == null ? SourceCache.class.getName() : cacheConfig.getCacheClass();

        log.debug("Initializing source cache "+cacheClass);
        Class clazz = Class.forName(cacheClass);
        SourceCache sourceCache = (SourceCache)clazz.newInstance();

        sourceCache.setSourceDefinition(sourceConfig);
        sourceCache.setPartition(partition);
        sourceCache.setSourceCacheManager(this);

        sourceCache.init(cacheConfig);

        return sourceCache;
    }

    public ConnectionManager getConnectionManager() {
        return connectionManager;
    }

    public void setConnectionManager(ConnectionManager source) {
        this.connectionManager = source;
    }

    public CacheConfig getCacheConfig() {
        return cacheConfig;
    }

    public void setCacheConfig(CacheConfig cacheConfig) {
        this.cacheConfig = cacheConfig;
    }

    public SourceCache create(Partition partition, SourceConfig sourceConfig) throws Exception {
        SourceCache sourceCache = createCacheStorage(partition, sourceConfig);

        Map map = (Map)caches.get(partition.getName());
        if (map == null) {
            map = new TreeMap();
            caches.put(partition.getName(), map);
        }
        map.put(sourceConfig.getName(), sourceCache);

        return sourceCache;
    }

    public SourceCache getSourceCache(Partition partition, SourceConfig sourceConfig) throws Exception {
        Map map = (Map)caches.get(partition.getName());
        if (map == null) return null;
        return (SourceCache)map.get(sourceConfig.getName());
    }

    public void create() throws Exception {
        for (Iterator i=caches.keySet().iterator(); i.hasNext(); ) {
            String partitionName = (String)i.next();
            Map map = (Map)caches.get(partitionName);

            for (Iterator j=map.keySet().iterator(); j.hasNext(); ) {
                String sourceName = (String)j.next();
                SourceCache sourceCache = (SourceCache)map.get(sourceName);
                sourceCache.create();
            }
        }
    }

    public void load() throws Exception {
        for (Iterator i=caches.keySet().iterator(); i.hasNext(); ) {
            String partitionName = (String)i.next();
            Map map = (Map)caches.get(partitionName);

            for (Iterator j=map.keySet().iterator(); j.hasNext(); ) {
                String sourceName = (String)j.next();
                SourceCache sourceCache = (SourceCache)map.get(sourceName);
                sourceCache.load();
            }
        }
    }

    public void clean() throws Exception {
        for (Iterator i=caches.keySet().iterator(); i.hasNext(); ) {
            String partitionName = (String)i.next();
            Map map = (Map)caches.get(partitionName);

            for (Iterator j=map.keySet().iterator(); j.hasNext(); ) {
                String sourceName = (String)j.next();
                SourceCache sourceCache = (SourceCache)map.get(sourceName);
                sourceCache.clean();
            }
        }
    }

    public void drop() throws Exception {
        for (Iterator i=caches.keySet().iterator(); i.hasNext(); ) {
            String partitionName = (String)i.next();
            Map map = (Map)caches.get(partitionName);

            for (Iterator j=map.keySet().iterator(); j.hasNext(); ) {
                String sourceName = (String)j.next();
                SourceCache sourceCache = (SourceCache)map.get(sourceName);
                sourceCache.drop();
            }
        }
    }
}
