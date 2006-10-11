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

import org.safehaus.penrose.connector.Connector;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.PartitionConfig;
import org.safehaus.penrose.partition.SourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

/**
 * @author Endi S. Dewata
 */
public class SourceCacheManager {

    Logger log = LoggerFactory.getLogger(getClass());

    CacheConfig cacheConfig;

    Connector connector;

    private Map caches = new TreeMap();

    public SourceCache createCacheStorage(Partition partition, SourceConfig sourceConfig) throws Exception {

        String cacheClass = cacheConfig.getCacheClass() == null ? SourceCache.class.getName() : cacheConfig.getCacheClass();

        log.debug("Initializing source cache "+cacheClass);
        Class clazz = Class.forName(cacheClass);
        SourceCache sourceCache = (SourceCache)clazz.newInstance();

        sourceCache.setSourceDefinition(sourceConfig);
        sourceCache.setPartition(partition);
        sourceCache.setConnector(connector);

        sourceCache.init(cacheConfig);

        return sourceCache;
    }

    public Connector getConnector() {
        return connector;
    }

    public void setConnector(Connector connector) {
        this.connector = connector;
    }

    public CacheConfig getCacheConfig() {
        return cacheConfig;
    }

    public void setCacheConfig(CacheConfig cacheConfig) {
        this.cacheConfig = cacheConfig;
    }

    public SourceCache getCacheStorage(Partition partition, SourceConfig sourceConfig) throws Exception {
        PartitionConfig partitionConfig = partition.getPartitionConfig();
        String cacheName = partitionConfig.getName()+"."+sourceConfig.getName();
        //log.debug("Getting source cache "+cacheName+".");
        return (SourceCache)caches.get(cacheName);
    }

    public void create() throws Exception {
        for (Iterator i=caches.values().iterator(); i.hasNext(); ) {
            SourceCache sourceCache = (SourceCache)i.next();
            sourceCache.create();
        }
    }

    public void create(Partition partition, SourceConfig sourceConfig) throws Exception {

        PartitionConfig partitionConfig = partition.getPartitionConfig();

        SourceCache sourceCache = createCacheStorage(partition, sourceConfig);
        caches.put(partitionConfig.getName()+"."+sourceConfig.getName(), sourceCache);
    }

    public void load() throws Exception {
        for (Iterator i=caches.values().iterator(); i.hasNext(); ) {
            SourceCache sourceCache = (SourceCache)i.next();
            sourceCache.load();
        }
    }

    public void clean() throws Exception {
        for (Iterator i=caches.values().iterator(); i.hasNext(); ) {
            SourceCache sourceCache = (SourceCache)i.next();
            sourceCache.clean();
        }
    }

    public void drop() throws Exception {
        for (Iterator i=caches.values().iterator(); i.hasNext(); ) {
            SourceCache sourceCache = (SourceCache)i.next();
            sourceCache.drop();
        }
    }

    public void remove(Partition partition, SourceConfig sourceConfig, Object key) throws Exception {
        getCacheStorage(partition, sourceConfig).remove(key);
    }

    public Object get(Partition partition, SourceConfig sourceConfig, Object key) throws Exception {
        return getCacheStorage(partition, sourceConfig).get(key);
    }

    public void put(Partition partition, SourceConfig sourceConfig, Object pk, Object sourceValues) throws Exception {
        getCacheStorage(partition, sourceConfig).put(pk, sourceValues);
    }

    public void put(Partition partition, SourceConfig sourceConfig, Filter filter, Collection pks) throws Exception {
        getCacheStorage(partition, sourceConfig).put(filter, pks);
    }

    public Collection search(Partition partition, SourceConfig sourceConfig, Filter filter) throws Exception {
        return getCacheStorage(partition, sourceConfig).search(filter);
    }

    public Map load(Partition partition, SourceConfig sourceConfig, Collection filters, Collection missingKeys) throws Exception {
        return getCacheStorage(partition, sourceConfig).load(filters, missingKeys);
    }

    public Map getExpired(Partition partition, SourceConfig sourceConfig) throws Exception {
        return getCacheStorage(partition, sourceConfig).getExpired();
    }

    public int getLastChangeNumber(Partition partition, SourceConfig sourceConfig) throws Exception {
        return getCacheStorage(partition, sourceConfig).getLastChangeNumber();
    }

    public void setLastChangeNumber(Partition partition, SourceConfig sourceConfig, int lastChangeNumber) throws Exception {
        getCacheStorage(partition, sourceConfig).setLastChangeNumber(lastChangeNumber);
    }
}
