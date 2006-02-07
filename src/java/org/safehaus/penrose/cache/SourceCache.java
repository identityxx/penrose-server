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
import org.safehaus.penrose.connector.Connector;
import org.safehaus.penrose.connector.ConnectionManager;
import org.safehaus.penrose.partition.SourceConfig;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.PartitionManager;
import org.safehaus.penrose.partition.PartitionConfig;
import org.safehaus.penrose.config.PenroseConfig;
import org.safehaus.penrose.filter.Filter;

import java.util.Map;
import java.util.TreeMap;
import java.util.Iterator;
import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public class SourceCache {

    Logger log = Logger.getLogger(getClass());

    CacheConfig cacheConfig;
    Connector connector;
    PenroseConfig penroseConfig;
    ConnectionManager connectionManager;
    PartitionManager partitionManager;

    private Map caches = new TreeMap();

    public SourceCacheStorage createCacheStorage(SourceConfig sourceConfig) throws Exception {

        Partition partition = partitionManager.getPartition(sourceConfig);

        SourceCacheStorage sourceCacheStorage = new SourceCacheStorage();
        sourceCacheStorage.setSourceDefinition(sourceConfig);
        sourceCacheStorage.setPartition(partition);
        sourceCacheStorage.setConnector(connector);
        sourceCacheStorage.init(cacheConfig);

        return sourceCacheStorage;
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

    public SourceCacheStorage getCacheStorage(SourceConfig sourceConfig) throws Exception {
        Partition partition = partitionManager.getPartition(sourceConfig);
        PartitionConfig partitionConfig = partition.getPartitionConfig();

        return (SourceCacheStorage)caches.get(partitionConfig.getName()+"."+sourceConfig.getName());
    }

    public void create() throws Exception {
        for (Iterator i=caches.values().iterator(); i.hasNext(); ) {
            SourceCacheStorage sourceCacheStorage = (SourceCacheStorage)i.next();
            sourceCacheStorage.create();
        }
    }

    public void create(SourceConfig sourceConfig) throws Exception {

        Partition partition = partitionManager.getPartition(sourceConfig);
        PartitionConfig partitionConfig = partition.getPartitionConfig();

        SourceCacheStorage sourceCacheStorage = createCacheStorage(sourceConfig);
        caches.put(partitionConfig.getName()+"."+sourceConfig.getName(), sourceCacheStorage);
    }

    public void load() throws Exception {
        for (Iterator i=caches.values().iterator(); i.hasNext(); ) {
            SourceCacheStorage sourceCacheStorage = (SourceCacheStorage)i.next();
            sourceCacheStorage.load();
        }
    }

    public void clean() throws Exception {
        for (Iterator i=caches.values().iterator(); i.hasNext(); ) {
            SourceCacheStorage sourceCacheStorage = (SourceCacheStorage)i.next();
            sourceCacheStorage.clean();
        }
    }

    public void drop() throws Exception {
        for (Iterator i=caches.values().iterator(); i.hasNext(); ) {
            SourceCacheStorage sourceCacheStorage = (SourceCacheStorage)i.next();
            sourceCacheStorage.drop();
        }
    }

    public PartitionManager getPartitionManager() {
        return partitionManager;
    }

    public void setPartitionManager(PartitionManager partitionManager) {
        this.partitionManager = partitionManager;
    }

    public PenroseConfig getPenroseConfig() {
        return penroseConfig;
    }

    public void setPenroseConfig(PenroseConfig penroseConfig) {
        this.penroseConfig = penroseConfig;
    }

    public ConnectionManager getConnectionManager() {
        return connectionManager;
    }

    public void setConnectionManager(ConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
    }

    public void remove(SourceConfig sourceConfig, Object key) throws Exception {
        getCacheStorage(sourceConfig).remove(key);
    }

    public Object get(SourceConfig sourceConfig, Object key) throws Exception {
        return getCacheStorage(sourceConfig).get(key);
    }

    public void put(SourceConfig sourceConfig, Object pk, Object sourceValues) throws Exception {
        getCacheStorage(sourceConfig).put(pk, sourceValues);
    }

    public void put(SourceConfig sourceConfig, Filter filter, Collection pks) throws Exception {
        getCacheStorage(sourceConfig).put(filter, pks);
    }

    public Collection search(SourceConfig sourceConfig, Filter filter) throws Exception {
        return getCacheStorage(sourceConfig).search(filter);
    }

    public Map load(SourceConfig sourceConfig, Collection filters, Collection missingKeys) throws Exception {
        return getCacheStorage(sourceConfig).load(filters, missingKeys);
    }

    public Map getExpired(SourceConfig sourceConfig) throws Exception {
        return getCacheStorage(sourceConfig).getExpired();
    }

    public int getLastChangeNumber(SourceConfig sourceConfig) throws Exception {
        return getCacheStorage(sourceConfig).getLastChangeNumber();
    }

    public void setLastChangeNumber(SourceConfig sourceConfig, int lastChangeNumber) throws Exception {
        getCacheStorage(sourceConfig).setLastChangeNumber(lastChangeNumber);
    }
}
