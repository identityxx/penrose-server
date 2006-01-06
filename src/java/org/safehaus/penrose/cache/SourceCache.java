package org.safehaus.penrose.cache;

import org.apache.log4j.Logger;
import org.safehaus.penrose.connector.ConnectorConfig;
import org.safehaus.penrose.connector.Connector;
import org.safehaus.penrose.connector.ConnectionManager;
import org.safehaus.penrose.partition.SourceConfig;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.PartitionManager;
import org.safehaus.penrose.partition.PartitionConfig;
import org.safehaus.penrose.config.PenroseConfig;

import java.util.Map;
import java.util.TreeMap;
import java.util.Iterator;

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

        SourceCacheStorage sourceCacheStorage = new InMemorySourceCacheStorage();
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
}
