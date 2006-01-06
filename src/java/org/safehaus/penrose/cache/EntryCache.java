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
import org.safehaus.penrose.mapping.Entry;
import org.safehaus.penrose.mapping.Row;
import org.safehaus.penrose.config.PenroseConfig;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.PartitionManager;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.connector.ConnectionManager;
import org.safehaus.penrose.filter.Filter;
import org.apache.log4j.Logger;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class EntryCache {

    Logger log = Logger.getLogger(getClass());

    public final static String DEFAULT_CACHE_NAME  = "Entry Cache";
    public final static String DEFAULT_CACHE_CLASS = DefaultEntryCache.class.getName();

    private CacheConfig cacheConfig;
    ConnectionManager connectionManager;
    PenroseConfig penroseConfig;
    PartitionManager partitionManager;

    private Map caches = new TreeMap();

    boolean first = true;

    public EntryCacheStorage createCacheStorage(String parentDn, EntryMapping entryMapping) throws Exception {

        Partition partition = partitionManager.getPartition(entryMapping);

        EntryCacheStorage cacheStorage = new InMemoryEntryCacheStorage();
        cacheStorage.setCacheConfig(cacheConfig);
        cacheStorage.setConnectionManager(connectionManager);
        cacheStorage.setPartition(partition);
        cacheStorage.setEntryMapping(entryMapping);
        cacheStorage.setParentDn(parentDn);

        cacheStorage.init();

        return cacheStorage;
    }

    public EntryCacheStorage getCacheStorage(String parentDn, EntryMapping entryMapping) throws Exception {

        String key = entryMapping.getRdn()+","+parentDn;
        EntryCacheStorage cacheStorage = (EntryCacheStorage)caches.get(key);

        if (cacheStorage == null) {
            cacheStorage = createCacheStorage(parentDn, entryMapping);
            caches.put(key, cacheStorage);
        }

        return cacheStorage;
    }

    public void put(EntryMapping entryMapping, String parentDn, Filter filter, Collection dns) throws Exception {
        getCacheStorage(parentDn, entryMapping).put(filter, dns);
    }

    public Collection search(EntryMapping entryMapping, String parentDn, Filter filter) throws Exception {
        return getCacheStorage(parentDn, entryMapping).search(filter);
    }

    public void put(Entry entry) throws Exception {
        getCacheStorage(entry.getParentDn(), entry.getEntryMapping()).put(entry.getRdn(), entry);
    }

    public Entry get(String dn) throws Exception {
        EntryMapping entryMapping = partitionManager.findEntryMapping(dn);
        String parentDn = Entry.getParentDn(dn);
        Row rdn = Entry.getRdn(dn);
        return getCacheStorage(parentDn, entryMapping).get(rdn);
    }

    public Entry get(EntryMapping entryMapping, String parentDn, Row rdn) throws Exception {
        return getCacheStorage(parentDn, entryMapping).get(rdn);
    }

    public void remove(Entry entry) throws Exception {
        EntryMapping entryMapping = entry.getEntryMapping();
        Partition partition = partitionManager.getPartition(entryMapping);
        remove(partition, entryMapping, entry.getParentDn(), entry.getRdn());
    }

    public void remove(Partition partition, EntryMapping entryMapping, String parentDn, Row rdn) throws Exception {
        String dn = rdn+","+parentDn;

        Collection children = partition.getChildren(entryMapping);
        for (Iterator i=children.iterator(); i.hasNext(); ) {
            EntryMapping childMapping = (EntryMapping)i.next();

            Collection childDns = search(childMapping, dn, null);
            for (Iterator j=childDns.iterator(); j.hasNext(); ) {
                String childDn = (String)j.next();

                Row childRdn = Entry.getRdn(childDn);
                remove(partition, childMapping, dn, childRdn);
            }
        }

        log.debug("Remove cache "+dn);
        getCacheStorage(parentDn, entryMapping).remove(rdn);
    }

    public PenroseConfig getPenroseConfig() {
        return penroseConfig;
    }

    public void setPenroseConfig(PenroseConfig penroseConfig) {
        this.penroseConfig = penroseConfig;
    }

    public PartitionManager getPartitionManager() {
        return partitionManager;
    }

    public void setPartitionManager(PartitionManager partitionManager) {
        this.partitionManager = partitionManager;
    }

    public void create() throws Exception {
        for (Iterator i=partitionManager.getPartitions().iterator(); i.hasNext(); ) {
            Partition partition = (Partition)i.next();
            Collection entryMappings = partition.getRootEntryMappings();
            create(partition, null, entryMappings);
        }
    }

    public  void create(Partition partition, String parentDn, Collection entryDefinitions) throws Exception {

        for (Iterator i=entryDefinitions.iterator(); i.hasNext(); ) {
            EntryMapping entryMapping = (EntryMapping)i.next();

            EntryCacheStorage cacheStorage = getCacheStorage(parentDn, entryMapping);

            if (first) {
                cacheStorage.globalCreate();
                first = false;
            }

            log.debug("Creating tables for "+entryMapping.getDn());
            cacheStorage.create();

            Collection children = partition.getChildren(entryMapping);
            create(partition, entryMapping.getDn(), children);
        }
    }

    public void load(Penrose penrose) throws Exception {
        for (Iterator i=partitionManager.getPartitions().iterator(); i.hasNext(); ) {
            Partition partition = (Partition)i.next();
            load(penrose, partition);
        }
    }

    public void load(Penrose penrose, Partition partition) throws Exception {
    }

    public void clean() throws Exception {
        for (Iterator i=partitionManager.getPartitions().iterator(); i.hasNext(); ) {
            Partition partition = (Partition)i.next();
            Collection entryMappings = partition.getRootEntryMappings();
            clean(partition, null, entryMappings);
        }
    }

    public void clean(Partition partition, String parentDn, Collection entryDefinitions) throws Exception {

        for (Iterator i=entryDefinitions.iterator(); i.hasNext(); ) {
            EntryMapping entryMapping = (EntryMapping)i.next();

            EntryCacheStorage entryCacheStorage = getCacheStorage(parentDn, entryMapping);
            Collection dns = entryCacheStorage.search(null);

            Collection children = partition.getChildren(entryMapping);
            for (Iterator j=dns.iterator(); j.hasNext(); ) {
                String dn = (String)j.next();
                clean(partition, dn, children);

                Row rdn = Entry.getRdn(dn);
                entryCacheStorage.remove(rdn);
            }
        }
    }

    public void drop() throws Exception {

        EntryCacheStorage cacheStorage = null;

        for (Iterator i=partitionManager.getPartitions().iterator(); i.hasNext(); ) {
            Partition partition = (Partition)i.next();
            Collection entryMappings = partition.getRootEntryMappings();
            cacheStorage = drop(partition, null, entryMappings);
        }

        cacheStorage.globalDrop();
    }

    public EntryCacheStorage drop(Partition partition, String parentDn, Collection entryDefinitions) throws Exception {

        EntryCacheStorage cacheStorage = null;

        for (Iterator i=entryDefinitions.iterator(); i.hasNext(); ) {
            EntryMapping entryMapping = (EntryMapping)i.next();

            Collection children = partition.getChildren(entryMapping);
            drop(partition, entryMapping.getDn(), children);

            cacheStorage = getCacheStorage(parentDn, entryMapping);
            log.debug("Dropping tables for "+entryMapping.getDn());
            cacheStorage.drop();
        }

        return cacheStorage;
    }

    public ConnectionManager getConnectionManager() {
        return connectionManager;
    }

    public void setConnectionManager(ConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
    }

    public CacheConfig getCacheConfig() {
        return cacheConfig;
    }

    public void setCacheConfig(CacheConfig cacheConfig) {
        this.cacheConfig = cacheConfig;
    }
}
