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
package org.safehaus.penrose.partition;

import org.safehaus.penrose.mapping.EntryMapping;
import org.safehaus.penrose.mapping.SourceMapping;
import org.safehaus.penrose.schema.SchemaManager;
import org.safehaus.penrose.cache.LRUCache;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.*;
import java.io.File;

/**
 * @author Endi S. Dewata
 */
public class PartitionManager implements PartitionManagerMBean {

    Logger log = LoggerFactory.getLogger(getClass());

    private SchemaManager schemaManager;

    private Map partitions = new TreeMap();

    public LRUCache cache = new LRUCache(20);

    public PartitionManager() {
    }

    public Collection load(String home, Collection partitionConfigs) throws Exception {

        Collection newPartitions = new ArrayList();

        for (Iterator i=partitionConfigs.iterator(); i.hasNext(); ) {
            PartitionConfig partitionConfig = (PartitionConfig)i.next();

            Partition partition = load(home, partitionConfig);
            if (partition == null) continue;

            newPartitions.add(partition);
        }

        return newPartitions;
    }

    public Partition load(String home, PartitionConfig partitionConfig) throws Exception {

        Partition partition = getPartition(partitionConfig.getName());
        if (partition != null) return null;

        log.debug("Loading "+partitionConfig.getName()+" partition.");

        PartitionReader partitionReader = new PartitionReader(home);
        partition = partitionReader.read(partitionConfig);

        addPartition(partition);

        return partition;
    }

    public void store(String home, Collection partitionConfigs) throws Exception {
        for (Iterator i=partitionConfigs.iterator(); i.hasNext(); ) {
            PartitionConfig partitionConfig = (PartitionConfig)i.next();
            store(home, partitionConfig);
        }
    }

    public void store(String home, PartitionConfig partitionConfig) throws Exception {

        String path = (home == null ? "" : home+File.separator)+partitionConfig.getPath();

        log.debug("Storing "+partitionConfig.getName()+" partition into "+path+".");

        Partition partition = getPartition(partitionConfig.getName());

        PartitionWriter partitionWriter = new PartitionWriter(path);
        partitionWriter.write(partition);
    }

    public void addPartition(Partition partition) {
        if (partition.isEnabled()) partition.setStatus(Partition.STARTED);
        partitions.put(partition.getName(), partition);
    }

    public Partition removePartition(String name) throws Exception {
        Partition partition = (Partition)partitions.remove(name);
        if (Partition.STARTED.equals(partition.getStatus())) partition.setStatus(Partition.STOPPED);
        return partition;
    }

    public void clear() throws Exception {
        partitions.clear();
    }

    public String getStatus(String name) throws Exception {
        Partition partition = getPartition(name);
        if (partition == null) return null;
        return partition.getStatus();
    }

    public void start(String name) throws Exception {
        Partition partition = getPartition(name);
        if (partition == null) return;
        partition.setStatus(Partition.STARTED);
    }

    public void stop(String name) throws Exception {
        Partition partition = getPartition(name);
        if (partition == null) return;
        partition.setStatus(Partition.STOPPED);
    }

    public Partition getPartition(String name) throws Exception {
        return (Partition)partitions.get(name);
    }

    public Partition getPartition(SourceMapping sourceMapping) throws Exception {
        String sourceName = sourceMapping.getSourceName();
        for (Iterator i=partitions.values().iterator(); i.hasNext(); ) {
            Partition partition = (Partition)i.next();
            if (partition.getSourceConfig(sourceName) != null) return partition;
        }
        return null;
    }

    public Partition getPartition(SourceConfig sourceConfig) throws Exception {
        String connectionName = sourceConfig.getConnectionName();
        for (Iterator i=partitions.values().iterator(); i.hasNext(); ) {
            Partition partition = (Partition)i.next();
            if (partition.getConnectionConfig(connectionName) != null) return partition;
        }
        return null;
    }

    public Partition getPartition(ConnectionConfig connectionConfig) throws Exception {
        String connectionName = connectionConfig.getName();
        for (Iterator i=partitions.values().iterator(); i.hasNext(); ) {
            Partition partition = (Partition)i.next();
            if (partition.getConnectionConfig(connectionName) != null) return partition;
        }
        return null;
    }

    public Partition getPartition(EntryMapping entryMapping) throws Exception {
        return getPartitionByDn(entryMapping.getDn());
    }

    /**
     * Find the closest partition matching the DN
     * @param dn
     * @return partition
     * @throws Exception
     */
    public Partition getPartitionByDn(String dn) throws Exception {
        Partition partition = (Partition)cache.get(dn);
        if (partition != null) return partition;

        String ndn = schemaManager.normalize(dn);
        ndn = ndn == null ? "" : ndn;

        String suffix = null;

        for (Iterator i=partitions.values().iterator(); i.hasNext(); ) {
            Partition p = (Partition)i.next();
            if (!Partition.STARTED.equals(p.getStatus())) continue;

            for (Iterator j=p.getRootEntryMappings().iterator(); j.hasNext(); ) {
                EntryMapping entryMapping = (EntryMapping)j.next();

                String s = schemaManager.normalize(entryMapping.getDn());

                //log.debug("Checking "+ndn+" with "+suffix);
                if (ndn.equals(s)) {
                    partition = p;
                    suffix = s;
                    continue;
                }

                if ("".equals(s)) continue;

                if (ndn.endsWith(s) && (suffix == null || s.length() > suffix.length())) {
                    partition = p;
                    suffix = s;
                }
            }
        }

        cache.put(dn, partition);
        return partition;
    }

    /**
     * Find a partition exactly matching the DN.
     * @param dn
     * @return partition
     * @throws Exception
     */
    public Partition findPartition(String dn) throws Exception {
        Partition partition = (Partition)cache.get(dn);
        if (partition != null) return partition;

        for (Iterator i=partitions.values().iterator(); i.hasNext(); ) {
            Partition p = (Partition)i.next();
            if (!Partition.STARTED.equals(p.getStatus())) continue;

            Collection list = p.findEntryMappings(dn);
            if (list == null || list.isEmpty()) continue;

            partition = p;
            cache.put(dn, partition);
            break;
        }

        return partition;
    }

    public Collection getPartitions() {
        Collection list = new ArrayList();
        for (Iterator i=partitions.values().iterator(); i.hasNext(); ) {
            Partition partition = (Partition)i.next();
            if (!Partition.STARTED.equals(partition.getStatus())) continue;

            list.add(partition);
        }
        return list;
    }

    public Collection getAllPartitions() {
        return new ArrayList(partitions.values()); // return Serializable list
    }

    public Collection getPartitionNames() {
        return new ArrayList(partitions.keySet()); // return Serializable list
    }

    public SchemaManager getSchemaManager() {
        return schemaManager;
    }

    public void setSchemaManager(SchemaManager schemaManager) {
        this.schemaManager = schemaManager;
    }
}
