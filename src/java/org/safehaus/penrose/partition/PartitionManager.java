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
package org.safehaus.penrose.partition;

import org.safehaus.penrose.mapping.EntryMapping;
import org.safehaus.penrose.mapping.SourceMapping;
import org.safehaus.penrose.schema.Schema;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.PartitionReader;
import org.safehaus.penrose.partition.PartitionValidationResult;
import org.safehaus.penrose.partition.PartitionValidator;
import org.safehaus.penrose.config.PenroseConfig;
import org.apache.log4j.Logger;

import java.util.Iterator;
import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;
import java.io.File;

/**
 * @author Endi S. Dewata
 */
public class PartitionManager {

    Logger log = Logger.getLogger(PartitionManager.class);

    private String home;
    private PenroseConfig penroseConfig;
    private Schema schema;

    PartitionValidator partitionValidator;

    private Map partitionConfigs = new TreeMap();
    private Map partitions = new TreeMap();

    public PartitionManager() {
    }

    public void init() {
        partitionValidator = new PartitionValidator();
        partitionValidator.setPenroseConfig(penroseConfig);
        partitionValidator.setSchema(schema);
    }

    public Partition load(PartitionConfig partitionConfig) throws Exception {

        String path = (home == null ? "" : home+File.separator)+partitionConfig.getPath();

        log.debug("Loading "+partitionConfig.getName()+" partition from "+path+".");

        PartitionReader partitionReader = new PartitionReader(path);
        Partition partition = partitionReader.read();

        Collection results = partitionValidator.validate(partition);

        for (Iterator j=results.iterator(); j.hasNext(); ) {
            PartitionValidationResult resultPartition = (PartitionValidationResult)j.next();

            if (resultPartition.getType().equals(PartitionValidationResult.ERROR)) {
                log.error("ERROR: "+resultPartition.getMessage()+" ["+resultPartition.getSource()+"]");
            } else {
                log.warn("WARNING: "+resultPartition.getMessage()+" ["+resultPartition.getSource()+"]");
            }
        }

        partitionConfigs.put(partitionConfig.getName(), partitionConfig);
        partitions.put(partitionConfig.getName(), partition);

        return partition;
    }

    public void store(PartitionConfig partitionConfig) throws Exception {
        String path = (home == null ? "" : home+File.separator)+partitionConfig.getPath();

        log.debug("Storing "+partitionConfig.getName()+" partition into "+path+".");

        Partition partition = getPartition(partitionConfig.getName());

        PartitionWriter partitionWriter = new PartitionWriter(path);
        partitionWriter.write(partition);
    }

    public PenroseConfig getPenroseConfig() {
        return penroseConfig;
    }

    public void setPenroseConfig(PenroseConfig penroseConfig) {
        this.penroseConfig = penroseConfig;
    }

    public Schema getSchema() {
        return schema;
    }

    public void setSchema(Schema schema) {
        this.schema = schema;
    }

    public Partition removePartition(String name) throws Exception {
        partitionConfigs.remove(name);
        return (Partition)partitions.remove(name);
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

    public Partition getPartitionByDn(String dn) throws Exception {
        String ndn = schema.normalize(dn);

        for (Iterator i=partitions.values().iterator(); i.hasNext(); ) {
            Partition partition = (Partition)i.next();
            for (Iterator j=partition.getRootEntryMappings().iterator(); j.hasNext(); ) {
                EntryMapping entryMapping = (EntryMapping)j.next();
                String suffix = schema.normalize(entryMapping.getDn());
                if (ndn.endsWith(suffix)) return partition;
            }
        }

        return null;
    }

    public Collection getPartitions() {
        return partitions.values();
    }

    public Collection getPartitionConfigs() {
        return partitionConfigs.values();
    }

    public String getHome() {
        return home;
    }

    public void setHome(String home) {
        this.home = home;
    }
}
