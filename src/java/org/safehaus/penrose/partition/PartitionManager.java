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
import org.safehaus.penrose.mapping.SourceDefinition;
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

/**
 * @author Endi S. Dewata
 */
public class PartitionManager {

    Logger log = Logger.getLogger(PartitionManager.class);

    private PenroseConfig penroseConfig;
    private Schema schema;

    PartitionValidator partitionValidator;

    private Map partitions = new TreeMap();

    public PartitionManager() {
    }

    public void init() {
        partitionValidator = new PartitionValidator();
        partitionValidator.setServerConfig(penroseConfig);
        partitionValidator.setSchema(schema);
    }

    public Partition load(String path) throws Exception {

        log.debug("Loading partition "+path+".");
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

        for (Iterator i=partition.getRootEntryMappings().iterator(); i.hasNext(); ) {
            EntryMapping entryMapping = (EntryMapping)i.next();
            String ndn = schema.normalize(entryMapping.getDn());
            partitions.put(ndn, partition);
        }

        return partition;
    }

    public PenroseConfig getServerConfig() {
        return penroseConfig;
    }

    public void setServerConfig(PenroseConfig penroseConfig) {
        this.penroseConfig = penroseConfig;
    }

    public Schema getSchema() {
        return schema;
    }

    public void setSchema(Schema schema) {
        this.schema = schema;
    }

    public Partition getPartition(SourceMapping sourceMapping) throws Exception {
        String connectionName = sourceMapping.getConnectionName();
        for (Iterator i=partitions.values().iterator(); i.hasNext(); ) {
            Partition partition = (Partition)i.next();
            if (partition.getConnectionConfig(connectionName) != null) return partition;
        }
        return null;
    }

    public Partition getPartition(SourceDefinition sourceDefinition) throws Exception {
        String connectionName = sourceDefinition.getConnectionName();
        for (Iterator i=partitions.values().iterator(); i.hasNext(); ) {
            Partition partition = (Partition)i.next();
            if (partition.getConnectionConfig(connectionName) != null) return partition;
        }
        return null;
    }

    public Partition getConfig(EntryMapping entryMapping) throws Exception {
        return getConfig(entryMapping.getDn());
    }

    public Partition getConfig(String dn) throws Exception {
        String ndn = schema.normalize(dn);
        for (Iterator i=partitions.keySet().iterator(); i.hasNext(); ) {
            String suffix = (String)i.next();
            if (ndn.endsWith(suffix)) return (Partition)partitions.get(suffix);
        }
        return null;
    }

    public Collection getPartitions() {
        return partitions.values();
    }

}
