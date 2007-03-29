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
import org.safehaus.penrose.entry.DN;
import org.safehaus.penrose.naming.PenroseContext;
import org.safehaus.penrose.config.PenroseConfig;
import org.safehaus.penrose.module.ModuleConfig;
import org.safehaus.penrose.module.ModuleManager;
import org.safehaus.penrose.connection.ConnectionManager;
import org.safehaus.penrose.source.SourceManager;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.*;
import java.io.File;

/**
 * @author Endi S. Dewata
 */
public class PartitionManager implements PartitionManagerMBean {

    Logger log = LoggerFactory.getLogger(getClass());

    private PenroseConfig penroseConfig;
    private PenroseContext penroseContext;

    private PartitionValidator partitionValidator = new PartitionValidator();

    private Map partitions = new LinkedHashMap();

    public PartitionManager() {
    }

    public void load(PartitionConfig partitionConfig) throws Exception {
        load(penroseConfig.getHome(), partitionConfig);
    }

    public void load(String home, PartitionConfig partitionConfig) throws Exception {

        log.debug("Loading "+partitionConfig.getName()+" partition.");

        PartitionReader partitionReader = new PartitionReader(home);
        Partition partition = partitionReader.read(partitionConfig);

        Collection results = partitionValidator.validate(partition);

        for (Iterator i=results.iterator(); i.hasNext(); ) {
            PartitionValidationResult resultPartition = (PartitionValidationResult)i.next();

            if (resultPartition.getType().equals(PartitionValidationResult.ERROR)) {
                log.error("ERROR: "+resultPartition.getMessage()+" ["+resultPartition.getSource()+"]");
            } else {
                log.warn("WARNING: "+resultPartition.getMessage()+" ["+resultPartition.getSource()+"]");
            }
        }

        ConnectionManager connectionManager = penroseContext.getConnectionManager();
        for (Iterator i=partition.getConnectionConfigs().iterator(); i.hasNext(); ) {
            ConnectionConfig connectionConfig = (ConnectionConfig)i.next();
            connectionManager.init(partition, connectionConfig);
        }

        SourceManager sourceManager = penroseContext.getSourceManager();
        for (Iterator i=partition.getSourceConfigs().iterator(); i.hasNext(); ) {
            SourceConfig sourceConfig = (SourceConfig)i.next();
            sourceManager.init(partition, sourceConfig);
        }

        for (Iterator i=partition.getEntryMappings().iterator(); i.hasNext(); ) {
            EntryMapping entryMapping = (EntryMapping)i.next();

            for (Iterator j=entryMapping.getSourceMappings().iterator(); j.hasNext(); ) {
                SourceMapping sourceMapping = (SourceMapping)j.next();
                sourceManager.init(partition, entryMapping, sourceMapping);
            }
        }

        ModuleManager moduleManager = penroseContext.getModuleManager();
        for (Iterator i=partition.getModuleConfigs().iterator(); i.hasNext(); ) {
            ModuleConfig moduleConfig = (ModuleConfig)i.next();
            moduleManager.init(partition, moduleConfig);
        }

        partitions.put(partition.getName(), partition);
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
        partitions.put(partition.getName(), partition);
    }

    public Partition removePartition(String name) throws Exception {
        return (Partition)partitions.remove(name);
    }

    public void clear() throws Exception {
        partitions.clear();
    }

    public Partition getPartition(String name) throws Exception {
        return (Partition)partitions.get(name);
    }

    public Partition getPartition(SourceMapping sourceMapping) throws Exception {

        if (sourceMapping == null) return null;

        String sourceName = sourceMapping.getSourceName();
        for (Iterator i=partitions.values().iterator(); i.hasNext(); ) {
            Partition partition = (Partition)i.next();
            if (partition.getSourceConfig(sourceName) != null) return partition;
        }
        return null;
    }

    public Partition getPartition(SourceConfig sourceConfig) throws Exception {

        if (sourceConfig == null) return null;

        String connectionName = sourceConfig.getConnectionName();
        for (Iterator i=partitions.values().iterator(); i.hasNext(); ) {
            Partition partition = (Partition)i.next();
            if (partition.getConnectionConfig(connectionName) != null) return partition;
        }
        return null;
    }

    public Partition getPartition(ConnectionConfig connectionConfig) throws Exception {

        if (connectionConfig == null) return null;

        String connectionName = connectionConfig.getName();
        for (Iterator i=partitions.values().iterator(); i.hasNext(); ) {
            Partition partition = (Partition)i.next();
            if (partition.getConnectionConfig(connectionName) != null) return partition;
        }
        return null;
    }

    public Partition getPartition(EntryMapping entryMapping) throws Exception {

        if (entryMapping == null) return null;

        for (Iterator i=partitions.values().iterator(); i.hasNext(); ) {
            Partition partition = (Partition)i.next();

            if (partition.contains(entryMapping)) {
                return partition;
            }
        }

        return null;
    }

    public Partition getPartition(DN dn) throws Exception {

        if (dn == null) return null;

        Partition p = null;
        DN s = null;

        for (Iterator i=partitions.values().iterator(); i.hasNext(); ) {
            Partition partition = (Partition)i.next();

            Collection suffixes = partition.getSuffixes();
            for (Iterator j=suffixes.iterator(); j.hasNext(); ) {
                DN suffix = (DN)j.next();

                if (suffix.isEmpty() && dn.isEmpty() // Root DSE
                    || dn.endsWith(suffix)) {

                    if (s == null || s.getSize() < suffix.getSize()) {
                        p = partition;
                        s = suffix;
                    }
                }
            }
        }

        return p;
    }

    public Collection getPartitions() {
        return partitions.values();
    }

    public Collection getPartitionNames() {
        return partitions.keySet();
    }

    public PenroseContext getPenroseContext() {
        return penroseContext;
    }

    public void setPenroseContext(PenroseContext penroseContext) {
        this.penroseContext = penroseContext;
        partitionValidator.setPenroseContext(penroseContext);
    }

    public PenroseConfig getPenroseConfig() {
        return penroseConfig;
    }

    public void setPenroseConfig(PenroseConfig penroseConfig) {
        this.penroseConfig = penroseConfig;
        partitionValidator.setPenroseConfig(penroseConfig);
    }

    public PartitionValidator getPartitionValidator() {
        return partitionValidator;
    }

    public void setPartitionValidator(PartitionValidator partitionValidator) {
        this.partitionValidator = partitionValidator;
    }
}
