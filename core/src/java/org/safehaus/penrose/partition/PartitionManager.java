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
import org.safehaus.penrose.ldap.DN;
import org.safehaus.penrose.naming.PenroseContext;
import org.safehaus.penrose.config.PenroseConfig;
import org.safehaus.penrose.source.*;
import org.safehaus.penrose.connection.ConnectionConfig;
import org.safehaus.penrose.connection.Connection;
import org.safehaus.penrose.connection.ConnectionManager;
import org.safehaus.penrose.module.ModuleConfig;
import org.safehaus.penrose.module.Module;
import org.safehaus.penrose.module.ModuleManager;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.*;
import java.io.File;

/**
 * @author Endi S. Dewata
 */
public class PartitionManager implements PartitionManagerMBean {

    public Logger log = LoggerFactory.getLogger(getClass());
    public Logger errorLog = org.safehaus.penrose.log.Error.log;
    public boolean debug = log.isDebugEnabled();

    private PenroseConfig penroseConfig;
    private PenroseContext penroseContext;

    private ConnectionManager  connectionManager;
    private SourceManager      sourceManager;
    private SourceSyncManager  sourceSyncManager;
    private ModuleManager      moduleManager;

    private PartitionValidator partitionValidator = new PartitionValidator();

    private Map<String,Partition> partitions = new LinkedHashMap<String,Partition>();

    public PartitionManager() {
    }

    public void loadPartitions(String dir) throws Exception {

        PartitionReader partitionReader = new PartitionReader();

        File services = new File(dir);
        for (File file : services.listFiles()) {
            if (!file.isDirectory()) continue;

            String name = file.getName();

            if (debug) {
                log.debug("----------------------------------------------------------------------------------");
                log.debug("Loading "+name+" partition.");
            }

            Partition partition = partitionReader.read(file);
            if (partition == null || !partition.isEnabled()) continue;

            Collection<PartitionValidationResult> results = partitionValidator.validate(partition);

            for (PartitionValidationResult result : results) {
                if (result.getType().equals(PartitionValidationResult.ERROR)) {
                    errorLog.error("ERROR: " + result.getMessage() + " [" + result.getSource() + "]");
                } else {
                    errorLog.warn("WARNING: " + result.getMessage() + " [" + result.getSource() + "]");
                }
            }

            for (ConnectionConfig connectionConfig : partition.getConnections().getConnectionConfigs()) {
                Connection connection = getConnectionManager().init(partition, connectionConfig);
                if (connection != null) connection.start();
            }

            for (SourceConfig sourceConfig : partition.getSources().getSourceConfigs()) {
                getSourceManager().init(partition, sourceConfig);
            }

            for (SourceSyncConfig sourceSyncConfig : partition.getSources().getSourceSyncConfigs()) {
                SourceSync sourceSync = getSourceSyncManager().init(partition, sourceSyncConfig);
                if (sourceSync != null) sourceSync.start();
            }

            for (EntryMapping entryMapping : partition.getMappings().getEntryMappings()) {
                getSourceManager().init(partition, entryMapping);
            }

            for (ModuleConfig moduleConfig : partition.getModules().getModuleConfigs()) {
                Module module = getModuleManager().init(partition, moduleConfig);
                if (module != null) module.start();
            }

            addPartition(partition);
        }

        log.debug("----------------------------------------------------------------------------------");
    }

    public void store(String home, Collection<PartitionConfig> partitionConfigs) throws Exception {
        for (PartitionConfig partitionConfig : partitionConfigs) {
            store(home, partitionConfig);
        }
    }

    public void store(String home, PartitionConfig partitionConfig) throws Exception {

        String path = (home == null ? "" : home+File.separator)+"partitions"+File.separator+partitionConfig.getName();

        if (debug) log.debug("Storing "+partitionConfig.getName()+" partition into "+path+".");

        Partition partition = getPartition(partitionConfig.getName());

        PartitionWriter partitionWriter = new PartitionWriter(path);
        partitionWriter.write(partition);
    }

    public void addPartition(Partition partition) {
        partitions.put(partition.getName(), partition);
    }

    public Partition removePartition(String name) throws Exception {
        return partitions.remove(name);
    }

    public void clear() throws Exception {
        for (Partition partition : getPartitions()) {

            for (ModuleConfig moduleConfig : partition.getModules().getModuleConfigs()) {
                Module module = getModuleManager().getModule(partition, moduleConfig.getName());
                if (module != null) module.stop();
            }

            for (SourceSyncConfig sourceSyncConfig : partition.getSources().getSourceSyncConfigs()) {
                SourceSync sourceSync = getSourceSyncManager().getSourceSync(partition, sourceSyncConfig.getName());
                if (sourceSync != null) sourceSync.stop();
            }

            for (ConnectionConfig connectionConfig : partition.getConnections().getConnectionConfigs()) {
                Connection connection = getConnectionManager().getConnection(partition, connectionConfig.getName());
                if (connection != null) connection.stop();
            }
        }

        partitions.clear();
    }

    public Partition getPartition(String name) throws Exception {
        return partitions.get(name);
    }

    public Partition getPartition(SourceMapping sourceMapping) throws Exception {

        if (sourceMapping == null) return null;

        String sourceName = sourceMapping.getSourceName();
        for (Partition partition : partitions.values()) {
            if (partition.getSources().getSourceConfig(sourceName) != null) return partition;
        }
        return null;
    }

    public Partition getPartition(SourceConfig sourceConfig) throws Exception {

        if (sourceConfig == null) return null;

        String connectionName = sourceConfig.getConnectionName();
        for (Partition partition : partitions.values()) {
            if (partition.getConnections().getConnectionConfig(connectionName) != null) return partition;
        }
        return null;
    }

    public Partition getPartition(ConnectionConfig connectionConfig) throws Exception {

        if (connectionConfig == null) return null;

        String connectionName = connectionConfig.getName();
        for (Partition partition : partitions.values()) {
            if (partition.getConnections().getConnectionConfig(connectionName) != null) return partition;
        }
        return null;
    }

    public Partition getPartition(EntryMapping entryMapping) throws Exception {

        if (entryMapping == null) return null;

        for (Partition partition : partitions.values()) {
            if (partition.getMappings().contains(entryMapping)) {
                return partition;
            }
        }

        return null;
    }

    public Partition getPartition(DN dn) throws Exception {

        if (debug) log.debug("Finding partition for \""+dn+"\".");

        if (dn == null) {
            log.debug("DN is null.");
            return null;
        }

        Partition p = null;
        DN s = null;

        for (Partition partition : partitions.values()) {
            if (debug) log.debug("Checking "+partition.getName()+" partition.");

            Collection<DN> suffixes = partition.getMappings().getSuffixes();
            for (DN suffix : suffixes) {
                if (suffix.isEmpty() && dn.isEmpty() // Root DSE
                        || dn.endsWith(suffix)) {

                    if (s == null || s.getSize() < suffix.getSize()) {
                        p = partition;
                        s = suffix;
                    }
                }
            }
        }

        if (debug) {
            if (p == null) {
                log.debug("Partition not found.");
            } else {
                log.debug("Found "+p.getName()+" partition.");
            }
        }

        return p;
    }

    public Collection<Partition> getPartitions() {
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

    public ConnectionManager getConnectionManager() {
        return connectionManager;
    }

    public void setConnectionManager(ConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
    }

    public SourceManager getSourceManager() {
        return sourceManager;
    }

    public void setSourceManager(SourceManager sourceManager) {
        this.sourceManager = sourceManager;
    }

    public SourceSyncManager getSourceSyncManager() {
        return sourceSyncManager;
    }

    public void setSourceSyncManager(SourceSyncManager sourceSyncManager) {
        this.sourceSyncManager = sourceSyncManager;
    }

    public ModuleManager getModuleManager() {
        return moduleManager;
    }

    public void setModuleManager(ModuleManager moduleManager) {
        this.moduleManager = moduleManager;
    }
}
