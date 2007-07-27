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
import org.safehaus.penrose.module.ModuleConfig;
import org.safehaus.penrose.module.Module;
import org.safehaus.penrose.session.SessionContext;
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

    private PenroseConfig  penroseConfig;
    private PenroseContext penroseContext;
    private SessionContext sessionContext;

    private PartitionReader    partitionReader    = new PartitionReader();
    private PartitionValidator partitionValidator = new PartitionValidator();

    private Map<String,Partition> partitions = new LinkedHashMap<String,Partition>();

    public PartitionManager() {
    }

    public PartitionConfig load(File dir) throws Exception {
        log.debug("Loading partition from "+dir.getAbsolutePath()+".");
        return partitionReader.read(dir);
    }

    public Partition init(PartitionConfig partitionConfig) throws Exception {

        Collection<PartitionValidationResult> results = partitionValidator.validate(partitionConfig);

        for (PartitionValidationResult result : results) {
            if (result.getType().equals(PartitionValidationResult.ERROR)) {
                errorLog.error("ERROR: " + result.getMessage() + " [" + result.getSource() + "]");
            } else {
                errorLog.warn("WARNING: " + result.getMessage() + " [" + result.getSource() + "]");
            }
        }

        Partition partition = new Partition(partitionConfig);
        partition.setPenroseConfig(penroseConfig);
        partition.setPenroseContext(penroseContext);
        partition.setSessionContext(sessionContext);

        for (ConnectionConfig connectionConfig : partitionConfig.getConnections().getConnectionConfigs()) {

            Connection connection = partition.createConnection(connectionConfig);
            partition.addConnection(connection);
        }

        for (SourceConfig sourceConfig : partitionConfig.getSources().getSourceConfigs()) {

            Source source = partition.createSource(sourceConfig);
            partition.addSource(source);
        }

        for (SourceSyncConfig sourceSyncConfig : partitionConfig.getSources().getSourceSyncConfigs()) {

            SourceSync sourceSync = partition.createSourceSync(sourceSyncConfig);
            partition.addSourceSync(sourceSync);
        }

        for (EntryMapping entryMapping : partitionConfig.getMappings().getEntryMappings()) {
            partition.createEntry(entryMapping);
        }

        for (ModuleConfig moduleConfig : partitionConfig.getModules().getModuleConfigs()) {
            if (!moduleConfig.isEnabled()) continue;

            Module module = partition.createModule(moduleConfig);
            partition.addModule(module);
        }

        addPartition(partition);

        return partition;
    }

    public void store(String home, Collection<Partition> partitions) throws Exception {
        for (Partition partition : partitions) {
            store(home, partition);
        }
    }

    public void store(String home, Partition partition) throws Exception {

        PartitionConfig partitionConfig = partition.getPartitionConfig();

        String path = (home == null ? "" : home+File.separator)+"partitions"+File.separator+partitionConfig.getName();

        if (debug) log.debug("Storing "+partitionConfig.getName()+" partition into "+path+".");

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

            for (Module module : partition.getModules()) {
                module.stop();
            }

            for (SourceSync sourceSync : partition.getSourceSyncs()) {
                sourceSync.stop();
            }

            for (Connection connection : partition.getConnections()) {
                connection.stop();
            }
        }

        partitions.clear();
    }

    public Partition getPartition(String name) {
        return partitions.get(name);
    }

    public Partition getPartition(SourceMapping sourceMapping) throws Exception {

        if (sourceMapping == null) return null;

        String sourceName = sourceMapping.getSourceName();
        for (Partition partition : partitions.values()) {
            PartitionConfig partitionConfig = partition.getPartitionConfig();
            if (partitionConfig.getSources().getSourceConfig(sourceName) != null) return partition;
        }
        return null;
    }

    public Partition getPartition(SourceConfig sourceConfig) throws Exception {

        if (sourceConfig == null) return null;

        String connectionName = sourceConfig.getConnectionName();
        for (Partition partition : partitions.values()) {
            PartitionConfig partitionConfig = partition.getPartitionConfig();
            if (partitionConfig.getConnections().getConnectionConfig(connectionName) != null) return partition;
        }
        return null;
    }

    public Partition getPartition(ConnectionConfig connectionConfig) throws Exception {

        if (connectionConfig == null) return null;

        String connectionName = connectionConfig.getName();
        for (Partition partition : partitions.values()) {
            PartitionConfig partitionConfig = partition.getPartitionConfig();
            if (partitionConfig.getConnections().getConnectionConfig(connectionName) != null) return partition;
        }
        return null;
    }

    public Partition getPartition(EntryMapping entryMapping) throws Exception {

        if (entryMapping == null) return null;

        for (Partition partition : partitions.values()) {
            PartitionConfig partitionConfig = partition.getPartitionConfig();
            if (partitionConfig.getMappings().contains(entryMapping)) {
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

            PartitionConfig partitionConfig = partition.getPartitionConfig();
            Collection<DN> suffixes = partitionConfig.getMappings().getSuffixes();
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

    public SessionContext getSessionContext() {
        return sessionContext;
    }

    public void setSessionContext(SessionContext sessionContext) {
        this.sessionContext = sessionContext;
    }
}
